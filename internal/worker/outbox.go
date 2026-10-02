// Package worker contém os laços de processamento em background.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// OutboxConfig parametriza o publicador da outbox.
type OutboxConfig struct {
	// ClaimerID identifica a instância, para auditoria da reserva.
	ClaimerID string
	// BatchSize é o tamanho do lote reservado por ciclo.
	BatchSize int
	// Interval é o intervalo entre ciclos.
	Interval time.Duration
	// Lease é por quanto tempo um lote fica reservado. Se a instância cair
	// antes de confirmar, a reserva expira e outra assume o trabalho.
	Lease time.Duration
	// MaxAttempts é o número de tentativas antes de considerar a publicação
	// permanentemente falha (o registro permanece na outbox para auditoria).
	MaxAttempts int
	// BaseBackoff e MaxBackoff governam o reagendamento após falha.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// OutboxPublisher publica os eventos pendentes da outbox.
//
// Suporta MÚLTIPLAS instâncias em paralelo: a reserva do lote é feita com
// FOR UPDATE SKIP LOCKED, então dois publicadores nunca recebem o mesmo evento.
// Se uma instância morre entre o commit da transação de negócio e a publicação,
// a reserva vence e outra instância publica o evento — sem perda e sem
// duplicação de efeito (o eventId é preservado como MessageDeduplicationId).
type OutboxPublisher struct {
	store     ports.Store
	publisher ports.Publisher
	config    OutboxConfig
	logger    *slog.Logger
}

// NewOutboxPublisher cria o publicador.
func NewOutboxPublisher(store ports.Store, publisher ports.Publisher, config OutboxConfig, logger *slog.Logger) *OutboxPublisher {
	if config.BatchSize <= 0 {
		config.BatchSize = 20
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.Lease <= 0 {
		config.Lease = 30 * time.Second
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 10
	}
	if config.BaseBackoff <= 0 {
		config.BaseBackoff = 2 * time.Second
	}
	if config.MaxBackoff < config.BaseBackoff {
		config.MaxBackoff = 5 * time.Minute
	}
	if config.ClaimerID == "" {
		config.ClaimerID = "instance"
	}
	return &OutboxPublisher{store: store, publisher: publisher, config: config, logger: logger}
}

// Run executa o laço até o contexto ser cancelado.
func (w *OutboxPublisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()

	for {
		if err := w.publishOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("ciclo de publicação da outbox falhou", "err", err)
		}

		select {
		case <-ctx.Done():
			w.logger.Info("publicador da outbox encerrado")
			return nil
		case <-ticker.C:
		}
	}
}

// publishOnce reserva e publica um lote.
//
// Quando o contexto é cancelado no meio do lote (SIGTERM durante o shutdown), os
// eventos já publicados são confirmados e os restantes ficam reservados até a
// expiração do lease — outra instância os assume. Nada é perdido.
func (w *OutboxPublisher) publishOnce(ctx context.Context) error {
	records, err := w.store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: w.config.ClaimerID,
		Limit:     w.config.BatchSize,
		Now:       time.Now().UTC(),
		Lease:     w.config.Lease,
	})
	if err != nil {
		return fmt.Errorf("reservando lote da outbox: %w", err)
	}
	if len(records) == 0 {
		return nil
	}

	published := make([]int64, 0, len(records))
	for _, record := range records {
		if err := w.publisher.Publish(ctx, record); err != nil {
			w.handleFailure(ctx, record, err)
			continue
		}
		published = append(published, record.ID)
	}

	if len(published) > 0 {
		if err := w.store.Outbox().MarkPublished(ctx, published); err != nil {
			// A publicação já ocorreu; o registro volta a ser elegível quando o
			// lease vencer. A republicação preserva o eventId, então o broker
			// deduplica e nenhum efeito é duplicado.
			return fmt.Errorf("confirmando %d evento(s) publicado(s): %w", len(published), err)
		}
		w.logger.Info("lote da outbox publicado", "count", len(published))
	}
	return nil
}

func (w *OutboxPublisher) handleFailure(ctx context.Context, record ports.OutboxRecord, publishErr error) {
	attempts := record.Attempts + 1
	failure := ports.OutboxFailure{
		Attempts:      attempts,
		NextAttemptAt: time.Now().UTC().Add(w.backoff(attempts)),
		LastError:     publishErr.Error(),
	}

	// O contexto pode já estar cancelado (shutdown); usa-se um contexto
	// independente para que o reagendamento seja persistido de qualquer forma.
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := w.store.Outbox().MarkFailed(persistCtx, record.ID, failure); err != nil {
		w.logger.Error("reagendando evento da outbox falhou",
			"eventId", record.EventID, "attempts", attempts, "err", err)
		return
	}

	if attempts >= w.config.MaxAttempts {
		w.logger.Error("evento da outbox esgotou as tentativas e permanece pendente para auditoria",
			"eventId", record.EventID,
			"eventType", record.EventType,
			"attempts", attempts,
			"err", publishErr,
		)
		return
	}
	w.logger.Warn("publicação falhou; evento reagendado",
		"eventId", record.EventID,
		"attempts", attempts,
		"nextAttemptAt", failure.NextAttemptAt,
		"err", publishErr,
	)
}

// backoff é exponencial com teto.
func (w *OutboxPublisher) backoff(attempts int) time.Duration {
	delay := w.config.BaseBackoff
	for i := 1; i < attempts; i++ {
		if delay >= w.config.MaxBackoff/2 {
			return w.config.MaxBackoff
		}
		delay *= 2
	}
	if delay > w.config.MaxBackoff {
		return w.config.MaxBackoff
	}
	return delay
}

// OutboxRunner adapta o publicador ao ciclo de vida da aplicação.
type OutboxRunner struct {
	publisher *OutboxPublisher
	logger    *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewOutboxRunner cria o runner.
func NewOutboxRunner(publisher *OutboxPublisher, logger *slog.Logger) *OutboxRunner {
	return &OutboxRunner{publisher: publisher, logger: logger}
}

// Start inicia o laço numa goroutine e retorna imediatamente.
//
// Um runner sem publisher é um no-op: permite desligar a publicação mantendo o
// mesmo grafo de composição.
func (r *OutboxRunner) Start(_ context.Context) error {
	if r.publisher == nil {
		r.logger.Info("publicador da outbox não iniciado (desabilitado)")
		return nil
	}

	runCtx, cancel := context.WithCancel(context.Background())

	r.mu.Lock()
	r.cancel = cancel
	r.done = make(chan struct{})
	done := r.done
	r.mu.Unlock()

	go func() {
		defer close(done)
		if err := r.publisher.Run(runCtx); err != nil {
			r.logger.Error("publicador da outbox terminou com erro", "err", err)
		}
	}()
	r.logger.Info("publicador da outbox iniciado")
	return nil
}

// Stop cancela o laço e aguarda a conclusão.
//
// A espera é o que garante o encerramento seguro: o ciclo em andamento termina
// (ou os registros ficam reservados até o lease vencer) antes de o pool do banco
// ser fechado pelo hook de OnStop registrado antes deste.
func (r *OutboxRunner) Stop(ctx context.Context) error {
	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.mu.Unlock()

	if cancel == nil {
		return nil
	}
	cancel()

	select {
	case <-done:
		r.logger.Info("publicador da outbox encerrado de forma limpa")
	case <-ctx.Done():
		return fmt.Errorf("publicador da outbox não encerrou dentro do prazo: %w", ctx.Err())
	}
	return nil
}
