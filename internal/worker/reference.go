package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// PendingReferenceResolver resolve UMA operação pendente vencida por execução.
type PendingReferenceResolver interface {
	ResolveNextPendingReference(ctx context.Context) (usecase.PendingResolution, error)
}

// ReferenceConfig parametriza o laço do worker de referências.
type ReferenceConfig struct {
	// Interval é a espera entre varreduras quando não há trabalho vencido.
	Interval time.Duration
	// ErrorBackoff é a espera após erro. Necessária porque o rollback devolve a
	// operação ao estado "vencida": sem a espera, um erro persistente (banco
	// fora, por exemplo) repetiria a MESMA operação em ciclo apertado, gastando
	// conexões sem chance de sucesso.
	ErrorBackoff time.Duration
}

func (c ReferenceConfig) normalized() ReferenceConfig {
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.ErrorBackoff <= 0 {
		c.ErrorBackoff = 2 * time.Second
	}
	return c
}

// ReferenceStats são os contadores do worker (base das métricas).
type ReferenceStats struct {
	// Scans é quantas execuções terminaram sem erro (com ou sem trabalho).
	Scans int64
	// Resolved, Rescheduled, Rejected e Exhausted contam os desfechos.
	Resolved    int64
	Rescheduled int64
	Rejected    int64
	Exhausted   int64
	// Errors conta as execuções que falharam por infraestrutura.
	Errors int64
}

// ReferenceWorker retoma operações que ficaram esperando por uma referência.
//
// # Por que este laço é diferente do publicador da outbox
//
// A outbox publica um EFEITO EXTERNO, que não cabe na transação de negócio; por
// isso o publicador reserva com lease e precisa recuperar reserva abandonada.
// Aqui o trabalho é LOCAL e cabe inteiramente numa transação (reservar e gravar
// o desfecho no mesmo commit), então não há lease nem recuperação: uma queda
// apenas reverte tudo e a operação continua vencida. Ver ARCHITECTURE.md.
//
// O laço é sequencial por processo. Paralelismo vem de instâncias concorrentes,
// e a exclusão entre elas é do FOR UPDATE SKIP LOCKED — cada operação é tentada
// por uma única instância.
type ReferenceWorker struct {
	resolver PendingReferenceResolver
	config   ReferenceConfig
	logger   *slog.Logger

	mu    sync.Mutex
	stats ReferenceStats
}

// NewReferenceWorker cria o worker.
func NewReferenceWorker(resolver PendingReferenceResolver, config ReferenceConfig, logger *slog.Logger) *ReferenceWorker {
	return &ReferenceWorker{resolver: resolver, config: config.normalized(), logger: logger}
}

// Stats devolve uma cópia dos contadores.
func (w *ReferenceWorker) Stats() ReferenceStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// Run executa o laço até o contexto ser cancelado.
//
// Enquanto houver trabalho vencido, o laço NÃO espera o intervalo: ele volta
// imediatamente, para drenar o acúmulo (um backlog de operações pendentes é o
// caso comum após uma indisponibilidade). O intervalo só é aplicado quando não
// há nada a fazer — que é o estado normal.
func (w *ReferenceWorker) Run(ctx context.Context) error {
	w.logger.Info("worker de referências iniciado", "interval", w.config.Interval.String())

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker de referências encerrado")
			return nil
		}

		resolution, err := w.resolver.ResolveNextPendingReference(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				w.logger.Info("worker de referências encerrado")
				return nil
			}
			w.bump(func(s *ReferenceStats) { s.Errors++ })
			w.logger.Error("retomada de referência falhou; nova tentativa em seguida",
				"err", err, "retryIn", w.config.ErrorBackoff.String())
			if !sleepOrDone(ctx, w.config.ErrorBackoff) {
				w.logger.Info("worker de referências encerrado")
				return nil
			}
			continue
		}

		w.bump(func(s *ReferenceStats) { s.Scans++ })

		if !resolution.Attempted {
			// Nada vencido: este é o caminho normal e frequente.
			if !sleepOrDone(ctx, w.config.Interval) {
				w.logger.Info("worker de referências encerrado")
				return nil
			}
			continue
		}

		w.record(resolution)
	}
}

// record contabiliza e registra o desfecho de uma tentativa.
func (w *ReferenceWorker) record(resolution usecase.PendingResolution) {
	switch resolution.Outcome {
	case usecase.OutcomeResolved:
		w.bump(func(s *ReferenceStats) { s.Resolved++ })
		w.logger.Info("referência pendente resolvida",
			"transactionId", resolution.TransactionID, "attempts", resolution.Attempts)

	case usecase.OutcomeRescheduled:
		w.bump(func(s *ReferenceStats) { s.Rescheduled++ })
		// Debug de propósito: enquanto a referência não chega, este evento se
		// repete a cada backoff. No nível info ele viraria ruído.
		w.logger.Debug("referência ainda indisponível; nova tentativa agendada",
			"transactionId", resolution.TransactionID,
			"attempts", resolution.Attempts,
			"nextAttemptAt", resolution.NextAttemptAt)

	case usecase.OutcomeRejected:
		w.bump(func(s *ReferenceStats) { s.Rejected++ })
		w.logger.Warn("referência pendente rejeitada por regra de negócio",
			"transactionId", resolution.TransactionID, "attempts", resolution.Attempts)

	case usecase.OutcomeExhausted:
		w.bump(func(s *ReferenceStats) { s.Exhausted++ })
		w.logger.Warn("referência pendente esgotada pela política",
			"transactionId", resolution.TransactionID, "attempts", resolution.Attempts)

	default:
		// Desfecho inesperado: não inventa contador, mas não engole o fato.
		w.logger.Error("desfecho desconhecido na retomada de referência",
			"transactionId", resolution.TransactionID, "outcome", string(resolution.Outcome))
	}
}

func (w *ReferenceWorker) bump(apply func(*ReferenceStats)) {
	w.mu.Lock()
	apply(&w.stats)
	w.mu.Unlock()
}

var _ ports.HealthChecker = (*ReferenceWorker)(nil)

// Name identifica o worker nos logs e no health.
func (w *ReferenceWorker) Name() string { return "reference-worker" }

// Check sempre responde saudável.
//
// O worker de referências não tem dependência externa própria: ele usa o
// PostgreSQL, que o readiness já verifica. Um verificador aqui só duplicaria a
// mesma checagem sob outro nome.
func (w *ReferenceWorker) Check(context.Context) error { return nil }

// ReferenceRunner adapta o worker ao ciclo de vida da aplicação.
type ReferenceRunner struct {
	worker *ReferenceWorker
	logger *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewReferenceRunner cria o runner.
func NewReferenceRunner(worker *ReferenceWorker, logger *slog.Logger) *ReferenceRunner {
	return &ReferenceRunner{worker: worker, logger: logger}
}

// Start inicia o laço numa goroutine e retorna imediatamente.
//
// Um runner sem worker é um no-op: mantém o grafo de composição idêntico com o
// worker ligado ou desligado.
func (r *ReferenceRunner) Start(_ context.Context) error {
	if r.worker == nil {
		r.logger.Info("worker de referências não iniciado (desabilitado)")
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
		if err := r.worker.Run(runCtx); err != nil {
			r.logger.Error("worker de referências terminou com erro", "err", err)
		}
	}()
	return nil
}

// Stop cancela o laço e aguarda a conclusão.
//
// A espera garante que a tentativa em andamento termine antes de o pool do banco
// ser fechado pelo hook registrado antes deste. Uma tentativa interrompida não
// deixa estado parcial: ela é uma transação, e o rollback devolve tudo.
func (r *ReferenceRunner) Stop(ctx context.Context) error {
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
		r.logger.Info("worker de referências encerrado de forma limpa")
	case <-ctx.Done():
		return fmt.Errorf("worker de referências não encerrou dentro do prazo: %w", ctx.Err())
	}
	return nil
}
