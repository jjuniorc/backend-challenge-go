package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// CodeRetriesExhausted é registrado quando a mensagem já foi entregue o número
// de vezes previsto no redrive e o consumidor antecipa o envio à DLQ.
const CodeRetriesExhausted = "SQS_RETRIES_EXHAUSTED"

// sideTimeout é o prazo das confirmações na fila (Delete, Release, DeadLetter),
// que precisam acontecer mesmo com o contexto do laço já cancelado.
const sideTimeout = 5 * time.Second

// ConsumerConfig parametriza o laço do consumidor.
type ConsumerConfig struct {
	// Name identifica o consumidor na inbox (inbox_messages.consumer_name).
	Name string
	// MaxMessages é o tamanho do lote por recebimento (1 a 10, limite do SQS).
	MaxMessages int
	// WaitTime é o long polling por recebimento (limite do SQS: 20s).
	WaitTime time.Duration
	// MaxReceiveCount é o limite de entregas do redrive. Ao ALCANÇAR esse número
	// de recebimentos a mensagem vai para a DLQ sem novo tratamento: o mesmo
	// número governa o redrive da fila e o código, evitando duas verdades.
	MaxReceiveCount int
	// BaseBackoff é o atraso da primeira reentrega após falha transitória.
	BaseBackoff time.Duration
	// MaxBackoff é o teto do backoff exponencial.
	MaxBackoff time.Duration
	// HandlingTimeout é o prazo máximo de tratamento de UMA mensagem. No
	// encerramento, é também o prazo concedido ao trabalho em andamento.
	HandlingTimeout time.Duration
}

func (c ConsumerConfig) normalized() ConsumerConfig {
	if c.MaxMessages < 1 {
		c.MaxMessages = 10
	}
	if c.MaxMessages > 10 {
		c.MaxMessages = 10
	}
	if c.WaitTime < 0 {
		c.WaitTime = 0
	}
	if c.WaitTime > 20*time.Second {
		c.WaitTime = 20 * time.Second
	}
	if c.MaxReceiveCount < 1 {
		c.MaxReceiveCount = 5
	}
	if c.BaseBackoff <= 0 {
		c.BaseBackoff = 2 * time.Second
	}
	if c.MaxBackoff < c.BaseBackoff {
		c.MaxBackoff = time.Minute
	}
	if c.HandlingTimeout <= 0 {
		c.HandlingTimeout = 30 * time.Second
	}
	if c.Name == "" {
		c.Name = "wager-transactions"
	}
	return c
}

// InboundProcessor trata UMA mensagem de entrada.
//
// O contrato é transacional e é o coração da entrega: a implementação DEVE
// registrar a inbox, aplicar as alterações de domínio (saldo, ledger) e gravar a
// outbox em UMA única transação SQL, e só então retornar nil. É esse nil que
// autoriza o consumidor a remover a mensagem da fila.
//
// Retornar nil para uma rejeição de negócio é CORRETO: a rejeição foi persistida
// com seu evento e é terminal — a mensagem deve mesmo ser removida.
type InboundProcessor interface {
	ProcessInbound(ctx context.Context, msg ports.InboundMessage, in InboundTransaction) error
}

// action é o desfecho do tratamento de uma mensagem.
type action int

const (
	// actionDelete confirma o tratamento durável (remove da fila).
	actionDelete action = iota
	// actionRetry devolve a mensagem para reentrega com backoff.
	actionRetry
	// actionDeadLetter encaminha a mensagem para a DLQ.
	actionDeadLetter
)

func (a action) String() string {
	switch a {
	case actionDelete:
		return "delete"
	case actionRetry:
		return "retry"
	default:
		return "dead-letter"
	}
}

// classify decide o desfecho a partir do erro do tratamento.
//
// A regra é conservadora de propósito: só é PERMANENTE o que o domínio
// classificou como entrada inválida, conflito, regra violada ou ausência de
// permissão — casos em que repetir a entrega produziria exatamente o mesmo
// resultado. Tudo o mais é tratado como transitório, porque uma falha de banco,
// de rede ou um defeito de programação AINDA podem ter sucesso na próxima
// entrega; o teto de tentativas garante que isso não vire laço infinito.
func classify(err error) action {
	if err == nil {
		return actionDelete
	}
	// Cancelamento/prazo do próprio tratamento: o trabalho não foi concluído e
	// nada indica que ele seria inválido — devolve para reentrega.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return actionRetry
	}

	switch domainerr.KindOf(err) {
	case domainerr.KindTransient, domainerr.KindInternal:
		// KindInternal também cobre o que NÃO é erro de domínio (falha de banco,
		// de rede, defeito de programação).
		return actionRetry
	default:
		return actionDeadLetter
	}
}

// ConsumerStats são os contadores do consumidor (base das métricas).
type ConsumerStats struct {
	Received        int64
	Processed       int64
	Retried         int64
	DeadLettered    int64
	InvalidMessages int64
}

// SQSConsumer consome a fila de entrada e aplica o tratamento transacional.
//
// Estratégia de falha:
//   - falha transitória -> Release com backoff exponencial (métrica de retries);
//   - falha permanente  -> DLQ com a razão registrada (mensagem inválida,
//     rejeição terminal, tentativas esgotadas);
//   - sucesso durável   -> Delete.
//
// O Delete é a ÚLTIMA operação. Uma queda entre o commit e o Delete deixa a
// mensagem voltar; como o tratamento é idempotente pela inbox, a reentrega vira
// replay e não movimenta o saldo duas vezes.
type SQSConsumer struct {
	source    ports.MessageSource
	processor InboundProcessor
	config    ConsumerConfig
	logger    *slog.Logger

	mu    sync.Mutex
	stats ConsumerStats
}

// NewSQSConsumer cria o consumidor.
func NewSQSConsumer(source ports.MessageSource, processor InboundProcessor, config ConsumerConfig, logger *slog.Logger) *SQSConsumer {
	return &SQSConsumer{
		source:    source,
		processor: processor,
		config:    config.normalized(),
		logger:    logger,
	}
}

// Name identifica o consumidor (health e log).
func (c *SQSConsumer) Name() string { return c.source.Name() }

// Check verifica a disponibilidade da fila de entrada.
func (c *SQSConsumer) Check(ctx context.Context) error { return c.source.Check(ctx) }

// Stats devolve uma cópia dos contadores.
func (c *SQSConsumer) Stats() ConsumerStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// Run executa o laço de consumo até o contexto ser cancelado.
//
// O laço é SEQUENCIAL por processo: o paralelismo entre carteiras vem das
// instâncias concorrentes somadas ao MessageGroupId da fila FIFO, não de
// goroutines internas. Isso mantém um único ponto de controle do encerramento.
func (c *SQSConsumer) Run(ctx context.Context) error {
	c.logger.Info("consumidor iniciado",
		"name", c.config.Name,
		"maxMessages", c.config.MaxMessages,
		"waitTime", c.config.WaitTime.String(),
		"maxReceiveCount", c.config.MaxReceiveCount,
		"handlingTimeout", c.config.HandlingTimeout.String(),
	)

	for {
		if ctx.Err() != nil {
			c.logger.Info("consumidor encerrado")
			return nil
		}

		messages, err := c.source.Receive(ctx, c.config.MaxMessages, c.config.WaitTime)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				c.logger.Info("consumidor encerrado")
				return nil
			}
			// Falha do broker não derruba o processo: espera e tenta de novo.
			c.logger.Error("recebimento de mensagens falhou", "err", err, "retryIn", c.config.BaseBackoff.String())
			if !sleepOrDone(ctx, c.config.BaseBackoff) {
				c.logger.Info("consumidor encerrado")
				return nil
			}
			continue
		}

		for _, msg := range messages {
			if ctx.Err() != nil {
				// Encerramento: devolver agora evita esperar o visibility
				// timeout só para a reentrega acontecer.
				c.releaseForShutdown(msg)
				continue
			}
			c.handleMessage(ctx, msg)
		}
	}
}

// handleMessage trata uma mensagem e devolve o desfecho aplicado.
func (c *SQSConsumer) handleMessage(ctx context.Context, msg ports.InboundMessage) action {
	c.bump(func(s *ConsumerStats) { s.Received++ })

	// O tratamento usa um contexto INDEPENDENTE do cancelamento do processo,
	// com prazo próprio. É o que garante o encerramento seguro exigido: no
	// SIGTERM, a mensagem em andamento é CONCLUÍDA dentro do prazo em vez de ser
	// abortada no meio — abortar deixaria trabalho parcial para a reentrega.
	handleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.config.HandlingTimeout)
	defer cancel()

	parsed, err := ParseInboundTransaction(msg.Body)
	if err != nil {
		// Corpo malformado não é corrigível por reentrega: é permanente.
		c.bump(func(s *ConsumerStats) { s.InvalidMessages++ })
		return c.deadLetter(msg, err, "mensagem inválida")
	}

	if msg.ReceiveCount >= c.config.MaxReceiveCount {
		// O SQS move a mensagem para a DLQ ao exceder o maxReceiveCount do
		// redrive. Antecipar aqui mantém o limite em um só lugar e evita uma
		// rodada de tratamento que já não teria chance de sucesso.
		exhausted := domainerr.New(domainerr.KindTransient, CodeRetriesExhausted,
			fmt.Sprintf("mensagem já entregue %d vez(es), limite %d", msg.ReceiveCount, c.config.MaxReceiveCount))
		return c.deadLetter(msg, exhausted, "tentativas esgotadas")
	}

	if err := c.processor.ProcessInbound(handleCtx, msg, parsed); err != nil {
		if classify(err) == actionDeadLetter {
			return c.deadLetter(msg, err, "falha permanente no tratamento")
		}
		return c.retry(ctx, msg, err)
	}

	c.bump(func(s *ConsumerStats) { s.Processed++ })
	return c.delete(ctx, msg)
}

func (c *SQSConsumer) delete(ctx context.Context, msg ports.InboundMessage) action {
	sideCtx, cancel := c.sideContext(ctx)
	defer cancel()

	if err := c.source.Delete(sideCtx, msg.ReceiptHandle); err != nil {
		// A confirmação falhou: a mensagem volta após o visibility timeout.
		// Como o tratamento já é durável, a reentrega cai na inbox e vira
		// replay — nenhuma movimentação duplicada.
		c.logger.Error("removendo a mensagem da fila falhou",
			"messageId", msg.MessageID, "err", err)
		return actionDelete
	}
	c.logger.Info("mensagem processada e removida da fila",
		"messageId", msg.MessageID, "groupId", msg.GroupID)
	return actionDelete
}

func (c *SQSConsumer) retry(ctx context.Context, msg ports.InboundMessage, cause error) action {
	delay := c.backoff(msg.ReceiveCount)
	sideCtx, cancel := c.sideContext(ctx)
	defer cancel()

	c.bump(func(s *ConsumerStats) { s.Retried++ })
	if err := c.source.Release(sideCtx, msg.ReceiptHandle, delay); err != nil {
		c.logger.Error("devolvendo a mensagem para reentrega falhou",
			"messageId", msg.MessageID, "err", err)
		return actionRetry
	}
	c.logger.Warn("falha transitória; mensagem devolvida para reentrega",
		"messageId", msg.MessageID,
		"receiveCount", msg.ReceiveCount,
		"retryIn", delay.String(),
		"err", cause)
	return actionRetry
}

func (c *SQSConsumer) deadLetter(msg ports.InboundMessage, cause error, reason string) action {
	// Contexto próprio (não derivado do laço): o encaminhamento à DLQ precisa
	// acontecer mesmo no encerramento.
	sideCtx, cancel := context.WithTimeout(context.Background(), sideTimeout)
	defer cancel()

	c.bump(func(s *ConsumerStats) { s.DeadLettered++ })
	fullReason := reason + ": " + cause.Error()
	if err := c.source.DeadLetter(sideCtx, msg, fullReason); err != nil {
		c.logger.Error("encaminhando a mensagem para a DLQ falhou",
			"messageId", msg.MessageID, "reason", reason, "err", err)
		return actionDeadLetter
	}
	c.logger.Error("mensagem encaminhada para a DLQ",
		"messageId", msg.MessageID, "receiveCount", msg.ReceiveCount, "reason", reason, "err", cause)
	return actionDeadLetter
}

// releaseForShutdown devolve uma mensagem NÃO INICIADA no encerramento.
func (c *SQSConsumer) releaseForShutdown(msg ports.InboundMessage) {
	sideCtx, cancel := context.WithTimeout(context.Background(), sideTimeout)
	defer cancel()

	if err := c.source.Release(sideCtx, msg.ReceiptHandle, 0); err != nil {
		c.logger.Error("liberando visibilidade no encerramento falhou",
			"messageId", msg.MessageID, "err", err)
		return
	}
	c.logger.Info("mensagem liberada para reentrega no encerramento", "messageId", msg.MessageID)
}

// sideContext deriva um contexto independente do laço, com prazo curto.
func (c *SQSConsumer) sideContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), sideTimeout)
}

// backoff é exponencial com teto, indexado pelo número de recebimentos.
func (c *SQSConsumer) backoff(receiveCount int) time.Duration {
	delay := c.config.BaseBackoff
	for i := 1; i < receiveCount; i++ {
		if delay >= c.config.MaxBackoff/2 {
			return c.config.MaxBackoff
		}
		delay *= 2
	}
	if delay > c.config.MaxBackoff {
		return c.config.MaxBackoff
	}
	return delay
}

func (c *SQSConsumer) bump(apply func(*ConsumerStats)) {
	c.mu.Lock()
	apply(&c.stats)
	c.mu.Unlock()
}

func sleepOrDone(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

var _ ports.HealthChecker = (*SQSConsumer)(nil)

// ConsumerRunner adapta o consumidor ao ciclo de vida da aplicação.
type ConsumerRunner struct {
	consumer *SQSConsumer
	logger   *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewConsumerRunner cria o runner.
func NewConsumerRunner(consumer *SQSConsumer, logger *slog.Logger) *ConsumerRunner {
	return &ConsumerRunner{consumer: consumer, logger: logger}
}

// Start inicia o laço numa goroutine e retorna imediatamente.
//
// Um runner sem consumidor é um no-op: mantém o grafo de composição idêntico
// com o consumo ligado ou desligado.
func (r *ConsumerRunner) Start(_ context.Context) error {
	if r.consumer == nil {
		r.logger.Info("consumidor não iniciado (desabilitado)")
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
		if err := r.consumer.Run(runCtx); err != nil {
			r.logger.Error("consumidor terminou com erro", "err", err)
		}
	}()
	return nil
}

// Stop cancela o laço e aguarda a conclusão.
//
// A espera é o que torna o encerramento seguro: o tratamento em andamento
// termina (dentro do HandlingTimeout) e sua confirmação na fila é emitida antes
// de o pool do banco ser fechado pelo hook registrado antes deste.
func (r *ConsumerRunner) Stop(ctx context.Context) error {
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
		r.logger.Info("consumidor encerrado de forma limpa")
	case <-ctx.Done():
		return fmt.Errorf("consumidor não encerrou dentro do prazo: %w", ctx.Err())
	}
	return nil
}
