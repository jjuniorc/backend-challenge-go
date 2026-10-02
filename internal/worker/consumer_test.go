package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// ── dublês ───────────────────────────────────────────────────────────
//
// A fonte é SIMULADA porque o alvo aqui é a LÓGICA do laço (classificação,
// backoff, encerramento), impossível de provocar de forma determinística contra
// um broker real. O caminho feliz contra o SQS REAL é coberto em
// internal/broker/sqs_source_integration_test.go (F7d): este dublê não substitui
// o broker, apenas controla o tempo e a falha.

type releaseCall struct {
	handle string
	delay  time.Duration
}

type deadLetterCall struct {
	messageID string
	reason    string
}

type fakeSource struct {
	mu      sync.Mutex
	batches [][]ports.InboundMessage
	calls   int

	// receiveErr é devolvido na PRIMEIRA chamada de Receive.
	receiveErr error
	// onReceive é chamado no início de cada Receive (permite cancelar o
	// contexto exatamente no ponto desejado do teste).
	onReceive func()

	deleted      []string
	released     []releaseCall
	deadLettered []deadLetterCall
}

func newFakeSource(batches ...[]ports.InboundMessage) *fakeSource {
	return &fakeSource{batches: batches}
}

func (f *fakeSource) Name() string                { return "fake" }
func (f *fakeSource) Check(context.Context) error { return nil }

func (f *fakeSource) Receive(ctx context.Context, _ int, _ time.Duration) ([]ports.InboundMessage, error) {
	f.mu.Lock()
	f.calls++
	firstCall := f.calls == 1
	hook := f.onReceive
	f.mu.Unlock()

	if hook != nil {
		hook()
	}

	f.mu.Lock()
	if firstCall && f.receiveErr != nil {
		err := f.receiveErr
		f.receiveErr = nil
		f.mu.Unlock()
		return nil, err
	}
	if len(f.batches) > 0 {
		batch := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return batch, nil
	}
	f.mu.Unlock()

	// Sem mais lotes: long polling até o contexto ser cancelado, como o broker
	// faria. Torna o término do laço determinístico no teste.
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeSource) Delete(_ context.Context, receiptHandle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, receiptHandle)
	return nil
}

func (f *fakeSource) Release(_ context.Context, receiptHandle string, delay time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, releaseCall{handle: receiptHandle, delay: delay})
	return nil
}

func (f *fakeSource) DeadLetter(_ context.Context, msg ports.InboundMessage, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadLettered = append(f.deadLettered, deadLetterCall{messageID: msg.MessageID, reason: reason})
	return nil
}

func (f *fakeSource) deletedHandles() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

func (f *fakeSource) releasedCalls() []releaseCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]releaseCall(nil), f.released...)
}

func (f *fakeSource) deadLetterCalls() []deadLetterCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]deadLetterCall(nil), f.deadLettered...)
}

type fakeProcessor struct {
	mu    sync.Mutex
	calls int
	seen  []InboundTransaction
	err   error
	// onProcess permite bloquear o tratamento para testar o encerramento.
	onProcess func(ctx context.Context) error
}

func (p *fakeProcessor) ProcessInbound(ctx context.Context, _ ports.InboundMessage, in InboundTransaction) error {
	p.mu.Lock()
	p.calls++
	p.seen = append(p.seen, in)
	hook, err := p.onProcess, p.err
	p.mu.Unlock()

	if hook != nil {
		return hook(ctx)
	}
	return err
}

func (p *fakeProcessor) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *fakeProcessor) transactions() []InboundTransaction {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]InboundTransaction(nil), p.seen...)
}

// ── helpers ──────────────────────────────────────────────────────────

func testConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Name:            "test-consumer",
		MaxMessages:     10,
		WaitTime:        time.Millisecond,
		MaxReceiveCount: 5,
		BaseBackoff:     2 * time.Second,
		MaxBackoff:      time.Minute,
		HandlingTimeout: 10 * time.Second,
	}
}

func messageBody(t *testing.T, messageID string) []byte {
	t.Helper()
	body := map[string]any{
		"messageId":  messageID,
		"type":       TypeTransactionRequested,
		"occurredAt": "2026-09-08T12:00:00.000Z",
		"data": map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": "ext-" + messageID,
			"idempotencyKey":        "provider-a:" + messageID,
			"playerId":              "player-1",
			"walletId":              "0192f291-27dd-7d3f-8071-5f8685deef37",
			"roundId":               "round-987",
			"gameId":                "fortune-chimp",
			"kind":                  "BET",
			"money":                 map[string]any{"amount": "25.00", "currency": "BRL"},
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("montando o corpo: %v", err)
	}
	return raw
}

func inboundMessage(t *testing.T, messageID string, receiveCount int) ports.InboundMessage {
	t.Helper()
	return ports.InboundMessage{
		MessageID:     messageID,
		ReceiptHandle: "handle-" + messageID,
		Body:          messageBody(t, messageID),
		ReceiveCount:  receiveCount,
		GroupID:       "0192f291-27dd-7d3f-8071-5f8685deef37",
		ReceivedAt:    time.Now().UTC(),
	}
}

// runConsumer roda o laço até ele terminar, com um watchdog contra travamento.
func runConsumer(t *testing.T, consumer *SQSConsumer, ctx context.Context) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run devolveu erro: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("o consumidor não terminou dentro do prazo")
	}
}

// ── caminho feliz ────────────────────────────────────────────────────

func TestValidMessageIsProcessedAndDeleted(t *testing.T) {
	source := newFakeSource([]ports.InboundMessage{
		inboundMessage(t, "msg-1", 1),
		inboundMessage(t, "msg-2", 1),
	})
	processor := &fakeProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, testConsumerConfig(), discardTestLogger())

	// Após consumir o lote, o Receive seguinte bloqueia no long polling; o
	// cancelamento é o que encerra o laço.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	if processor.callCount() != 2 {
		t.Fatalf("tratamentos = %d, quer 2", processor.callCount())
	}
	if got := len(source.deletedHandles()); got != 2 {
		t.Fatalf("mensagens removidas = %d, quer 2", got)
	}
	if got := len(source.releasedCalls()); got != 0 {
		t.Fatalf("mensagens devolvidas = %d, quer 0", got)
	}
	if got := len(source.deadLetterCalls()); got != 0 {
		t.Fatalf("mensagens na DLQ = %d, quer 0", got)
	}

	stats := consumer.Stats()
	if stats.Received != 2 || stats.Processed != 2 || stats.Retried != 0 || stats.DeadLettered != 0 {
		t.Fatalf("contadores = %+v", stats)
	}

	// O comando chegou ao tratamento com o hash de transporte calculado.
	seen := processor.transactions()
	if seen[0].PayloadHash == "" {
		t.Fatal("o tratamento deveria receber o hash do corpo bruto")
	}
	if seen[0].MessageID != "msg-1" {
		t.Fatalf("messageId = %q, quer msg-1", seen[0].MessageID)
	}
}

// ── mensagem inválida ────────────────────────────────────────────────

func TestInvalidMessageGoesToDLQWithoutProcessing(t *testing.T) {
	broken := ports.InboundMessage{
		MessageID:     "msg-broken",
		ReceiptHandle: "handle-msg-broken",
		Body:          []byte(`{"messageId":"msg-broken","type":"OutroEvento"}`),
		ReceiveCount:  1,
		ReceivedAt:    time.Now().UTC(),
	}
	source := newFakeSource([]ports.InboundMessage{broken})
	processor := &fakeProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, testConsumerConfig(), discardTestLogger())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	// O corpo malformado não pode chegar ao caso de uso.
	if processor.callCount() != 0 {
		t.Fatalf("tratamentos = %d, quer 0 (mensagem inválida não é tratada)", processor.callCount())
	}
	if got := len(source.deletedHandles()); got != 0 {
		t.Fatalf("mensagens removidas = %d, quer 0", got)
	}
	calls := source.deadLetterCalls()
	if len(calls) != 1 {
		t.Fatalf("mensagens na DLQ = %d, quer 1", len(calls))
	}
	if !strings.Contains(calls[0].reason, "mensagem inválida") {
		t.Fatalf("razão = %q, deveria indicar mensagem inválida", calls[0].reason)
	}
	if !strings.Contains(calls[0].reason, "SQS_UNKNOWN_TYPE") {
		t.Fatalf("razão = %q, deveria trazer o código do erro", calls[0].reason)
	}
	if stats := consumer.Stats(); stats.InvalidMessages != 1 {
		t.Fatalf("InvalidMessages = %d, quer 1", stats.InvalidMessages)
	}
}

// ── classificação e backoff ──────────────────────────────────────────

func TestTransientErrorReleasesWithExponentialBackoff(t *testing.T) {
	transient := domainerr.New(domainerr.KindTransient, "TEST_TRANSIENT", "banco indisponível")

	// Mesma mensagem em três entregas sucessivas: 1, 2 e 3.
	source := newFakeSource(
		[]ports.InboundMessage{inboundMessage(t, "msg-1", 1)},
		[]ports.InboundMessage{inboundMessage(t, "msg-1", 2)},
		[]ports.InboundMessage{inboundMessage(t, "msg-1", 3)},
	)
	processor := &fakeProcessor{err: transient}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, testConsumerConfig(), discardTestLogger())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	releases := source.releasedCalls()
	if len(releases) != 3 {
		t.Fatalf("devoluções = %d, quer 3", len(releases))
	}
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second}
	for i, call := range releases {
		if call.delay != want[i] {
			t.Errorf("devolução %d: delay = %s, quer %s", i+1, call.delay, want[i])
		}
	}
	if got := len(source.deletedHandles()); got != 0 {
		t.Fatalf("mensagens removidas = %d, quer 0 (falha transitória não confirma)", got)
	}
	if got := len(source.deadLetterCalls()); got != 0 {
		t.Fatalf("mensagens na DLQ = %d, quer 0 (falha transitória não é permanente)", got)
	}
	if stats := consumer.Stats(); stats.Retried != 3 {
		t.Fatalf("Retried = %d, quer 3", stats.Retried)
	}
}

func TestBackoffIsCappedAtMax(t *testing.T) {
	consumer := NewSQSConsumer(newFakeSource(), &fakeProcessor{}, ConsumerConfig{
		BaseBackoff: time.Second,
		MaxBackoff:  5 * time.Second,
	}, discardTestLogger())

	cases := []struct {
		receiveCount int
		want         time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 5 * time.Second}, // 8s excederia o teto
		{5, 5 * time.Second},
	}
	for _, c := range cases {
		if got := consumer.backoff(c.receiveCount); got != c.want {
			t.Errorf("backoff(%d) = %s, quer %s", c.receiveCount, got, c.want)
		}
	}
}

func TestPermanentErrorGoesToDLQ(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want action
	}{
		{"entrada inválida", domainerr.New(domainerr.KindInvalid, "TEST_INVALID", "campo inválido"), actionDeadLetter},
		{"conflito", domainerr.New(domainerr.KindConflict, "TEST_CONFLICT", "chave reutilizada"), actionDeadLetter},
		{"regra violada", domainerr.New(domainerr.KindRuleViolation, "TEST_RULE", "regra"), actionDeadLetter},
		{"saldo insuficiente", domainerr.New(domainerr.KindInsufficientFunds, "TEST_FUNDS", "sem saldo"), actionDeadLetter},
		{"não encontrado", domainerr.New(domainerr.KindNotFound, "TEST_404", "ausente"), actionDeadLetter},
		{"moeda incompatível", domainerr.New(domainerr.KindCurrencyMismatch, "TEST_CUR", "moeda"), actionDeadLetter},
		{"estado inválido", domainerr.New(domainerr.KindInvalidState, "TEST_STATE", "estado"), actionDeadLetter},
		{"proibido", domainerr.New(domainerr.KindForbidden, "TEST_403", "proibido"), actionDeadLetter},
		{"transitório", domainerr.New(domainerr.KindTransient, "TEST_TRANSIENT", "rede"), actionRetry},
		{"interno", domainerr.New(domainerr.KindInternal, "TEST_INTERNAL", "defeito"), actionRetry},
		// Erro comum (não-domínio): o banco caiu, por exemplo. Repetir é a única
		// chance de sucesso, então é transitório por precaução.
		{"erro comum", errors.New("conexão recusada"), actionRetry},
		{"contexto cancelado", context.Canceled, actionRetry},
		{"prazo excedido", context.DeadlineExceeded, actionRetry},
		{"sem erro", nil, actionDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classify(tt.err); got != tt.want {
				t.Fatalf("classify = %s, quer %s", got, tt.want)
			}
		})
	}
}

func TestPermanentErrorInProcessingGoesToDLQ(t *testing.T) {
	permanent := domainerr.New(domainerr.KindInvalid, "TEST_INVALID", "campo inválido")
	source := newFakeSource([]ports.InboundMessage{inboundMessage(t, "msg-1", 1)})
	processor := &fakeProcessor{err: permanent}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, testConsumerConfig(), discardTestLogger())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	if got := len(source.deadLetterCalls()); got != 1 {
		t.Fatalf("mensagens na DLQ = %d, quer 1", got)
	}
	if got := len(source.releasedCalls()); got != 0 {
		t.Fatalf("devoluções = %d, quer 0", got)
	}
	if got := len(source.deletedHandles()); got != 0 {
		t.Fatalf("mensagens removidas = %d, quer 0", got)
	}
}

// ── tentativas esgotadas ─────────────────────────────────────────────

func TestRetriesExhaustedGoesToDLQWithoutProcessing(t *testing.T) {
	config := testConsumerConfig()
	config.MaxReceiveCount = 5

	// A entrega de número 5 já é o limite: o tratamento não deve nem começar.
	source := newFakeSource([]ports.InboundMessage{inboundMessage(t, "msg-1", 5)})
	processor := &fakeProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, config, discardTestLogger())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	if processor.callCount() != 0 {
		t.Fatalf("tratamentos = %d, quer 0 (tentativas esgotadas não reprocessam)", processor.callCount())
	}
	calls := source.deadLetterCalls()
	if len(calls) != 1 {
		t.Fatalf("mensagens na DLQ = %d, quer 1", len(calls))
	}
	if !strings.Contains(calls[0].reason, "tentativas esgotadas") {
		t.Fatalf("razão = %q, deveria indicar tentativas esgotadas", calls[0].reason)
	}
	if !strings.Contains(calls[0].reason, CodeRetriesExhausted) {
		t.Fatalf("razão = %q, deveria trazer %s", calls[0].reason, CodeRetriesExhausted)
	}
	if stats := consumer.Stats(); stats.DeadLettered != 1 {
		t.Fatalf("DeadLettered = %d, quer 1", stats.DeadLettered)
	}
}

func TestLastAttemptIsStillProcessed(t *testing.T) {
	config := testConsumerConfig()
	config.MaxReceiveCount = 5

	// A entrega 4 ainda tem chance: é tratada normalmente.
	source := newFakeSource([]ports.InboundMessage{inboundMessage(t, "msg-1", 4)})
	processor := &fakeProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, config, discardTestLogger())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	if processor.callCount() != 1 {
		t.Fatalf("tratamentos = %d, quer 1", processor.callCount())
	}
	if got := len(source.deletedHandles()); got != 1 {
		t.Fatalf("mensagens removidas = %d, quer 1", got)
	}
	if got := len(source.deadLetterCalls()); got != 0 {
		t.Fatalf("mensagens na DLQ = %d, quer 0", got)
	}
}

// ── encerramento (SIGTERM) ───────────────────────────────────────────

func TestShutdownReleasesMessagesNotYetStarted(t *testing.T) {
	// O contexto é cancelado exatamente quando o lote chega: as duas mensagens
	// ainda não começaram, então devem ser devolvidas sem tratamento.
	source := newFakeSource([]ports.InboundMessage{
		inboundMessage(t, "msg-1", 1),
		inboundMessage(t, "msg-2", 1),
	})
	processor := &fakeProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	source.onReceive = cancel

	consumer := NewSQSConsumer(source, processor, testConsumerConfig(), discardTestLogger())
	runConsumer(t, consumer, ctx)

	if processor.callCount() != 0 {
		t.Fatalf("tratamentos = %d, quer 0 (mensagens não iniciadas não são tratadas)", processor.callCount())
	}
	releases := source.releasedCalls()
	if len(releases) != 2 {
		t.Fatalf("devoluções = %d, quer 2", len(releases))
	}
	for _, call := range releases {
		if call.delay != 0 {
			t.Fatalf("delay = %s, quer 0 (reentrega imediata no encerramento)", call.delay)
		}
	}
	if got := len(source.deletedHandles()); got != 0 {
		t.Fatalf("mensagens removidas = %d, quer 0", got)
	}
}

func TestInFlightMessageCompletesAfterShutdown(t *testing.T) {
	// Diferente do caso anterior: a mensagem JÁ começou quando o encerramento
	// chega. O tratamento usa contexto independente, então ele é CONCLUÍDO e a
	// mensagem é confirmada — em vez de abortada no meio e reentregue.
	source := newFakeSource([]ports.InboundMessage{inboundMessage(t, "msg-1", 1)})

	started := make(chan struct{})
	finish := make(chan struct{})
	processor := &fakeProcessor{
		onProcess: func(context.Context) error {
			close(started)
			<-finish
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, testConsumerConfig(), discardTestLogger())

	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	// Espera o tratamento começar e só então cancela (simula SIGTERM no meio).
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("o tratamento não começou")
	}
	cancel()
	close(finish)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run devolveu erro: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("o consumidor não terminou dentro do prazo")
	}

	if got := len(source.deletedHandles()); got != 1 {
		t.Fatalf("mensagens removidas = %d, quer 1 (o trabalho em andamento deveria ser confirmado)", got)
	}
	if got := len(source.releasedCalls()); got != 0 {
		t.Fatalf("devoluções = %d, quer 0 (mensagem já concluída não volta)", got)
	}
}

// ── robustez do laço ─────────────────────────────────────────────────

func TestReceiveFailureDoesNotStopTheLoop(t *testing.T) {
	// O primeiro recebimento falha (broker oscilando); o laço deve esperar e
	// continuar, tratando o lote seguinte normalmente.
	source := newFakeSource([]ports.InboundMessage{inboundMessage(t, "msg-1", 1)})
	source.receiveErr = errors.New("broker temporariamente indisponível")
	processor := &fakeProcessor{}

	config := testConsumerConfig()
	config.BaseBackoff = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	consumer := NewSQSConsumer(source, processor, config, discardTestLogger())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	runConsumer(t, consumer, ctx)

	if processor.callCount() != 1 {
		t.Fatalf("tratamentos = %d, quer 1 (o laço deveria sobreviver à falha de recebimento)", processor.callCount())
	}
	if got := len(source.deletedHandles()); got != 1 {
		t.Fatalf("mensagens removidas = %d, quer 1", got)
	}
}

func TestRunnerWithoutConsumerIsNoop(t *testing.T) {
	runner := NewConsumerRunner(nil, discardTestLogger())
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := runner.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestRunnerStartsAndStopsCleanly(t *testing.T) {
	source := newFakeSource()
	consumer := NewSQSConsumer(source, &fakeProcessor{}, testConsumerConfig(), discardTestLogger())
	runner := NewConsumerRunner(consumer, discardTestLogger())

	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runner.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Parar duas vezes não pode falhar nem travar.
	if err := runner.Stop(ctx); err != nil {
		t.Fatalf("segundo Stop: %v", err)
	}
}

// discardTestLogger devolve um logger silencioso para os testes unitarios.
//
// O nome e DISTINTO de discardLogger (definido em outbox_integration_test.go,
// que tem //go:build integration) de proposito: os dois arquivos compilam
// juntos quando a suite roda com -tags integration, e um nome compartilhado
// seria uma definicao duplicada.
func discardTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
