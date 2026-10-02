//go:build integration

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
)

const testWalletID = "00000000-0000-7000-8000-0000000000f0"

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testClock() time.Time {
	// Relogio REAL: os eventos sao inseridos com next_attempt_at = now()
	// do Postgres, e a reserva so devolve o que satisfaz
	// next_attempt_at <= Now. Um instante artificial no passado nunca casa.
	return time.Now().UTC()
}

// insertEvents grava n eventos pendentes com identidade determinística.
func insertEvents(t *testing.T, store ports.Store, n int) []string {
	t.Helper()
	ctx := context.Background()
	now := testClock()

	eventIDs := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		eventID := fmt.Sprintf("00000000-0000-7000-8000-%012d", i)
		env, err := events.NewWalletBalanceChanged(events.Meta{
			EventID:       eventID,
			CorrelationID: "corr-outbox-test",
			OccurredAt:    now,
		}, events.BalanceChangedParams{
			WalletID:      testWalletID,
			TransactionID: fmt.Sprintf("00000000-0000-7000-8000-0000000000b%d", i%10),
			Direction:     ledger.DirectionCredit,
			Money:         mustBRL(t, "1.00"),
			BalanceBefore: mustBRL(t, "0.00"),
			BalanceAfter:  mustBRL(t, "1.00"),
			WalletVersion: int64(i),
		})
		if err != nil {
			t.Fatalf("montando evento %d: %v", i, err)
		}
		if err := store.Outbox().Insert(ctx, env); err != nil {
			t.Fatalf("inserindo evento %d: %v", i, err)
		}
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs
}

func mustBRL(t *testing.T, value string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(value, "BRL")
	if err != nil {
		t.Fatalf("money(%q): %v", value, err)
	}
	return m
}

// ── publisher de teste ───────────────────────────────────────────────

// recordingPublisher registra cada eventId publicado e, opcionalmente, falha.
//
// É usado para INJETAR FALHA do broker, cenário que não é possível reproduzir
// derrubando o MiniStack dentro de um teste. O caminho feliz com SQS real é
// coberto por internal/broker/sqs_integration_test.go — este fake não substitui
// o broker, apenas provoca a indisponibilidade.
type recordingPublisher struct {
	mu        sync.Mutex
	published map[string]int
	attempts  map[string]int
	failWith  error
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{
		published: map[string]int{},
		attempts:  map[string]int{},
	}
}

func (p *recordingPublisher) Name() string                { return "recording" }
func (p *recordingPublisher) Check(context.Context) error { return nil }

func (p *recordingPublisher) Publish(_ context.Context, record ports.OutboxRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts[record.EventID]++
	if p.failWith != nil {
		return p.failWith
	}
	p.published[record.EventID]++
	return nil
}

func (p *recordingPublisher) publishedCount(eventID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.published[eventID]
}

func (p *recordingPublisher) attemptsCount(eventID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.attempts[eventID]
}

func (p *recordingPublisher) totalPublished() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, count := range p.published {
		total += count
	}
	return total
}

// ── reserva exclusiva (múltiplos publishers) ─────────────────────────

// TestClaimBatchIsExclusiveBetweenPublishers prova que dois publicadores
// disputando a mesma outbox recebem conjuntos DISJUNTOS: nenhum evento é
// reservado por dois ao mesmo tempo.
func TestClaimBatchIsExclusiveBetweenPublishers(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const total = 20
	eventIDs := insertEvents(t, store, total)

	const claimers = 4
	var (
		mu      sync.Mutex
		seen    = map[string]int{}
		wg      sync.WaitGroup
		roundWG sync.WaitGroup
	)
	start := make(chan struct{})
	now := testClock()

	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			claimerID := fmt.Sprintf("publisher-%d", idx)
			<-start
			// Cada publicador roda vários ciclos até a outbox esvaziar.
			for round := 0; round < 12; round++ {
				records, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
					ClaimerID: claimerID,
					Limit:     3,
					Now:       now,
					Lease:     time.Minute,
				})
				if err != nil {
					t.Errorf("publisher %d: ClaimBatch: %v", idx, err)
					return
				}
				mu.Lock()
				for _, record := range records {
					seen[record.EventID]++
				}
				mu.Unlock()
				if len(records) == 0 {
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	roundWG.Wait()

	// Nenhum evento pode ter sido reservado por dois publicadores.
	for _, eventID := range eventIDs {
		if seen[eventID] > 1 {
			t.Fatalf("evento %s foi reservado %d vezes (deveria ser no máximo 1)", eventID, seen[eventID])
		}
	}
	if len(seen) != total {
		t.Fatalf("eventos reservados = %d, quer %d", len(seen), total)
	}
}

// TestClaimBatchSkipsEventsHeldByAnotherClaimer prova que uma reserva ATIVA
// esconde o evento dos demais.
func TestClaimBatchSkipsEventsHeldByAnotherClaimer(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	insertEvents(t, store, 5)
	now := testClock()

	first, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-1", Limit: 5, Now: now, Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("primeiro ClaimBatch: %v", err)
	}
	if len(first) != 5 {
		t.Fatalf("primeiro lote = %d, quer 5", len(first))
	}

	second, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-2", Limit: 5, Now: now, Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("segundo ClaimBatch: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("segundo lote = %d, quer 0 (eventos estão reservados)", len(second))
	}
}

// TestExpiredLeaseIsRecoveredByAnotherPublisher prova a RECUPERAÇÃO de trabalho
// abandonado: uma instância que caiu entre o commit e a publicação deixa uma
// reserva que vence, e outra instância assume os eventos.
func TestExpiredLeaseIsRecoveredByAnotherPublisher(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	insertEvents(t, store, 3)
	now := testClock()

	// publisher-1 reserva com lease curto e "morre" sem publicar.
	held, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-1", Limit: 3, Now: now, Lease: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("reserva inicial: %v", err)
	}
	if len(held) != 3 {
		t.Fatalf("reserva inicial = %d, quer 3", len(held))
	}

	// Antes de vencer, ninguém mais consegue reservar.
	stillHeld, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-2", Limit: 3, Now: now.Add(10 * time.Second), Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("reserva durante o lease: %v", err)
	}
	if len(stillHeld) != 0 {
		t.Fatalf("reserva durante o lease = %d, quer 0", len(stillHeld))
	}

	// Depois de vencer, publisher-2 assume os MESMOS eventos.
	recovered, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-2", Limit: 3, Now: now.Add(time.Minute), Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("recuperação: %v", err)
	}
	if len(recovered) != 3 {
		t.Fatalf("eventos recuperados = %d, quer 3", len(recovered))
	}

	heldByID := map[string]bool{}
	for _, record := range held {
		heldByID[record.EventID] = true
	}
	for _, record := range recovered {
		if !heldByID[record.EventID] {
			t.Fatalf("evento recuperado (%s) não estava na reserva abandonada", record.EventID)
		}
	}
}

// ── vários workers, publicação exatamente uma vez ────────────────────

// TestWorkersPublishEachEventExactlyOnce roda três publicadores concorrentes
// sobre a MESMA outbox e verifica que cada evento é publicado uma única vez.
func TestWorkersPublishEachEventExactlyOnce(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const total = 30
	eventIDs := insertEvents(t, store, total)

	recorder := newRecordingPublisher()
	logger := discardLogger()

	const workers = 3
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			publisher := NewOutboxPublisher(store, recorder, OutboxConfig{
				ClaimerID: fmt.Sprintf("worker-%d", idx),
				BatchSize: 4,
				Interval:  time.Millisecond,
				Lease:     time.Minute,
			}, logger)

			<-start
			for round := 0; round < 20; round++ {
				if err := publisher.publishOnce(ctx); err != nil {
					t.Errorf("worker %d: publishOnce: %v", idx, err)
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	pending, err := store.Outbox().CountPending(ctx)
	if err != nil {
		t.Fatalf("CountPending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("eventos pendentes = %d, quer 0", pending)
	}

	if got := recorder.totalPublished(); got != total {
		t.Fatalf("publicações = %d, quer %d", got, total)
	}
	for _, eventID := range eventIDs {
		if count := recorder.publishedCount(eventID); count != 1 {
			t.Fatalf("evento %s publicado %d vezes, quer exatamente 1", eventID, count)
		}
	}
}

// TestRepublishKeepsSameEventID prova que a recuperação após falha REPUBLICA o
// mesmo evento, preservando o eventId — que é o MessageDeduplicationId no SQS.
func TestRepublishKeepsSameEventID(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	insertEvents(t, store, 1)
	now := testClock()

	first, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-1", Limit: 1, Now: now, Lease: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("reserva inicial: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("reserva inicial = %d, quer 1", len(first))
	}

	// A instância cai sem publicar; o lease vence e outra assume.
	second, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "publisher-2", Limit: 1, Now: now.Add(time.Minute), Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("recuperação: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("recuperação = %d, quer 1", len(second))
	}

	if first[0].EventID != second[0].EventID {
		t.Fatalf("eventId mudou na republicação: %s -> %s", first[0].EventID, second[0].EventID)
	}
	if string(first[0].Payload) != string(second[0].Payload) {
		t.Fatal("payload mudou na republicação (o snapshot deveria ser imutável)")
	}
	if first[0].AggregateID != second[0].AggregateID {
		t.Fatal("aggregateId mudou na republicação (MessageGroupId seria diferente)")
	}
}

// ── falha do broker: backoff e auditoria ─────────────────────────────

func TestFailedPublishIsRescheduledWithBackoff(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const total = 1
	eventIDs := insertEvents(t, store, total)

	recorder := newRecordingPublisher()
	recorder.failWith = errors.New("broker indisponível")

	publisher := NewOutboxPublisher(store, recorder, OutboxConfig{
		ClaimerID:   "worker-1",
		BatchSize:   5,
		Lease:       time.Minute,
		MaxAttempts: 3,
		BaseBackoff: time.Minute,
		MaxBackoff:  time.Hour,
	}, discardLogger())

	if err := publisher.publishOnce(ctx); err != nil {
		t.Fatalf("publishOnce não deveria propagar falha de broker: %v", err)
	}

	if got := recorder.attemptsCount(eventIDs[0]); got != 1 {
		t.Fatalf("tentativas = %d, quer 1", got)
	}
	if got := recorder.totalPublished(); got != 0 {
		t.Fatalf("publicações = %d, quer 0", got)
	}

	// O evento continua PENDENTE (nenhuma perda) e com a auditoria da falha.
	var (
		attempts    int
		lastError   string
		nextAttempt *time.Time
	)
	if err := scanOutbox(ctx, t, eventIDs[0], &attempts, &lastError, &nextAttempt); err != nil {
		t.Fatalf("lendo a outbox: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts persistido = %d, quer 1", attempts)
	}
	if lastError == "" {
		t.Fatal("last_error deveria registrar a falha para auditoria")
	}
	if nextAttempt == nil {
		t.Fatal("next_attempt_at deveria ser reagendado")
	}

	pending, err := store.Outbox().CountPending(ctx)
	if err != nil {
		t.Fatalf("CountPending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pendentes = %d, quer 1 (a falha não pode perder o evento)", pending)
	}

	// Dentro da janela de backoff o evento não é elegível.
	now := time.Now().UTC()
	inBackoff, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "worker-2", Limit: 5, Now: now, Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("ClaimBatch durante o backoff: %v", err)
	}
	if len(inBackoff) != 0 {
		t.Fatalf("reservas durante o backoff = %d, quer 0", len(inBackoff))
	}

	// Depois do backoff, o evento volta a ser elegível.
	afterBackoff, err := store.Outbox().ClaimBatch(ctx, ports.ClaimRequest{
		ClaimerID: "worker-2", Limit: 5, Now: now.Add(2 * time.Minute), Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("ClaimBatch após o backoff: %v", err)
	}
	if len(afterBackoff) != 1 {
		t.Fatalf("reservas após o backoff = %d, quer 1", len(afterBackoff))
	}
	if afterBackoff[0].Attempts != 1 {
		t.Fatalf("attempts do registro = %d, quer 1", afterBackoff[0].Attempts)
	}
}

// TestSecondFailureExhaustsAttemptsAndKeepsEvent prova que, esgotadas as
// tentativas, o evento permanece na outbox para auditoria em vez de sumir.
func TestSecondFailureExhaustsAttemptsAndKeepsEvent(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	eventIDs := insertEvents(t, store, 1)

	recorder := newRecordingPublisher()
	recorder.failWith = errors.New("broker indisponível")

	publisher := NewOutboxPublisher(store, recorder, OutboxConfig{
		ClaimerID:   "worker-1",
		BatchSize:   5,
		Lease:       time.Minute,
		MaxAttempts: 1, // esgota na primeira falha
		BaseBackoff: time.Second,
		MaxBackoff:  time.Second,
	}, discardLogger())

	if err := publisher.publishOnce(ctx); err != nil {
		t.Fatalf("publishOnce: %v", err)
	}

	pending, err := store.Outbox().CountPending(ctx)
	if err != nil {
		t.Fatalf("CountPending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pendentes = %d, quer 1 (não pode ser descartado)", pending)
	}
	if got := recorder.attemptsCount(eventIDs[0]); got != 1 {
		t.Fatalf("tentativas = %d, quer 1", got)
	}
}

// ── helpers de leitura ───────────────────────────────────────────────

func scanOutbox(ctx context.Context, t *testing.T, eventID string, attempts *int, lastError *string, nextAttempt **time.Time) error {
	t.Helper()
	// A leitura usa o banco de teste do pacote (mesma base dos casos de uso).
	return testsupport.QueryOutbox(ctx, t, eventID, attempts, lastError, nextAttempt)
}
