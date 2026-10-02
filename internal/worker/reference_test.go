package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// resolutionStep é UM retorno do resolvedor: o resultado OU o erro da chamada.
//
// Este é o contrato do caso de uso — ResolveNextPendingReference devolve
// (PendingResolution{}, err) quando falha, nunca os dois. A primeira versão deste
// dublê mantinha resultados e erros em filas separadas, e por isso a primeira
// chamada devolvia o erro E consumia o primeiro resultado, que era descartado:
// o teste acusou o worker por um comportamento que só existia no dublê.
type resolutionStep struct {
	result usecase.PendingResolution
	err    error
}

type fakeResolver struct {
	mu     sync.Mutex
	steps  []resolutionStep
	calls  int
	onCall func(call int)
}

func (f *fakeResolver) ResolveNextPendingReference(context.Context) (usecase.PendingResolution, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls

	var step resolutionStep
	if len(f.steps) > 0 {
		step = f.steps[0]
		f.steps = f.steps[1:]
	}
	hook := f.onCall
	f.mu.Unlock()

	if hook != nil {
		hook(call)
	}
	return step.result, step.err
}

func (f *fakeResolver) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func testReferenceConfig() ReferenceConfig {
	return ReferenceConfig{Interval: time.Millisecond, ErrorBackoff: time.Millisecond}
}

// runReferenceWorker roda o laço até o contexto terminar, com watchdog.
func runReferenceWorker(t *testing.T, w *ReferenceWorker, ctx context.Context) {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run devolveu erro: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("o worker não terminou dentro do prazo")
	}
}

// ── construtores de passo ────────────────────────────────────────────

// idle é o resultado de "nada vencido", que é o caso normal.
var idle = resolutionStep{result: usecase.PendingResolution{Attempted: false}}

func resolvedStep(id string) resolutionStep {
	return resolutionStep{result: usecase.PendingResolution{
		Attempted: true, TransactionID: id, Outcome: usecase.OutcomeResolved, Attempts: 2,
	}}
}

func stepWithOutcome(id string, outcome usecase.ResolutionOutcome) resolutionStep {
	return resolutionStep{result: usecase.PendingResolution{
		Attempted: true, TransactionID: id, Outcome: outcome,
	}}
}

func errorStep(message string) resolutionStep {
	return resolutionStep{err: errors.New(message)}
}

// ── laço ─────────────────────────────────────────────────────────────

func TestReferenceWorkerDrainsBacklogWithoutWaitingForInterval(t *testing.T) {
	source := &fakeResolver{steps: []resolutionStep{
		resolvedStep("tx-1"), resolvedStep("tx-2"), resolvedStep("tx-3"), idle,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancela quando o resultado ocioso já foi consumido: prova que o laço
	// chegou ao fim do backlog.
	source.onCall = func(call int) {
		if call == 4 {
			cancel()
		}
	}

	worker := NewReferenceWorker(source, testReferenceConfig(), discardTestLogger())
	runReferenceWorker(t, worker, ctx)

	stats := worker.Stats()
	if stats.Resolved != 3 {
		t.Fatalf("Resolved = %d, quer 3", stats.Resolved)
	}
	if stats.Scans != 4 {
		t.Fatalf("Scans = %d, quer 4", stats.Scans)
	}
	// Drenagem: 4 chamadas seguidas, e não uma por intervalo.
	if got := source.callCount(); got != 4 {
		t.Fatalf("chamadas ao resolvedor = %d, quer 4", got)
	}
}

func TestReferenceWorkerCountsEachOutcome(t *testing.T) {
	source := &fakeResolver{steps: []resolutionStep{
		stepWithOutcome("tx-r", usecase.OutcomeResolved),
		stepWithOutcome("tx-s", usecase.OutcomeRescheduled),
		stepWithOutcome("tx-j", usecase.OutcomeRejected),
		stepWithOutcome("tx-e", usecase.OutcomeExhausted),
		idle,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	source.onCall = func(call int) {
		if call == 5 {
			cancel()
		}
	}

	worker := NewReferenceWorker(source, testReferenceConfig(), discardTestLogger())
	runReferenceWorker(t, worker, ctx)

	stats := worker.Stats()
	if stats.Resolved != 1 || stats.Rescheduled != 1 || stats.Rejected != 1 || stats.Exhausted != 1 {
		t.Fatalf("contadores = %+v", stats)
	}
	if stats.Errors != 0 {
		t.Fatalf("Errors = %d, quer 0", stats.Errors)
	}
}

// ── erro de infraestrutura ───────────────────────────────────────────

func TestReferenceWorkerBacksOffAfterError(t *testing.T) {
	source := &fakeResolver{steps: []resolutionStep{
		errorStep("banco indisponível"),
		resolvedStep("tx-1"),
		idle,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	source.onCall = func(call int) {
		if call == 3 {
			cancel()
		}
	}

	worker := NewReferenceWorker(source, testReferenceConfig(), discardTestLogger())
	runReferenceWorker(t, worker, ctx)

	stats := worker.Stats()
	if stats.Errors != 1 {
		t.Fatalf("Errors = %d, quer 1", stats.Errors)
	}
	// O laço SOBREVIVEU ao erro e tratou o resultado seguinte.
	if stats.Resolved != 1 {
		t.Fatalf("Resolved = %d, quer 1 (o laço não pode morrer por erro de infra)", stats.Resolved)
	}
}

// TestReferenceWorkerIgnoresOutcomeWhenThereIsAnError documenta a precedência.
//
// O caso de uso nunca devolve resultado e erro na mesma chamada — este teste
// prende o contrato para que um dublê que devolva os dois não passe despercebido,
// que foi exatamente o defeito da primeira versão do fakeResolver.
func TestReferenceWorkerIgnoresOutcomeWhenThereIsAnError(t *testing.T) {
	both := resolvedStep("tx-ignorado")
	both.err = errors.New("falha junto com um resultado impossível")

	source := &fakeResolver{steps: []resolutionStep{both, idle}}
	ctx, cancel := context.WithCancel(context.Background())
	source.onCall = func(call int) {
		if call == 2 {
			cancel()
		}
	}

	worker := NewReferenceWorker(source, testReferenceConfig(), discardTestLogger())
	runReferenceWorker(t, worker, ctx)

	stats := worker.Stats()
	if stats.Errors != 1 {
		t.Fatalf("Errors = %d, quer 1", stats.Errors)
	}
	if stats.Resolved != 0 {
		t.Fatalf("Resolved = %d, quer 0 (o erro tem precedência sobre o resultado)", stats.Resolved)
	}
}

func TestReferenceWorkerStopsOnCancelDuringErrorBackoff(t *testing.T) {
	source := &fakeResolver{steps: []resolutionStep{errorStep("banco indisponível")}}

	ctx, cancel := context.WithCancel(context.Background())
	config := testReferenceConfig()
	config.ErrorBackoff = 5 * time.Second // a espera longa é o ponto do teste

	worker := NewReferenceWorker(source, config, discardTestLogger())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	// Espera o erro acontecer e cancela DURANTE a espera do backoff.
	deadline := time.Now().Add(3 * time.Second)
	for source.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run devolveu erro: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("o worker não encerrou: a espera do backoff deveria ser interrompível")
	}
}

func TestReferenceWorkerUnknownOutcomeIsLoggedNotCounted(t *testing.T) {
	source := &fakeResolver{steps: []resolutionStep{
		stepWithOutcome("tx-x", usecase.ResolutionOutcome("DESCONHECIDO")),
		idle,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	source.onCall = func(call int) {
		if call == 2 {
			cancel()
		}
	}

	worker := NewReferenceWorker(source, testReferenceConfig(), discardTestLogger())
	runReferenceWorker(t, worker, ctx)

	stats := worker.Stats()
	if stats.Resolved+stats.Rescheduled+stats.Rejected+stats.Exhausted != 0 {
		t.Fatalf("desfecho desconhecido não deveria alimentar contador: %+v", stats)
	}
	if stats.Errors != 0 {
		t.Fatalf("Errors = %d, quer 0 (desfecho desconhecido não é erro de infra)", stats.Errors)
	}
}

// ── ciclo de vida ────────────────────────────────────────────────────

func TestReferenceRunnerWithoutWorkerIsNoop(t *testing.T) {
	runner := NewReferenceRunner(nil, discardTestLogger())
	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := runner.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestReferenceRunnerStartsAndStopsCleanly(t *testing.T) {
	source := &fakeResolver{steps: []resolutionStep{idle}}
	worker := NewReferenceWorker(source, testReferenceConfig(), discardTestLogger())
	runner := NewReferenceRunner(worker, discardTestLogger())

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
