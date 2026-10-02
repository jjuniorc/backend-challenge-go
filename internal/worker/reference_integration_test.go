//go:build integration

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// Estes testes exercitam o worker de referências contra PostgreSQL REAL, com o
// caso de uso real. É o cenário completo que o README §12 item 8 descreve:
// uma operação que espera por referência é ASSUMIDA e concluída, sem perda e sem
// duplicação, inclusive com mais de uma instância em execução.

// referenceSetup cria carteira, casos de uso e uma política com backoff curto.
//
// O backoff curto é o único ajuste: sem ele, a retomada inicial só ficaria
// vencida após 5s (default), e o teste esperaria dezenas de segundos por nada. O
// caminho de código é o mesmo.
func referenceSetup(t *testing.T) (ports.Store, *usecase.ProcessTransaction, string) {
	t.Helper()

	store := testsupport.NewStore(t)
	ids := usecase.UUIDv7{}
	clock := usecase.SystemClock{}

	policy := usecase.ReferencePolicy{
		MaxAttempts: 10,
		TTL:         time.Hour,
		BaseBackoff: 20 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
	}
	openWallet := usecase.NewOpenWallet(store, ids, clock)
	process := usecase.NewProcessTransaction(store, ids, clock, policy)

	balance, err := money.ParseDecimal("1000.00", "BRL")
	if err != nil {
		t.Fatalf("setup money: %v", err)
	}
	opened, err := openWallet.Execute(context.Background(), usecase.OpenWalletCommand{
		PlayerID:       "player-1",
		InitialBalance: balance,
		CorrelationID:  "corr-open",
	})
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	return store, process, opened.WalletID
}

func execute(t *testing.T, process *usecase.ProcessTransaction, cmd usecase.ProcessTransactionCommand) usecase.ProcessTransactionResult {
	t.Helper()
	result, err := process.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("ProcessTransaction(%s/%s): %v", cmd.Kind, cmd.ExternalTransactionID, err)
	}
	return result
}

func refundCommand(t *testing.T, walletID, externalID, referenceExternalID, amount string) usecase.ProcessTransactionCommand {
	t.Helper()
	parsed, err := money.ParseDecimal(amount, "BRL")
	if err != nil {
		t.Fatalf("money(%q): %v", amount, err)
	}
	return usecase.ProcessTransactionCommand{
		ProviderID:            "provider-a",
		ExternalTransactionID: externalID,
		IdempotencyKey:        "provider-a:" + externalID,
		PlayerID:              "player-1",
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "REFUND",
		Money:                 parsed,
		ReferenceExternalID:   referenceExternalID,
	}
}

func betCommand(t *testing.T, walletID, externalID, amount string) usecase.ProcessTransactionCommand {
	t.Helper()
	parsed, err := money.ParseDecimal(amount, "BRL")
	if err != nil {
		t.Fatalf("money(%q): %v", amount, err)
	}
	return usecase.ProcessTransactionCommand{
		ProviderID:            "provider-a",
		ExternalTransactionID: externalID,
		IdempotencyKey:        "provider-a:" + externalID,
		PlayerID:              "player-1",
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 parsed,
	}
}

func transactionStatus(t *testing.T, transactionID string) string {
	t.Helper()
	return testsupport.QueryString(t, `SELECT status FROM wager_transactions WHERE id = $1::uuid`, transactionID)
}

func countByQuery(t *testing.T, sqlText string, args ...any) int64 {
	t.Helper()
	return testsupport.QueryInt(t, sqlText, args...)
}

// startReferenceWorker sobe o worker pelo RUNNER, que é o que a produção usa.
func startReferenceWorker(t *testing.T, process *usecase.ProcessTransaction) *ReferenceRunner {
	t.Helper()

	worker := NewReferenceWorker(process, ReferenceConfig{
		Interval:     20 * time.Millisecond,
		ErrorBackoff: 20 * time.Millisecond,
	}, discardTestLogger())
	runner := NewReferenceRunner(worker, discardTestLogger())

	if err := runner.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := runner.Stop(ctx); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return runner
}

// ── cenário completo, uma instância ──────────────────────────────────

func TestReferenceWorkerResolvesPendingReferenceEndToEnd(t *testing.T) {
	store, process, walletID := referenceSetup(t)

	// O REFUND chega antes da BET referenciada: fica pendente.
	refund := execute(t, process, refundCommand(t, walletID, "ext-refund", "ext-bet", "10.00"))
	if refund.Status != wagertransaction.StatusPendingReference {
		t.Fatalf("status inicial = %s, quer PENDING_REFERENCE", refund.Status)
	}

	// A BET chega e conclui.
	bet := execute(t, process, betCommand(t, walletID, "ext-bet", "10.00"))
	if bet.Status != wagertransaction.StatusProcessed {
		t.Fatalf("status da BET = %s, quer PROCESSED", bet.Status)
	}
	if got := balanceOf(t, store, walletID); got != "990.00" {
		t.Fatalf("saldo após a BET = %s, quer 990.00", got)
	}

	startReferenceWorker(t, process)

	// O worker resolve sozinho, sem intervenção.
	waitForCondition(t, 30*time.Second, "o REFUND pendente ser resolvido", func() bool {
		return transactionStatus(t, refund.TransactionID) == "PROCESSED"
	})

	// Saldo de volta ao inicial: 1000 - 10 + 10.
	if got := balanceOf(t, store, walletID); got != "1000.00" {
		t.Fatalf("saldo após a retomada = %s, quer 1000.00", got)
	}
	// Ledger: abertura + BET + REFUND.
	if n := ledgerRows(t, walletID); n != 3 {
		t.Fatalf("lançamentos = %d, quer 3", n)
	}
	// E a reversão registra a referência interna (auditoria).
	orphan := countByQuery(t,
		`SELECT count(*) FROM wager_transactions
		  WHERE wallet_id = $1::uuid AND status = 'PROCESSED' AND kind = 'REFUND'
		    AND reference_transaction_id IS NULL`, walletID)
	if orphan != 0 {
		t.Fatalf("reversões aplicadas sem referência interna = %d, quer 0", orphan)
	}
}

// ── duas instâncias, cada pendência uma única vez ────────────────────

func TestTwoReferenceWorkersResolveEachPendingOnceWithSkipLocked(t *testing.T) {
	store, process, walletID := referenceSetup(t)
	const total = 4

	// Cada reversão referencia a SUA aposta, e todas chegam antes delas.
	for i := 1; i <= total; i++ {
		refund := execute(t, process, refundCommand(t, walletID,
			fmt.Sprintf("ext-refund-%d", i), fmt.Sprintf("ext-bet-%d", i), "10.00"))
		if refund.Status != wagertransaction.StatusPendingReference {
			t.Fatalf("reversão %d = %s, quer PENDING_REFERENCE", i, refund.Status)
		}
	}

	// As apostas chegam.
	for i := 1; i <= total; i++ {
		execute(t, process, betCommand(t, walletID, fmt.Sprintf("ext-bet-%d", i), "10.00"))
	}

	if got := balanceOf(t, store, walletID); got != "960.00" {
		t.Fatalf("saldo após as apostas = %s, quer 960.00", got)
	}

	// DUAS instâncias do worker, ao mesmo tempo, sobre a mesma tabela.
	startReferenceWorker(t, process)
	startReferenceWorker(t, process)

	// Todas as pendências são resolvidas...
	waitForCondition(t, 40*time.Second, "as 4 referências serem resolvidas", func() bool {
		processed := countByQuery(t,
			`SELECT count(*) FROM wager_transactions
			  WHERE wallet_id = $1::uuid AND kind = 'REFUND' AND status = 'PROCESSED'`, walletID)
		return processed == total
	})
	// ...e nada fica pendente.
	waitForCondition(t, 20*time.Second, "não sobrar pendência", func() bool {
		pending := countByQuery(t,
			`SELECT count(*) FROM wager_transactions
			  WHERE wallet_id = $1::uuid AND status = 'PENDING_REFERENCE'`, walletID)
		return pending == 0
	})

	// O que prova "uma única vez": o efeito financeiro.
	//
	// Saldo: 1000 - 40 (apostas) + 40 (reversões) = 1000. Uma duplicação
	// elevaria acima disso; uma perda, abaixo.
	if got := balanceOf(t, store, walletID); got != "1000.00" {
		t.Fatalf("saldo = %s, quer 1000.00 (exclusão entre instâncias)", got)
	}
	// Ledger: abertura (crédito) + 4 apostas (débito) + 4 reversões (crédito).
	if n := ledgerRows(t, walletID); n != 1+2*total {
		t.Fatalf("lançamentos = %d, quer %d", n, 1+2*total)
	}
	credits := countByQuery(t,
		`SELECT count(*) FROM wallet_ledger_entries
		  WHERE wallet_id = $1::uuid AND direction = 'CREDIT'`, walletID)
	if credits != total+1 {
		t.Fatalf("créditos = %d, quer %d (abertura + %d reversões)", credits, total+1, total)
	}
	// Transações: OPENING + 4 BET + 4 REFUND, sem linha extra.
	if n := transactionRows(t, walletID); n != 1+2*total {
		t.Fatalf("transações = %d, quer %d", n, 1+2*total)
	}
	// E nenhuma reversão aplicada sem referência interna.
	orphan := countByQuery(t,
		`SELECT count(*) FROM wager_transactions
		  WHERE wallet_id = $1::uuid AND status = 'PROCESSED' AND kind = 'REFUND'
		    AND reference_transaction_id IS NULL`, walletID)
	if orphan != 0 {
		t.Fatalf("reversões sem referência interna = %d, quer 0", orphan)
	}
}
