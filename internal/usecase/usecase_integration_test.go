//go:build integration

package usecase_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("setup money(%q): %v", s, err)
	}
	return m
}

func setup(t *testing.T) (ports.Store, *usecase.OpenWallet, *usecase.ProcessTransaction) {
	t.Helper()
	store := testsupport.NewStore(t)
	ids := usecase.UUIDv7{}
	clock := usecase.SystemClock{}
	return store, usecase.NewOpenWallet(store, ids, clock),
		usecase.NewProcessTransaction(store, ids, clock, usecase.DefaultReferencePolicy())
}

func mustOpenWallet(t *testing.T, uc *usecase.OpenWallet, player, balance string) usecase.OpenWalletResult {
	t.Helper()
	res, err := uc.Execute(context.Background(), usecase.OpenWalletCommand{
		PlayerID:       player,
		InitialBalance: brl(t, balance),
		CorrelationID:  "corr-open",
	})
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}
	return res
}

func command(t *testing.T, walletID, externalID, kind, amount string) usecase.ProcessTransactionCommand {
	t.Helper()
	return usecase.ProcessTransactionCommand{
		ProviderID:            "provider-a",
		ExternalTransactionID: externalID,
		IdempotencyKey:        "provider-a:" + externalID,
		PlayerID:              "player-1",
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Money:                 brl(t, amount),
		CorrelationID:         "corr-" + externalID,
	}
}

// mustProcess roda o caso de uso exigindo sucesso de transporte.
func mustProcess(t *testing.T, uc *usecase.ProcessTransaction, cmd usecase.ProcessTransactionCommand) usecase.ProcessTransactionResult {
	t.Helper()
	res, err := uc.Execute(context.Background(), cmd)
	if err != nil {
		t.Fatalf("ProcessTransaction(%s/%s): %v", cmd.Kind, cmd.ExternalTransactionID, err)
	}
	return res
}

func ledgerCount(t *testing.T, walletID, direction string) int64 {
	t.Helper()
	return testsupport.QueryInt(t,
		`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1::uuid AND direction = $2`,
		walletID, direction)
}

func outboxCount(t *testing.T, eventType string) int64 {
	t.Helper()
	return testsupport.QueryInt(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, eventType)
}

func storedBalance(t *testing.T, store ports.Store, walletID string) string {
	t.Helper()
	w, err := store.Wallets().Get(context.Background(), walletID)
	if err != nil {
		t.Fatalf("Get wallet: %v", err)
	}
	return w.Balance().String()
}

// ── abertura de carteira ─────────────────────────────────────────────

func TestOpenWalletWithPositiveBalance(t *testing.T) {
	store, openWallet, _ := setup(t)

	res := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	if res.Version != 1 {
		t.Fatalf("versão = %d, quer 1 (abertura não incrementa)", res.Version)
	}
	if res.Balance.String() != "1000.00" {
		t.Fatalf("saldo = %s", res.Balance.String())
	}
	if n := ledgerCount(t, res.WalletID, "CREDIT"); n != 1 {
		t.Fatalf("créditos no ledger = %d, quer 1", n)
	}
	if n := outboxCount(t, "WagerTransactionProcessed"); n != 1 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 1", n)
	}
	if n := outboxCount(t, "WalletBalanceChanged"); n != 1 {
		t.Fatalf("WalletBalanceChanged = %d, quer 1", n)
	}
	if got := storedBalance(t, store, res.WalletID); got != "1000.00" {
		t.Fatalf("saldo armazenado = %s", got)
	}
}

func TestOpenWalletWithZeroBalanceCreatesNothing(t *testing.T) {
	store, openWallet, _ := setup(t)

	res := mustOpenWallet(t, openWallet, "player-1", "0.00")

	if res.Balance.String() != "0.00" || res.Version != 1 {
		t.Fatalf("carteira zerada divergente: %s v%d", res.Balance.String(), res.Version)
	}
	if n := testsupport.QueryInt(t, `SELECT count(*) FROM wager_transactions`); n != 0 {
		t.Fatalf("transações = %d, quer 0 (sem OPENING)", n)
	}
	if n := testsupport.QueryInt(t, `SELECT count(*) FROM wallet_ledger_entries`); n != 0 {
		t.Fatalf("lançamentos = %d, quer 0", n)
	}
	if n := testsupport.QueryInt(t, `SELECT count(*) FROM outbox_events`); n != 0 {
		t.Fatalf("eventos = %d, quer 0", n)
	}
	if got := storedBalance(t, store, res.WalletID); got != "0.00" {
		t.Fatalf("saldo armazenado = %s", got)
	}
}

func TestOpenWalletDuplicateRejected(t *testing.T) {
	_, openWallet, _ := setup(t)

	mustOpenWallet(t, openWallet, "player-1", "100.00")

	_, err := openWallet.Execute(context.Background(), usecase.OpenWalletCommand{
		PlayerID: "player-1", InitialBalance: brl(t, "50.00"),
	})
	if !errors.Is(err, ports.ErrWalletAlreadyExists) {
		t.Fatalf("erro = %v, quer ErrWalletAlreadyExists", err)
	}
}

// ── operações básicas ────────────────────────────────────────────────

func TestBetDebitsWalletAndPublishesEvents(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	res := mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "25.00"))

	if res.Status != wagertransaction.StatusProcessed {
		t.Fatalf("status = %s, quer PROCESSED", res.Status)
	}
	if res.Balance.String() != "975.00" {
		t.Fatalf("saldo retornado = %s, quer 975.00", res.Balance.String())
	}
	if res.IdempotentReplay {
		t.Fatal("primeira execução não deveria ser replay")
	}
	if n := ledgerCount(t, w.WalletID, "DEBIT"); n != 1 {
		t.Fatalf("débitos = %d, quer 1", n)
	}
	if got := storedBalance(t, store, w.WalletID); got != "975.00" {
		t.Fatalf("saldo armazenado = %s, quer 975.00", got)
	}
	// 1 da abertura + 1 desta operação
	if n := outboxCount(t, "WagerTransactionProcessed"); n != 2 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 2", n)
	}
	if n := outboxCount(t, "WalletBalanceChanged"); n != 2 {
		t.Fatalf("WalletBalanceChanged = %d, quer 2", n)
	}
}

func TestBetInsufficientFundsIsRejected(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "10.00")

	res := mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "10.01"))

	if res.Status != wagertransaction.StatusRejected {
		t.Fatalf("status = %s, quer REJECTED", res.Status)
	}
	if res.FailureCode != usecase.CodeWalletInsufficientFunds {
		t.Fatalf("failureCode = %q, quer %q", res.FailureCode, usecase.CodeWalletInsufficientFunds)
	}
	if n := ledgerCount(t, w.WalletID, "DEBIT"); n != 0 {
		t.Fatalf("débitos = %d, quer 0 (rejeição não gera lançamento)", n)
	}
	if got := storedBalance(t, store, w.WalletID); got != "10.00" {
		t.Fatalf("saldo = %s, quer 10.00 inalterado", got)
	}
	if n := outboxCount(t, "WagerTransactionRejected"); n != 1 {
		t.Fatalf("WagerTransactionRejected = %d, quer 1", n)
	}
}

func TestLossHasNoLedgerAndNoVersionChange(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	res := mustProcess(t, process, command(t, w.WalletID, "ext-loss-1", "LOSS", "0.00"))

	if res.Status != wagertransaction.StatusProcessed {
		t.Fatalf("status = %s, quer PROCESSED", res.Status)
	}
	if res.Balance.String() != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00", res.Balance.String())
	}
	if n := testsupport.QueryInt(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1::uuid`, w.WalletID); n != 1 {
		t.Fatalf("lançamentos = %d, quer 1 (só a abertura)", n)
	}
	wallet, err := store.Wallets().Get(context.Background(), w.WalletID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if wallet.Version() != 1 {
		t.Fatalf("versão = %d, quer 1 (LOSS não altera saldo)", wallet.Version())
	}
	if n := outboxCount(t, "WagerTransactionProcessed"); n != 2 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 2 (abertura + LOSS)", n)
	}
	if n := outboxCount(t, "WalletBalanceChanged"); n != 1 {
		t.Fatalf("WalletBalanceChanged = %d, quer 1 (só a abertura)", n)
	}
}

func TestWinCreditsWallet(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	res := mustProcess(t, process, command(t, w.WalletID, "ext-win-1", "WIN", "50.00"))

	if res.Balance.String() != "150.00" {
		t.Fatalf("saldo = %s, quer 150.00", res.Balance.String())
	}
	if n := ledgerCount(t, w.WalletID, "CREDIT"); n != 2 {
		t.Fatalf("créditos = %d, quer 2 (abertura + WIN)", n)
	}
	if got := storedBalance(t, store, w.WalletID); got != "150.00" {
		t.Fatalf("saldo armazenado = %s", got)
	}
}

// ── concorrência (o coração da avaliação) ────────────────────────────

func TestFiftyParallelIdenticalBetsProduceSingleDebit(t *testing.T) {
	store, openWallet, process := setup(t)
	ctx := context.Background()
	w := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	cmd := command(t, w.WalletID, "ext-parallel-1", "BET", "25.00")

	const workers = 50
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []usecase.ProcessTransactionResult
		errs    []error
	)
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := process.Execute(ctx, cmd)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			results = append(results, res)
		}()
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("erros inesperados: %v", errs)
	}
	if len(results) != workers {
		t.Fatalf("resultados = %d, quer %d", len(results), workers)
	}

	processed, replays := 0, 0
	ids := map[string]struct{}{}
	for _, r := range results {
		if r.Status != wagertransaction.StatusProcessed {
			t.Fatalf("status = %s, quer PROCESSED", r.Status)
		}
		if r.IdempotentReplay {
			replays++
		} else {
			processed++
		}
		ids[r.TransactionID] = struct{}{}
	}
	if processed != 1 {
		t.Fatalf("execuções efetivas = %d, quer 1", processed)
	}
	if replays != workers-1 {
		t.Fatalf("replays = %d, quer %d", replays, workers-1)
	}
	if len(ids) != 1 {
		t.Fatalf("transactionIds distintos = %d, quer 1", len(ids))
	}

	// UM único débito no ledger.
	if n := ledgerCount(t, w.WalletID, "DEBIT"); n != 1 {
		t.Fatalf("débitos no ledger = %d, quer 1", n)
	}
	// Uma única WagerTransactionProcessed desta operação (2 com a abertura).
	if n := outboxCount(t, "WagerTransactionProcessed"); n != 2 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 2", n)
	}
	if got := storedBalance(t, store, w.WalletID); got != "975.00" {
		t.Fatalf("saldo = %s, quer 975.00", got)
	}
}

func TestTwoEightyBetsOnHundredBalance(t *testing.T) {
	store, openWallet, process := setup(t)
	ctx := context.Background()
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	cmds := []usecase.ProcessTransactionCommand{
		command(t, w.WalletID, "ext-80-a", "BET", "80.00"),
		command(t, w.WalletID, "ext-80-b", "BET", "80.00"),
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []usecase.ProcessTransactionResult
		errs    []error
	)
	start := make(chan struct{})

	for _, cmd := range cmds {
		wg.Add(1)
		go func(c usecase.ProcessTransactionCommand) {
			defer wg.Done()
			<-start
			res, err := process.Execute(ctx, c)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			results = append(results, res)
		}(cmd)
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("erros inesperados: %v", errs)
	}

	processed, rejected := 0, 0
	for _, r := range results {
		switch r.Status {
		case wagertransaction.StatusProcessed:
			processed++
		case wagertransaction.StatusRejected:
			rejected++
			if r.FailureCode != usecase.CodeWalletInsufficientFunds {
				t.Fatalf("failureCode = %q", r.FailureCode)
			}
		default:
			t.Fatalf("status inesperado: %s", r.Status)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d, quer 1 e 1", processed, rejected)
	}

	if n := ledgerCount(t, w.WalletID, "DEBIT"); n != 1 {
		t.Fatalf("débitos no ledger = %d, quer 1", n)
	}
	if got := storedBalance(t, store, w.WalletID); got != "20.00" {
		t.Fatalf("saldo final = %s, quer 20.00", got)
	}
}

func TestDistinctWalletsProcessInParallel(t *testing.T) {
	store, openWallet, process := setup(t)
	ctx := context.Background()

	const (
		wallets         = 3
		betsPerWallet   = 10
		betAmount       = "10.00"
		expectedBalance = "900.00" // 1000.00 - 10 * 10.00
	)
	// (playerId, moeda) é único: cada carteira usa um jogador distinto.
	ids := make([]string, wallets)
	for i := 0; i < wallets; i++ {
		res, err := openWallet.Execute(ctx, usecase.OpenWalletCommand{
			PlayerID:       playerIDFor(i),
			InitialBalance: brl(t, "1000.00"),
		})
		if err != nil {
			t.Fatalf("OpenWallet(%d): %v", i, err)
		}
		ids[i] = res.WalletID
	}

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	start := make(chan struct{})
	// Cada carteira pertence a um jogador distinto, entao o playerId da operacao
	// precisa acompanhar a carteira: o helper command usa um jogador padrao que
	// so corresponde a carteira de player-1. O comando e montado FORA da
	// goroutine para nao chamar t.Fatalf de goroutine nem ler estado compartilhado.
	for i, walletID := range ids {
		for j := 0; j < betsPerWallet; j++ {
			cmd := command(t, walletID, fmt.Sprintf("ext-%d-%d", i, j), "BET", betAmount)
			cmd.PlayerID = playerIDFor(i)
			wg.Add(1)
			go func(c usecase.ProcessTransactionCommand) {
				defer wg.Done()
				<-start
				if _, err := process.Execute(ctx, c); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(cmd)
		}
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("erros inesperados: %v", errs)
	}

	for i, walletID := range ids {
		if n := outboxCount(t, "WagerTransactionRejected"); n != 0 {
			t.Fatalf("carteira %d: houve %d rejeicao(oes) inesperada(s)", i, n)
		}
		if got := storedBalance(t, store, walletID); got != expectedBalance {
			t.Fatalf("carteira %d: saldo = %s, quer %s", i, got, expectedBalance)
		}
		if n := ledgerCount(t, walletID, "DEBIT"); n != betsPerWallet {
			t.Fatalf("carteira %d: débitos = %d, quer %d", i, n, betsPerWallet)
		}
	}
}

func playerIDFor(i int) string {
	return fmt.Sprintf("player-%d", i)
}

// ── idempotência e replay ────────────────────────────────────────────

func TestReplayReturnsOriginalBalanceAfterOtherMovements(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	bet := command(t, w.WalletID, "ext-bet-1", "BET", "25.00")
	first := mustProcess(t, process, bet)
	if first.Balance.String() != "975.00" {
		t.Fatalf("saldo do processamento = %s", first.Balance.String())
	}

	// Outra movimentação altera o saldo atual da carteira.
	mustProcess(t, process, command(t, w.WalletID, "ext-win-1", "WIN", "100.00"))

	replay := mustProcess(t, process, bet)
	if !replay.IdempotentReplay {
		t.Fatal("replay deveria estar marcado como idempotente")
	}
	if replay.TransactionID != first.TransactionID {
		t.Fatalf("transactionId do replay = %s, quer %s", replay.TransactionID, first.TransactionID)
	}
	// O saldo devolvido é o do processamento ORIGINAL, não o atual (1075.00).
	if replay.Balance.String() != "975.00" {
		t.Fatalf("saldo do replay = %s, quer 975.00 (saldo do processamento original)", replay.Balance.String())
	}
}

func TestReplayWithDifferentContentConflicts(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "25.00"))

	// Mesma chave, valor diferente.
	conflicting := command(t, w.WalletID, "ext-bet-1", "BET", "30.00")
	_, err := process.Execute(context.Background(), conflicting)
	if domainerr.CodeOf(err) != usecase.CodeIdempotencyKeyReused {
		t.Fatalf("erro = %v (code %q), quer %q", err, domainerr.CodeOf(err), usecase.CodeIdempotencyKeyReused)
	}
}

func TestSameOperationWithDifferentKeyConflicts(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "25.00"))

	other := command(t, w.WalletID, "ext-bet-1", "BET", "25.00")
	other.IdempotencyKey = "provider-a:outra-chave"
	_, err := process.Execute(context.Background(), other)
	if domainerr.CodeOf(err) != usecase.CodeIdempotencyKeyMismatch {
		t.Fatalf("erro = %v (code %q), quer %q", err, domainerr.CodeOf(err), usecase.CodeIdempotencyKeyMismatch)
	}
}

// ── referências e reversões ──────────────────────────────────────────

func TestRefundBeforeReferenceGoesPending(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	refund := command(t, w.WalletID, "ext-refund-1", "REFUND", "25.00")
	refund.ReferenceExternalID = "ext-bet-inexistente"

	res := mustProcess(t, process, refund)
	if res.Status != wagertransaction.StatusPendingReference {
		t.Fatalf("status = %s, quer PENDING_REFERENCE", res.Status)
	}
	if res.Balance.String() != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00 inalterado", res.Balance.String())
	}
	if n := ledgerCount(t, w.WalletID, "DEBIT"); n != 0 {
		t.Fatalf("débitos = %d, quer 0", n)
	}
	if n := outboxCount(t, "WagerTransactionPendingReference"); n != 1 {
		t.Fatalf("WagerTransactionPendingReference = %d, quer 1", n)
	}
	// Agendamento durável para o worker de referências (F8).
	attempts := testsupport.QueryInt(t,
		`SELECT COALESCE(reference_attempts, 0) FROM wager_transactions WHERE status = 'PENDING_REFERENCE'`)
	if attempts != 1 {
		t.Fatalf("reference_attempts = %d, quer 1", attempts)
	}
	scheduled := testsupport.QueryInt(t,
		`SELECT count(*) FROM wager_transactions WHERE status = 'PENDING_REFERENCE' AND next_attempt_at IS NOT NULL`)
	if scheduled != 1 {
		t.Fatalf("next_attempt_at agendado em %d linhas, quer 1", scheduled)
	}
	if got := storedBalance(t, store, w.WalletID); got != "100.00" {
		t.Fatalf("saldo armazenado = %s", got)
	}
}

func TestRefundAfterReferenceCreditsWallet(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	bet := command(t, w.WalletID, "ext-bet-1", "BET", "25.00")
	mustProcess(t, process, bet)

	refund := command(t, w.WalletID, "ext-refund-1", "REFUND", "25.00")
	refund.ReferenceExternalID = "ext-bet-1"
	res := mustProcess(t, process, refund)

	if res.Status != wagertransaction.StatusProcessed {
		t.Fatalf("status = %s, quer PROCESSED", res.Status)
	}
	if res.Balance.String() != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00 (devolvido)", res.Balance.String())
	}
	if n := ledgerCount(t, w.WalletID, "CREDIT"); n != 2 {
		t.Fatalf("créditos = %d, quer 2 (abertura + refund)", n)
	}
	if got := storedBalance(t, store, w.WalletID); got != "100.00" {
		t.Fatalf("saldo armazenado = %s", got)
	}
	// A referência interna resolvida ficou persistida.
	resolved := testsupport.QueryInt(t,
		`SELECT count(*) FROM wager_transactions WHERE kind = 'REFUND' AND reference_transaction_id IS NOT NULL`)
	if resolved != 1 {
		t.Fatalf("REFUND sem reference_transaction_id resolvido: %d", resolved)
	}
}

func TestSecondReversalOfSameReferenceIsRejected(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "25.00"))

	refund := command(t, w.WalletID, "ext-refund-1", "REFUND", "25.00")
	refund.ReferenceExternalID = "ext-bet-1"
	if res := mustProcess(t, process, refund); res.Status != wagertransaction.StatusProcessed {
		t.Fatalf("REFUND deveria ser aceito, status = %s", res.Status)
	}

	rollback := command(t, w.WalletID, "ext-rollback-1", "ROLLBACK", "25.00")
	rollback.ReferenceExternalID = "ext-bet-1"
	res := mustProcess(t, process, rollback)

	if res.Status != wagertransaction.StatusRejected {
		t.Fatalf("status = %s, quer REJECTED", res.Status)
	}
	if res.FailureCode != usecase.CodeReferenceAlreadyReversed {
		t.Fatalf("failureCode = %q, quer %q", res.FailureCode, usecase.CodeReferenceAlreadyReversed)
	}
}

func TestRollbackOfBetCreditsBack(t *testing.T) {
	store, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "25.00"))

	rollback := command(t, w.WalletID, "ext-rollback-1", "ROLLBACK", "25.00")
	rollback.ReferenceExternalID = "ext-bet-1"
	res := mustProcess(t, process, rollback)

	if res.Balance.String() != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00", res.Balance.String())
	}
	if got := storedBalance(t, store, w.WalletID); got != "100.00" {
		t.Fatalf("saldo armazenado = %s", got)
	}
}

func TestReversalInsufficientFundsHasDistinctCode(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	// WIN 100.00 -> 200.00 ; BET 150.00 -> 50.00
	win := command(t, w.WalletID, "ext-win-1", "WIN", "100.00")
	mustProcess(t, process, win)
	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "150.00"))

	// ROLLBACK do WIN debita 100.00 com apenas 50.00 disponíveis.
	rollback := command(t, w.WalletID, "ext-rollback-1", "ROLLBACK", "100.00")
	rollback.ReferenceExternalID = "ext-win-1"
	res := mustProcess(t, process, rollback)

	if res.Status != wagertransaction.StatusRejected {
		t.Fatalf("status = %s, quer REJECTED", res.Status)
	}
	if res.FailureCode != usecase.CodeReversalInsufficientFunds {
		t.Fatalf("failureCode = %q, quer %q (distinto de %q)",
			res.FailureCode, usecase.CodeReversalInsufficientFunds, usecase.CodeWalletInsufficientFunds)
	}
}

func TestReferenceRoundMismatchIsRejected(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "25.00"))

	refund := command(t, w.WalletID, "ext-refund-1", "REFUND", "25.00")
	refund.ReferenceExternalID = "ext-bet-1"
	refund.RoundID = "outra-rodada"
	res := mustProcess(t, process, refund)

	if res.Status != wagertransaction.StatusRejected {
		t.Fatalf("status = %s, quer REJECTED", res.Status)
	}
	if res.FailureCode != usecase.CodeReferenceRoundMismatch {
		t.Fatalf("failureCode = %q, quer %q", res.FailureCode, usecase.CodeReferenceRoundMismatch)
	}
}

func TestWalletPlayerMismatchIsRejected(t *testing.T) {
	_, openWallet, process := setup(t)
	w := mustOpenWallet(t, openWallet, "player-1", "100.00")

	cmd := command(t, w.WalletID, "ext-bet-1", "BET", "25.00")
	cmd.PlayerID = "outro-jogador"
	res := mustProcess(t, process, cmd)

	if res.Status != wagertransaction.StatusRejected {
		t.Fatalf("status = %s, quer REJECTED", res.Status)
	}
	if res.FailureCode != usecase.CodeWalletPlayerMismatch {
		t.Fatalf("failureCode = %q, quer %q", res.FailureCode, usecase.CodeWalletPlayerMismatch)
	}
}

// ── reconciliação ────────────────────────────────────────────────────

func TestReconciliationAfterMixedOperations(t *testing.T) {
	store, openWallet, process := setup(t)
	ctx := context.Background()
	w := mustOpenWallet(t, openWallet, "player-1", "1000.00")

	mustProcess(t, process, command(t, w.WalletID, "ext-bet-1", "BET", "100.00"))
	mustProcess(t, process, command(t, w.WalletID, "ext-win-1", "WIN", "50.00"))
	mustProcess(t, process, command(t, w.WalletID, "ext-loss-1", "LOSS", "0.00"))

	refund := command(t, w.WalletID, "ext-refund-1", "REFUND", "100.00")
	refund.ReferenceExternalID = "ext-bet-1"
	mustProcess(t, process, refund)

	wallet, err := store.Wallets().Get(ctx, w.WalletID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	sum, entries, err := store.Ledger().SumByWallet(ctx, w.WalletID)
	if err != nil {
		t.Fatalf("SumByWallet: %v", err)
	}
	if entries != 4 {
		t.Fatalf("lançamentos = %d, quer 4 (abertura + BET + WIN + REFUND; LOSS não gera)", entries)
	}
	if wallet.Balance().Amount() != sum {
		t.Fatalf("divergência: armazenado = %s, reconstruído = %d", wallet.Balance().String(), sum)
	}
	if wallet.Balance().String() != "1050.00" {
		t.Fatalf("saldo = %s, quer 1050.00", wallet.Balance().String())
	}
}
