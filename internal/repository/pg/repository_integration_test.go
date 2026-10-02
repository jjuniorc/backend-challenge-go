//go:build integration

package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wallet"
	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/repository/pg"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
)

var baseTime = time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("setup money(%q): %v", s, err)
	}
	return m
}

func insertWallet(t *testing.T, store *pg.DB, id, player, balance string) wallet.Wallet {
	t.Helper()
	w, err := wallet.New(wallet.NewParams{
		ID: id, PlayerID: player, InitialBalance: brl(t, balance), Now: baseTime,
	})
	if err != nil {
		t.Fatalf("wallet.New: %v", err)
	}
	if err := store.Wallets().Insert(context.Background(), w); err != nil {
		t.Fatalf("insert wallet: %v", err)
	}
	return w
}

func newExternal(t *testing.T, id, walletID, externalID, key string, kind wagertransaction.Kind, amount string) wagertransaction.Transaction {
	t.Helper()
	params := wagertransaction.ExternalParams{
		ID: id, ProviderID: "provider-a", ExternalTransactionID: externalID,
		IdempotencyKey: key, PayloadHash: "hash-" + id,
		WalletID: walletID, PlayerID: "player-1", RoundID: "round-1", GameID: "game-1",
		Kind: kind, Money: brl(t, amount), Now: baseTime,
	}
	// REFUND e ROLLBACK exigem referenceExternalTransactionId.
	if kind.RequiresReference() {
		params.ReferenceExternalID = "ext-1"
	}
	tx, err := wagertransaction.NewExternal(params)
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", kind, err)
	}
	return tx
}

func TestWalletRoundTrip(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()

	insertWallet(t, store, "00000000-0000-7000-8000-000000000001", "player-1", "1000.00")

	got, err := store.Wallets().Get(ctx, "00000000-0000-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.PlayerID() != "player-1" || got.Currency() != "BRL" {
		t.Fatalf("carteira divergente: %+v", got)
	}
	if got.Balance().String() != "1000.00" || got.Version() != 1 {
		t.Fatalf("saldo/versão divergentes: %s v%d", got.Balance().String(), got.Version())
	}

	byPlayer, err := store.Wallets().FindByPlayerAndCurrency(ctx, "player-1", "BRL")
	if err != nil {
		t.Fatalf("FindByPlayerAndCurrency: %v", err)
	}
	if byPlayer.ID() != got.ID() {
		t.Fatal("busca por (player, moeda) devolveu outra carteira")
	}
}

func TestWalletDuplicateRejected(t *testing.T) {
	store := testsupport.NewStore(t)
	insertWallet(t, store, "00000000-0000-7000-8000-000000000001", "player-1", "1000.00")

	dup, err := wallet.New(wallet.NewParams{
		ID: "00000000-0000-7000-8000-000000000002", PlayerID: "player-1",
		InitialBalance: brl(t, "10.00"), Now: baseTime,
	})
	if err != nil {
		t.Fatalf("wallet.New: %v", err)
	}
	err = store.Wallets().Insert(context.Background(), dup)
	if !errors.Is(err, ports.ErrWalletAlreadyExists) {
		t.Fatalf("erro = %v, quer ErrWalletAlreadyExists", err)
	}
	if name := ports.ConstraintNameOf(err); name != "wallets_player_currency_uniq" {
		t.Fatalf("constraint = %q, quer wallets_player_currency_uniq", name)
	}
}

func TestWalletLostUpdateIsPrevented(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")

	// Dois "escritores" leem a mesma versão.
	writerA, err := store.Wallets().Get(ctx, walletID)
	if err != nil {
		t.Fatalf("Get A: %v", err)
	}
	writerB, err := store.Wallets().Get(ctx, walletID)
	if err != nil {
		t.Fatalf("Get B: %v", err)
	}
	versionA, versionB := writerA.Version(), writerB.Version()

	if err := writerA.Debit(brl(t, "30.00"), baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("Debit A: %v", err)
	}
	if err := store.Wallets().UpdateBalance(ctx, writerA, versionA); err != nil {
		t.Fatalf("UpdateBalance A: %v", err)
	}

	if err := writerB.Debit(brl(t, "40.00"), baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("Debit B: %v", err)
	}
	err = store.Wallets().UpdateBalance(ctx, writerB, versionB)
	if !errors.Is(err, ports.ErrVersionConflict) {
		t.Fatalf("erro = %v, quer ErrVersionConflict (lost update deveria ser impedido)", err)
	}

	final, err := store.Wallets().Get(ctx, walletID)
	if err != nil {
		t.Fatalf("Get final: %v", err)
	}
	if final.Balance().String() != "70.00" {
		t.Fatalf("saldo = %s, quer 70.00 (só a escrita A deveria valer)", final.Balance().String())
	}
	if final.Version() != 2 {
		t.Fatalf("versão = %d, quer 2", final.Version())
	}
}

// ── ledger ───────────────────────────────────────────────────────────

func TestLedgerIsImmutable(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const (
		walletID = "00000000-0000-7000-8000-000000000001"
		txID     = "00000000-0000-7000-8000-0000000000b1"
		entryID  = "00000000-0000-7000-8000-0000000000e1"
	)

	insertWallet(t, store, walletID, "player-1", "100.00")
	tx := newExternal(t, txID, walletID, "ext-1", "key-1", wagertransaction.KindBet, "25.00")
	if err := store.Transactions().Insert(ctx, tx); err != nil {
		t.Fatalf("insert tx: %v", err)
	}

	entry, err := ledger.New(ledger.Params{
		ID: entryID, WalletID: walletID, TransactionID: txID,
		Direction: ledger.DirectionDebit, Amount: brl(t, "25.00"),
		BalanceBefore: brl(t, "100.00"), BalanceAfter: brl(t, "75.00"), CreatedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	if err := store.Ledger().Insert(ctx, entry); err != nil {
		t.Fatalf("insert ledger: %v", err)
	}

	if _, err := store.Wallets().Get(ctx, walletID); err != nil {
		t.Fatalf("sanidade: %v", err)
	}

	// UPDATE direto pelo banco de teste: precisa ser bloqueado pelo trigger.
	errUpdate := testsupport.ExecErr(t, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE id = $1::uuid`, entryID)
	if !errors.Is(errUpdate, ports.ErrImmutable) {
		t.Fatalf("UPDATE no ledger: erro = %v, quer ErrImmutable", errUpdate)
	}
	errDelete := testsupport.ExecErr(t, `DELETE FROM wallet_ledger_entries WHERE id = $1::uuid`, entryID)
	if !errors.Is(errDelete, ports.ErrImmutable) {
		t.Fatalf("DELETE no ledger: erro = %v, quer ErrImmutable", errDelete)
	}

	if count := testsupport.QueryInt(t, `SELECT count(*) FROM wallet_ledger_entries`); count != 1 {
		t.Fatalf("lançamentos = %d, quer 1 (nada deveria ter sido alterado)", count)
	}
}

func TestLedgerSumMatchesStoredBalance(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")

	// A abertura PRECISA estar no ledger: é ela que conecta o saldo armazenado
	// à soma dos lançamentos (README, seção de reconciliação).
	opening, err := wagertransaction.NewOpening(wagertransaction.OpeningParams{
		ID: "00000000-0000-7000-8000-0000000000a1", WalletID: walletID,
		PlayerID: "player-1", Money: brl(t, "100.00"), Now: baseTime,
	})
	if err != nil {
		t.Fatalf("NewOpening: %v", err)
	}
	if err := store.Transactions().Insert(ctx, opening); err != nil {
		t.Fatalf("insert opening: %v", err)
	}
	openingEntry, err := ledger.New(ledger.Params{
		ID: "00000000-0000-7000-8000-0000000000e0", WalletID: walletID,
		TransactionID: opening.ID(), Direction: ledger.DirectionCredit,
		Amount: brl(t, "100.00"), BalanceBefore: brl(t, "0.00"),
		BalanceAfter: brl(t, "100.00"), CreatedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("ledger.New abertura: %v", err)
	}
	if err := store.Ledger().Insert(ctx, openingEntry); err != nil {
		t.Fatalf("insert abertura: %v", err)
	}

	bet := newExternal(t, "00000000-0000-7000-8000-0000000000b1", walletID, "ext-1", "key-1", wagertransaction.KindBet, "25.00")
	if err := store.Transactions().Insert(ctx, bet); err != nil {
		t.Fatalf("insert bet: %v", err)
	}
	win := newExternal(t, "00000000-0000-7000-8000-0000000000b2", walletID, "ext-2", "key-2", wagertransaction.KindWin, "10.00")
	if err := store.Transactions().Insert(ctx, win); err != nil {
		t.Fatalf("insert win: %v", err)
	}

	debit, err := ledger.New(ledger.Params{
		ID: "00000000-0000-7000-8000-0000000000e1", WalletID: walletID, TransactionID: bet.ID(),
		Direction: ledger.DirectionDebit, Amount: brl(t, "25.00"),
		BalanceBefore: brl(t, "100.00"), BalanceAfter: brl(t, "75.00"), CreatedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("ledger.New debit: %v", err)
	}
	credit, err := ledger.New(ledger.Params{
		ID: "00000000-0000-7000-8000-0000000000e2", WalletID: walletID, TransactionID: win.ID(),
		Direction: ledger.DirectionCredit, Amount: brl(t, "10.00"),
		BalanceBefore: brl(t, "75.00"), BalanceAfter: brl(t, "85.00"), CreatedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("ledger.New credit: %v", err)
	}
	for _, e := range []ledger.Entry{debit, credit} {
		if err := store.Ledger().Insert(ctx, e); err != nil {
			t.Fatalf("insert ledger: %v", err)
		}
	}

	// O saldo armazenado acompanha os movimentos: 100.00 - 25.00 + 10.00 = 85.00.
	// Versão 3 = criação (1) + BET (2) + WIN (3): a OPENING não incrementa.
	updated, err := wallet.Rehydrate(wallet.RehydrateParams{
		ID: walletID, PlayerID: "player-1", Currency: "BRL", Balance: brl(t, "85.00"),
		Version: 3, CreatedAt: baseTime, UpdatedAt: baseTime,
	})
	if err != nil {
		t.Fatalf("wallet.Rehydrate: %v", err)
	}
	if err := store.Wallets().UpdateBalance(ctx, updated, 1); err != nil {
		t.Fatalf("UpdateBalance: %v", err)
	}

	sum, entries, err := store.Ledger().SumByWallet(ctx, walletID)
	if err != nil {
		t.Fatalf("SumByWallet: %v", err)
	}
	if entries != 3 {
		t.Fatalf("lançamentos = %d, quer 3 (abertura + aposta + prêmio)", entries)
	}
	if sum != 8500 {
		t.Fatalf("soma reconstruída = %d, quer 8500 (85.00)", sum)
	}

	// A reconciliação de verdade: reconstruído do ledger == armazenado.
	stored, err := store.Wallets().Get(ctx, walletID)
	if err != nil {
		t.Fatalf("Get wallet: %v", err)
	}
	if stored.Balance().Amount() != sum {
		t.Fatalf("saldo armazenado (%s) difere do reconstruído (%d)",
			stored.Balance().String(), sum)
	}
}

func TestTransactionRoundTrip(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")
	tx := newExternal(t, "00000000-0000-7000-8000-0000000000b1", walletID, "ext-1", "key-1", wagertransaction.KindBet, "25.00")
	if err := store.Transactions().Insert(ctx, tx); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	loaded, err := store.Transactions().GetByID(ctx, tx.ID())
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if loaded.Status() != wagertransaction.StatusPending {
		t.Fatalf("status = %s, quer PENDING", loaded.Status())
	}
	if loaded.PayloadHash() != "hash-"+tx.ID() {
		t.Fatalf("payloadHash = %q", loaded.PayloadHash())
	}
	if !loaded.Amount().Equal(brl(t, "25.00")) {
		t.Fatalf("amount = %s", loaded.Amount().String())
	}

	byKey, err := store.Transactions().FindByProviderAndIdempotencyKey(ctx, "provider-a", "key-1")
	if err != nil {
		t.Fatalf("FindByProviderAndIdempotencyKey: %v", err)
	}
	if byKey.ID() != tx.ID() {
		t.Fatal("busca por chave devolveu outra transação")
	}
	byExternal, err := store.Transactions().FindByProviderAndExternalID(ctx, "provider-a", "ext-1")
	if err != nil {
		t.Fatalf("FindByProviderAndExternalID: %v", err)
	}
	if byExternal.ID() != tx.ID() {
		t.Fatal("busca por external id devolveu outra transação")
	}

	// Transição para PROCESSED precisa persistir o saldo resultante.
	if err := loaded.MarkProcessed(brl(t, "75.00"), baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := store.Transactions().Update(ctx, loaded); err != nil {
		t.Fatalf("Update: %v", err)
	}
	reloaded, err := store.Transactions().GetByID(ctx, tx.ID())
	if err != nil {
		t.Fatalf("GetByID após update: %v", err)
	}
	if reloaded.Status() != wagertransaction.StatusProcessed {
		t.Fatalf("status = %s, quer PROCESSED", reloaded.Status())
	}
	if reloaded.ResultBalance().String() != "75.00" {
		t.Fatalf("resultBalance = %s, quer 75.00", reloaded.ResultBalance().String())
	}
}

func TestTransactionDuplicateIdempotencyKeyRejected(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")

	first := newExternal(t, "00000000-0000-7000-8000-0000000000b1", walletID, "ext-1", "key-1", wagertransaction.KindBet, "25.00")
	if err := store.Transactions().Insert(ctx, first); err != nil {
		t.Fatalf("insert first: %v", err)
	}

	// Mesma chave, outro externalTransactionId.
	dup := newExternal(t, "00000000-0000-7000-8000-0000000000b2", walletID, "ext-2", "key-1", wagertransaction.KindBet, "25.00")
	err := store.Transactions().Insert(ctx, dup)
	if !errors.Is(err, ports.ErrIdempotencyConflict) {
		t.Fatalf("erro = %v, quer ErrIdempotencyConflict", err)
	}
	if name := ports.ConstraintNameOf(err); name != "wt_provider_idempotency_key_uniq" {
		t.Fatalf("constraint = %q", name)
	}
}

func TestTransactionDuplicateExternalIDRejected(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")

	first := newExternal(t, "00000000-0000-7000-8000-0000000000b1", walletID, "ext-1", "key-1", wagertransaction.KindBet, "25.00")
	if err := store.Transactions().Insert(ctx, first); err != nil {
		t.Fatalf("insert first: %v", err)
	}

	// Mesmo externalTransactionId, outra chave.
	dup := newExternal(t, "00000000-0000-7000-8000-0000000000b2", walletID, "ext-1", "key-2", wagertransaction.KindBet, "25.00")
	err := store.Transactions().Insert(ctx, dup)
	if !errors.Is(err, ports.ErrIdempotencyConflict) {
		t.Fatalf("erro = %v, quer ErrIdempotencyConflict", err)
	}
	if name := ports.ConstraintNameOf(err); name != "wt_provider_external_id_uniq" {
		t.Fatalf("constraint = %q", name)
	}
}

func TestOpeningPerWalletIsUnique(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")

	opening, err := wagertransaction.NewOpening(wagertransaction.OpeningParams{
		ID: "00000000-0000-7000-8000-0000000000a1", WalletID: walletID,
		PlayerID: "player-1", Money: brl(t, "100.00"), Now: baseTime,
	})
	if err != nil {
		t.Fatalf("NewOpening: %v", err)
	}
	if err := store.Transactions().Insert(ctx, opening); err != nil {
		t.Fatalf("insert opening: %v", err)
	}

	// A OPENING precisa ser relida com resultBalance preenchido.
	loaded, err := store.Transactions().GetByID(ctx, opening.ID())
	if err != nil {
		t.Fatalf("GetByID da OPENING: %v", err)
	}
	if loaded.ResultBalance().String() != "100.00" {
		t.Fatalf("resultBalance da OPENING = %s", loaded.ResultBalance().String())
	}

	second, err := wagertransaction.NewOpening(wagertransaction.OpeningParams{
		ID: "00000000-0000-7000-8000-0000000000a2", WalletID: walletID,
		PlayerID: "player-1", Money: brl(t, "50.00"), Now: baseTime,
	})
	if err != nil {
		t.Fatalf("NewOpening 2: %v", err)
	}
	err = store.Transactions().Insert(ctx, second)
	if !errors.Is(err, ports.ErrOpeningAlreadyExists) {
		t.Fatalf("erro = %v, quer ErrOpeningAlreadyExists", err)
	}
}

func TestHasSuccessfulReversal(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-000000000001"

	insertWallet(t, store, walletID, "player-1", "100.00")

	bet := newExternal(t, "00000000-0000-7000-8000-0000000000b1", walletID, "ext-1", "key-1", wagertransaction.KindBet, "25.00")
	if err := store.Transactions().Insert(ctx, bet); err != nil {
		t.Fatalf("insert bet: %v", err)
	}

	reversed, err := store.Transactions().HasSuccessfulReversal(ctx, bet.ID())
	if err != nil {
		t.Fatalf("HasSuccessfulReversal: %v", err)
	}
	if reversed {
		t.Fatal("não deveria haver reversão ainda")
	}

	// REFUND síncrono: resolve a referência ainda em PENDING e conclui.
	refund := newExternal(t, "00000000-0000-7000-8000-0000000000c1", walletID, "ext-refund-1", "key-refund-1", wagertransaction.KindRefund, "25.00")
	if err := refund.ResolveReference(bet.ID(), baseTime); err != nil {
		t.Fatalf("ResolveReference: %v", err)
	}
	if err := refund.MarkProcessed(brl(t, "100.00"), baseTime.Add(time.Minute)); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := store.Transactions().Insert(ctx, refund); err != nil {
		t.Fatalf("insert refund: %v", err)
	}

	reversed, err = store.Transactions().HasSuccessfulReversal(ctx, bet.ID())
	if err != nil {
		t.Fatalf("HasSuccessfulReversal: %v", err)
	}
	if !reversed {
		t.Fatal("deveria haver reversão bem-sucedida")
	}

	// Segunda reversão da mesma referência é bloqueada pelo índice único.
	rollback := newExternal(t, "00000000-0000-7000-8000-0000000000c2", walletID, "ext-rollback-1", "key-rollback-1", wagertransaction.KindRollback, "25.00")
	if err := rollback.ResolveReference(bet.ID(), baseTime); err != nil {
		t.Fatalf("ResolveReference rollback: %v", err)
	}
	if err := rollback.MarkProcessed(brl(t, "100.00"), baseTime.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkProcessed rollback: %v", err)
	}
	err = store.Transactions().Insert(ctx, rollback)
	if !errors.Is(err, ports.ErrReferenceAlreadyReversed) {
		t.Fatalf("erro = %v, quer ErrReferenceAlreadyReversed", err)
	}
	if name := ports.ConstraintNameOf(err); name != "wt_single_successful_reversal_per_reference" {
		t.Fatalf("constraint = %q", name)
	}
}

func TestNotFoundTranslation(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()

	if _, err := store.Wallets().Get(ctx, "00000000-0000-7000-8000-0000000000ff"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("Get carteira inexistente: erro = %v, quer ErrNotFound", err)
	}
	if _, err := store.Transactions().GetByID(ctx, "00000000-0000-7000-8000-0000000000ff"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("Get transação inexistente: erro = %v, quer ErrNotFound", err)
	}
	if _, err := store.Transactions().FindByProviderAndIdempotencyKey(ctx, "provider-a", "nao-existe"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("busca por chave inexistente: erro = %v, quer ErrNotFound", err)
	}
}

// ── outbox e inbox ───────────────────────────────────────────────────

func TestOutboxInsertIsIdempotentByEventID(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()

	env, err := events.NewWalletBalanceChanged(
		events.Meta{EventID: "00000000-0000-7000-8000-0000000000a1", CorrelationID: "corr-1", OccurredAt: baseTime},
		events.BalanceChangedParams{
			WalletID: "00000000-0000-7000-8000-000000000001", TransactionID: "00000000-0000-7000-8000-0000000000b1",
			Direction: ledger.DirectionDebit, Money: brl(t, "25.00"),
			BalanceBefore: brl(t, "100.00"), BalanceAfter: brl(t, "75.00"), WalletVersion: 2,
		})
	if err != nil {
		t.Fatalf("evento: %v", err)
	}

	if err := store.Outbox().Insert(ctx, env); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := store.Outbox().Insert(ctx, env); err != nil {
		t.Fatalf("insert 2 (replay) deveria ser no-op sem erro: %v", err)
	}

	if count := testsupport.QueryInt(t, `SELECT count(*) FROM outbox_events`); count != 1 {
		t.Fatalf("eventos na outbox = %d, quer 1", count)
	}
	if count := testsupport.QueryInt(t, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); count != 1 {
		t.Fatalf("eventos pendentes = %d, quer 1", count)
	}
}

func TestInboxInsertAndComplete(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()

	msg := ports.InboxMessage{
		ConsumerName: "wager-transactions-consumer",
		MessageID:    "msg-1",
		PayloadHash:  "hash-msg-1",
		ReceivedAt:   baseTime,
	}

	inserted, err := store.Inbox().Insert(ctx, msg)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if !inserted {
		t.Fatal("primeira inserção deveria retornar inserted=true")
	}

	inserted, err = store.Inbox().Insert(ctx, msg)
	if err != nil {
		t.Fatalf("Insert duplicado: %v", err)
	}
	if inserted {
		t.Fatal("reentrega deveria retornar inserted=false")
	}

	stored, err := store.Inbox().Get(ctx, msg.ConsumerName, msg.MessageID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Completed {
		t.Fatal("mensagem não deveria estar concluída")
	}
	if stored.PayloadHash != "hash-msg-1" {
		t.Fatalf("payloadHash = %q", stored.PayloadHash)
	}

	if err := store.Inbox().Complete(ctx, msg.ConsumerName, msg.MessageID, ""); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	stored, err = store.Inbox().Get(ctx, msg.ConsumerName, msg.MessageID)
	if err != nil {
		t.Fatalf("Get após Complete: %v", err)
	}
	if !stored.Completed {
		t.Fatal("mensagem deveria estar concluída")
	}
}
