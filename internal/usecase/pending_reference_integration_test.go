//go:build integration

package usecase_test

import (
	"context"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// Estes testes exercitam a RETOMADA de PENDING_REFERENCE com PostgreSQL real.
// É o cenário que o README exige em §12 item 8: uma operação que espera por uma
// referência é assumida e concluída por outra execução, sem perder nem duplicar
// dinheiro.
//
// A política tem backoff curto em todos os testes para que a operação fique
// VENCIDA rapidamente; sem isso, o agendamento inicial (5s no default) exigiria
// espera real. Os valores continuam exercitando o mesmo caminho de código.

// setupWithPolicy monta o caso de uso com uma política de referência específica.
func setupWithPolicy(t *testing.T, policy usecase.ReferencePolicy) (ports.Store, *usecase.OpenWallet, *usecase.ProcessTransaction) {
	t.Helper()

	store := testsupport.NewStore(t)
	ids := usecase.UUIDv7{}
	clock := usecase.SystemClock{}
	return store, usecase.NewOpenWallet(store, ids, clock),
		usecase.NewProcessTransaction(store, ids, clock, policy)
}

// fastPolicy devolve uma política com backoff curto: a operação fica vencida em
// poucos milissegundos, sem alterar mais nenhuma regra.
func fastPolicy(maxAttempts int, ttl time.Duration) usecase.ReferencePolicy {
	return usecase.ReferencePolicy{
		MaxAttempts: maxAttempts,
		TTL:         ttl,
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  time.Minute,
	}
}

// waitDue aguarda a operação ficar vencida. Espera curta e explícita, e não
// laço: o agendamento é de 50ms, então 90ms é suficiente com folga.
func waitDue() { time.Sleep(90 * time.Millisecond) }

func statusOf(t *testing.T, transactionID string) string {
	t.Helper()
	return testsupport.QueryString(t,
		`SELECT status FROM wager_transactions WHERE id = $1::uuid`, transactionID)
}

func failureCodeOf(t *testing.T, transactionID string) string {
	t.Helper()
	return testsupport.QueryString(t,
		`SELECT COALESCE(failure_code, '') FROM wager_transactions WHERE id = $1::uuid`, transactionID)
}

func resolvedReferenceOf(t *testing.T, transactionID string) string {
	t.Helper()
	return testsupport.QueryString(t,
		`SELECT COALESCE(reference_transaction_id::text, '') FROM wager_transactions WHERE id = $1::uuid`, transactionID)
}

// ── resolve quando a referência chega ────────────────────────────────

func TestPendingReferenceResolvesWhenTheReferenceArrives(t *testing.T) {
	store, openWallet, process := setupWithPolicy(t, fastPolicy(10, time.Hour))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	// O REFUND chega ANTES da BET que ele referencia: fica pendente.
	refundCmd := command(t, walletID, "ext-refund", "REFUND", "25.00")
	refundCmd.ReferenceExternalID = "ext-bet"
	pending := mustProcess(t, process, refundCmd)
	if pending.Status != wagertransaction.StatusPendingReference {
		t.Fatalf("status = %s, quer PENDING_REFERENCE", pending.Status)
	}

	// Sem movimentação enquanto espera.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo pendente = %s, quer 100.00 (espera não movimenta)", got)
	}

	waitDue()

	// A BET chega e conclui.
	bet := mustProcess(t, process, command(t, walletID, "ext-bet", "BET", "25.00"))
	if bet.Status != wagertransaction.StatusProcessed {
		t.Fatalf("status da BET = %s, quer PROCESSED", bet.Status)
	}
	if got := storedBalance(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo após a BET = %s, quer 75.00", got)
	}

	// A retomada resolve a pendência.
	resolution, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("ResolveNextPendingReference: %v", err)
	}
	if !resolution.Attempted {
		t.Fatal("deveria haver uma operação vencida para tentar")
	}
	if resolution.Outcome != usecase.OutcomeResolved {
		t.Fatalf("desfecho = %s, quer RESOLVED", resolution.Outcome)
	}
	if resolution.TransactionID != pending.TransactionID {
		t.Fatalf("tentou %s, quer a operação pendente %s", resolution.TransactionID, pending.TransactionID)
	}

	// O crédito da reversão foi aplicado: 75 + 25 = 100.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo após a retomada = %s, quer 100.00", got)
	}
	if got := statusOf(t, pending.TransactionID); got != "PROCESSED" {
		t.Fatalf("status da operação retomada = %s, quer PROCESSED", got)
	}

	// A referência INTERNA resolvida é persistida: é o que torna a reversão
	// auditável e alimenta o índice que impede reverter o mesmo débito duas vezes.
	if got := resolvedReferenceOf(t, pending.TransactionID); got != bet.TransactionID {
		t.Fatalf("reference_transaction_id = %q, quer o id da BET %q", got, bet.TransactionID)
	}

	// Ledger coerente: abertura (crédito), BET (débito) e REFUND (crédito).
	if n := ledgerCount(t, walletID, "CREDIT"); n != 2 {
		t.Fatalf("créditos no ledger = %d, quer 2 (abertura + refund)", n)
	}
	if n := ledgerCount(t, walletID, "DEBIT"); n != 1 {
		t.Fatalf("débitos no ledger = %d, quer 1 (BET)", n)
	}
}

// ── reagendamento com backoff ────────────────────────────────────────

func TestPendingReferenceIsRescheduledWithGrowingBackoff(t *testing.T) {
	store, openWallet, process := setupWithPolicy(t, fastPolicy(10, time.Hour))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	refundCmd := command(t, walletID, "ext-refund", "REFUND", "25.00")
	refundCmd.ReferenceExternalID = "ext-bet-que-nunca-chega"
	pending := mustProcess(t, process, refundCmd)

	waitDue()

	resolution, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("ResolveNextPendingReference: %v", err)
	}
	if resolution.Outcome != usecase.OutcomeRescheduled {
		t.Fatalf("desfecho = %s, quer RESCHEDULED", resolution.Outcome)
	}
	// A pendência inicial registra 1 tentativa; esta é a 2ª.
	if resolution.Attempts != 2 {
		t.Fatalf("attempts = %d, quer 2", resolution.Attempts)
	}
	if resolution.NextAttemptAt.IsZero() {
		t.Fatal("RESCHEDULED deveria informar o próximo agendamento")
	}

	// O backoff CRESCE: Backoff(2) = 100ms > BaseBackoff = 50ms. O limite
	// inferior é conservador de propósito (o instante exato depende do relógio),
	// mas já distingue "cresceu" de "repetiu o base".
	if remaining := time.Until(resolution.NextAttemptAt); remaining <= 50*time.Millisecond {
		t.Fatalf("próxima tentativa em %s, deveria exceder o backoff base (50ms)", remaining)
	}

	// Nada de financeiro foi tocado, e a operação segue pendente.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00 (reagendar não movimenta)", got)
	}
	if got := statusOf(t, pending.TransactionID); got != "PENDING_REFERENCE" {
		t.Fatalf("status = %s, quer PENDING_REFERENCE", got)
	}
	if n := ledgerCount(t, walletID, "DEBIT"); n != 0 {
		t.Fatalf("débitos = %d, quer 0 (a operação não foi aplicada)", n)
	}
	if n := outboxCount(t, "WagerTransactionProcessed"); n != 1 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 1 (apenas a abertura)", n)
	}
}

// ── esgotamento ──────────────────────────────────────────────────────

func TestPendingReferenceExhaustsByAttempts(t *testing.T) {
	store, openWallet, process := setupWithPolicy(t, fastPolicy(2, time.Hour))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	refundCmd := command(t, walletID, "ext-refund", "REFUND", "25.00")
	refundCmd.ReferenceExternalID = "ext-bet-que-nunca-chega"
	pending := mustProcess(t, process, refundCmd)

	// 1ª retomada: ainda há tentativa disponível (attempts passa a 2).
	waitDue()
	first, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("primeira retomada: %v", err)
	}
	if first.Outcome != usecase.OutcomeRescheduled {
		t.Fatalf("primeira retomada = %s, quer RESCHEDULED", first.Outcome)
	}

	// 2ª retomada: attempts chegaria a 3, acima do máximo de 2 => esgota.
	time.Sleep(150 * time.Millisecond)
	second, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("segunda retomada: %v", err)
	}
	if second.Outcome != usecase.OutcomeExhausted {
		t.Fatalf("segunda retomada = %s, quer EXHAUSTED", second.Outcome)
	}

	// Finalizada como REJECTED, com código de falha estável.
	if got := statusOf(t, pending.TransactionID); got != "REJECTED" {
		t.Fatalf("status = %s, quer REJECTED", got)
	}
	if got := failureCodeOf(t, pending.TransactionID); got != usecase.CodeReferenceTimeout {
		t.Fatalf("failure_code = %q, quer %q", got, usecase.CodeReferenceTimeout)
	}
	if n := outboxCount(t, "WagerTransactionRejected"); n != 1 {
		t.Fatalf("WagerTransactionRejected = %d, quer 1", n)
	}

	// Rejeitar não movimenta: o saldo é o da abertura.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00", got)
	}
	if n := ledgerCount(t, walletID, "CREDIT"); n != 1 {
		t.Fatalf("créditos = %d, quer 1 (apenas a abertura)", n)
	}

	// E não há mais nada vencido: a operação saiu do estado pendente.
	after, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("retomada após esgotar: %v", err)
	}
	if after.Attempted {
		t.Fatal("uma operação REJECTED não pode voltar a ser tentada")
	}
}

func TestPendingReferenceExhaustsByTTL(t *testing.T) {
	// TTL de 1 nanossegundo: qualquer idade já o excede. Isola o critério de TTL
	// do critério de tentativas, que fica alto de propósito.
	store, openWallet, process := setupWithPolicy(t, fastPolicy(100, time.Nanosecond))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	refundCmd := command(t, walletID, "ext-refund", "REFUND", "25.00")
	refundCmd.ReferenceExternalID = "ext-bet-que-nunca-chega"
	pending := mustProcess(t, process, refundCmd)

	waitDue()

	resolution, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("ResolveNextPendingReference: %v", err)
	}
	if resolution.Outcome != usecase.OutcomeExhausted {
		t.Fatalf("desfecho = %s, quer EXHAUSTED (TTL vencido)", resolution.Outcome)
	}
	if got := statusOf(t, pending.TransactionID); got != "REJECTED" {
		t.Fatalf("status = %s, quer REJECTED", got)
	}
	if got := failureCodeOf(t, pending.TransactionID); got != usecase.CodeReferenceTimeout {
		t.Fatalf("failure_code = %q, quer %q", got, usecase.CodeReferenceTimeout)
	}

	// Esgotar por TTL rejeita SEM movimentar: o saldo é o da abertura e não há
	// lançamento de débito. Sem esta asserção o teste passaria aprovando um
	// caminho que rejeita corretamente mas poderia ter movimentado dinheiro
	// antes de decidir — e é justamente isso que o TTL não pode causar.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00 (a política esgotada não movimenta)", got)
	}
	if n := ledgerCount(t, walletID, "DEBIT"); n != 0 {
		t.Fatalf("débitos = %d, quer 0 (a operação nunca foi aplicada)", n)
	}
}

// ── rejeição por regra, com a referência presente ────────────────────

func TestPendingReferenceRejectsIncompatibleReference(t *testing.T) {
	store, openWallet, process := setupWithPolicy(t, fastPolicy(10, time.Hour))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	refundCmd := command(t, walletID, "ext-refund", "REFUND", "25.00")
	refundCmd.ReferenceExternalID = "ext-bet"
	pending := mustProcess(t, process, refundCmd)

	waitDue()

	// A BET aparece, mas com OUTRA rodada: a referência deixa de ser compatível
	// e a rejeição é definitiva (não adianta continuar tentando).
	betCmd := command(t, walletID, "ext-bet", "BET", "25.00")
	betCmd.RoundID = "outra-rodada"
	if res := mustProcess(t, process, betCmd); res.Status != wagertransaction.StatusProcessed {
		t.Fatalf("status da BET = %s, quer PROCESSED", res.Status)
	}

	resolution, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("ResolveNextPendingReference: %v", err)
	}
	if resolution.Outcome != usecase.OutcomeRejected {
		t.Fatalf("desfecho = %s, quer REJECTED", resolution.Outcome)
	}
	if got := statusOf(t, pending.TransactionID); got != "REJECTED" {
		t.Fatalf("status = %s, quer REJECTED", got)
	}
	if got := failureCodeOf(t, pending.TransactionID); got != usecase.CodeReferenceRoundMismatch {
		t.Fatalf("failure_code = %q, quer %q", got, usecase.CodeReferenceRoundMismatch)
	}

	// O débito da BET permanece; o REFUND não foi aplicado.
	if got := storedBalance(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00", got)
	}
	if n := ledgerCount(t, walletID, "CREDIT"); n != 1 {
		t.Fatalf("créditos = %d, quer 1 (o REFUND rejeitado não credita)", n)
	}
}

// ── nada a fazer ─────────────────────────────────────────────────────

func TestResolveNextPendingReferenceWithoutDueWorkDoesNothing(t *testing.T) {
	store, openWallet, process := setupWithPolicy(t, fastPolicy(10, time.Hour))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	resolution, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("ResolveNextPendingReference: %v", err)
	}
	if resolution.Attempted {
		t.Fatal("sem operação pendente vencida, Attempted deveria ser false")
	}

	// Nada foi tocado.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00", got)
	}
	if n := ledgerCount(t, walletID, "CREDIT"); n != 1 {
		t.Fatalf("créditos = %d, quer 1 (apenas a abertura)", n)
	}
}

// TestPendingReferenceCannotRefundTheSameReferenceTwice é o teste de regressão
// do risco mais grave da retomada.
//
// Duas reversões da MESMA referência ficam pendentes — ambas criadas antes de a
// BET existir. A primeira é retomada e aplicada; a segunda tem de ser RECUSADA.
//
// A proteção é dupla: a checagem HasSuccessfulReversal na retomada, e o índice
// wt_single_successful_reversal_per_reference como rede de segurança.
//
// O que este teste pega: se a retomada aplicar o crédito SEM persistir
// reference_transaction_id, o índice deixa de ver a primeira reversão (NULL não
// colide em índice único) e a SEGUNDA também seria aplicada — a mesma aposta
// devolvida duas vezes, que é o critério eliminatório "movimentação duplicada".
func TestPendingReferenceCannotRefundTheSameReferenceTwice(t *testing.T) {
	store, openWallet, process := setupWithPolicy(t, fastPolicy(10, time.Hour))
	walletID := mustOpenWallet(t, openWallet, "player-1", "100.00").WalletID

	// As duas reversões chegam ANTES da BET referenciada.
	firstCmd := command(t, walletID, "ext-refund-1", "REFUND", "25.00")
	firstCmd.ReferenceExternalID = "ext-bet"
	first := mustProcess(t, process, firstCmd)
	if first.Status != wagertransaction.StatusPendingReference {
		t.Fatalf("primeira reversão = %s, quer PENDING_REFERENCE", first.Status)
	}

	secondCmd := command(t, walletID, "ext-refund-2", "REFUND", "25.00")
	secondCmd.ReferenceExternalID = "ext-bet"
	second := mustProcess(t, process, secondCmd)
	if second.Status != wagertransaction.StatusPendingReference {
		t.Fatalf("segunda reversão = %s, quer PENDING_REFERENCE", second.Status)
	}

	waitDue()

	// A BET chega e conclui.
	bet := mustProcess(t, process, command(t, walletID, "ext-bet", "BET", "25.00"))
	if got := storedBalance(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo após a BET = %s, quer 75.00", got)
	}

	// Primeira retomada: a reversão mais antiga é aplicada UMA vez.
	resolved, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("primeira retomada: %v", err)
	}
	if resolved.Outcome != usecase.OutcomeResolved {
		t.Fatalf("primeira retomada = %s, quer RESOLVED", resolved.Outcome)
	}
	if resolved.TransactionID != first.TransactionID {
		t.Fatalf("retomou %s, quer a reversão mais antiga %s", resolved.TransactionID, first.TransactionID)
	}
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo após a primeira reversão = %s, quer 100.00", got)
	}
	// A referência interna PRECISA estar persistida: é ela que alimenta o índice
	// que impede a segunda reversão.
	if got := resolvedReferenceOf(t, resolved.TransactionID); got != bet.TransactionID {
		t.Fatalf("reference_transaction_id = %q, quer o id da BET %q", got, bet.TransactionID)
	}

	// Segunda retomada (a outra reversão, mesma referência): recusada.
	time.Sleep(150 * time.Millisecond)
	refused, err := process.ResolveNextPendingReference(context.Background())
	if err != nil {
		t.Fatalf("segunda retomada: %v", err)
	}
	if refused.Outcome != usecase.OutcomeRejected {
		t.Fatalf("segunda retomada = %s, quer REJECTED (reversão já aplicada)", refused.Outcome)
	}
	if refused.TransactionID != second.TransactionID {
		t.Fatalf("retomou %s, quer a segunda reversão %s", refused.TransactionID, second.TransactionID)
	}
	if got := failureCodeOf(t, refused.TransactionID); got != usecase.CodeReferenceAlreadyReversed {
		t.Fatalf("failure_code = %q, quer %q", got, usecase.CodeReferenceAlreadyReversed)
	}

	// O saldo NÃO volta a subir: a aposta foi devolvida uma única vez.
	if got := storedBalance(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00 (a mesma aposta não pode ser devolvida duas vezes)", got)
	}
	if n := ledgerCount(t, walletID, "CREDIT"); n != 2 {
		t.Fatalf("créditos no ledger = %d, quer 2 (abertura + UMA reversão)", n)
	}
	if n := ledgerCount(t, walletID, "DEBIT"); n != 1 {
		t.Fatalf("débitos no ledger = %d, quer 1 (a BET)", n)
	}
}
