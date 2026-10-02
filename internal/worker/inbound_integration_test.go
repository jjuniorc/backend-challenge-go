//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// Estes testes exercitam a exigência do README §6.5 com PostgreSQL REAL: o
// registro da inbox e a conclusão do tratamento compartilham a transação SQL das
// alterações de domínio, do ledger e da outbox.
//
// O que eles provam, e que um teste com dublê não provaria:
//   - a reentrega depois do commit e antes da remoção da mensagem NÃO movimenta
//     o saldo duas vezes;
//   - entregas CONCORRENTES da mesma mensagem produzem um único débito;
//   - reentrega com conteúdo diferente é recusada como conflito permanente.

const testConsumerName = "wager-transactions"

func setupInbound(t *testing.T) (ports.Store, *usecase.ProcessTransaction, string) {
	t.Helper()
	store := testsupport.NewStore(t)
	ids := usecase.UUIDv7{}
	clock := usecase.SystemClock{}

	balance, err := money.ParseDecimal("100.00", "BRL")
	if err != nil {
		t.Fatalf("setup money: %v", err)
	}

	opened, err := usecase.NewOpenWallet(store, ids, clock).Execute(context.Background(), usecase.OpenWalletCommand{
		PlayerID:       "player-1",
		InitialBalance: balance,
		CorrelationID:  "corr-open",
	})
	if err != nil {
		t.Fatalf("OpenWallet: %v", err)
	}

	process := usecase.NewProcessTransaction(store, ids, clock, usecase.DefaultReferencePolicy())
	return store, process, opened.WalletID
}

type inboundParams struct {
	messageID  string
	walletID   string
	externalID string
	kind       string
	amount     string
	reference  string
}

// inboundFrom monta a mensagem no formato do contrato de entrada do README §10.
func inboundFrom(t *testing.T, p inboundParams) ports.InboundMessage {
	t.Helper()
	data := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": p.externalID,
		"idempotencyKey":        "provider-a:" + p.externalID,
		"playerId":              "player-1",
		"walletId":              p.walletID,
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  p.kind,
		"money":                 map[string]any{"amount": p.amount, "currency": "BRL"},
	}
	if p.reference != "" {
		data["referenceExternalTransactionId"] = p.reference
	}

	body, err := json.Marshal(map[string]any{
		"messageId":  p.messageID,
		"type":       TypeTransactionRequested,
		"occurredAt": "2026-09-08T12:00:00.000Z",
		"data":       data,
	})
	if err != nil {
		t.Fatalf("montando o corpo: %v", err)
	}

	return ports.InboundMessage{
		MessageID:     p.messageID,
		ReceiptHandle: "handle-" + p.messageID,
		Body:          body,
		ReceiveCount:  1,
		GroupID:       p.walletID,
		ReceivedAt:    time.Now().UTC(),
	}
}

func processInbound(t *testing.T, processor *TransactionProcessor, msg ports.InboundMessage) error {
	t.Helper()
	parsed, err := ParseInboundTransaction(msg.Body)
	if err != nil {
		t.Fatalf("ParseInboundTransaction: %v", err)
	}
	return processor.ProcessInbound(context.Background(), msg, parsed)
}

// ── consultas de verificação ─────────────────────────────────────────

func inboundRows(t *testing.T, messageID string) (total, completed int64) {
	t.Helper()
	total = testsupport.QueryInt(t,
		`SELECT count(*) FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		testConsumerName, messageID)
	completed = testsupport.QueryInt(t,
		`SELECT count(*) FROM inbox_messages
		  WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NOT NULL
		    AND transaction_id IS NOT NULL`,
		testConsumerName, messageID)
	return total, completed
}

func ledgerRows(t *testing.T, walletID string) int64 {
	t.Helper()
	return testsupport.QueryInt(t,
		`SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1::uuid`, walletID)
}

func transactionRows(t *testing.T, walletID string) int64 {
	t.Helper()
	return testsupport.QueryInt(t,
		`SELECT count(*) FROM wager_transactions WHERE wallet_id = $1::uuid`, walletID)
}

func outboxRows(t *testing.T, eventType string) int64 {
	t.Helper()
	return testsupport.QueryInt(t, `SELECT count(*) FROM outbox_events WHERE event_type = $1`, eventType)
}

func balanceOf(t *testing.T, store ports.Store, walletID string) string {
	t.Helper()
	w, err := store.Wallets().Get(context.Background(), walletID)
	if err != nil {
		t.Fatalf("Get wallet: %v", err)
	}
	return w.Balance().String()
}

// ── atomicidade e reentrega ──────────────────────────────────────────

func TestInboundProcessingIsAtomicAndReplaySafe(t *testing.T) {
	store, process, walletID := setupInbound(t)
	processor := NewTransactionProcessor(process, testConsumerName)
	msg := inboundFrom(t, inboundParams{
		messageID: "msg-1", walletID: walletID, externalID: "ext-1", kind: "BET", amount: "25.00",
	})

	// Abertura deixou 1 lançamento (crédito) e 1 transação (OPENING).
	if err := processInbound(t, processor, msg); err != nil {
		t.Fatalf("primeiro tratamento: %v", err)
	}

	// Um único commit confirmou: saldo, lançamento, transação, inbox e eventos.
	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos = %d, quer 2 (abertura + débito)", n)
	}
	if n := transactionRows(t, walletID); n != 2 {
		t.Fatalf("transações = %d, quer 2 (OPENING + BET)", n)
	}
	total, completed := inboundRows(t, "msg-1")
	if total != 1 || completed != 1 {
		t.Fatalf("inbox: total = %d, concluídas = %d; quer 1 e 1", total, completed)
	}
	if n := outboxRows(t, "WagerTransactionProcessed"); n != 2 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 2 (abertura + aposta)", n)
	}
	if n := outboxRows(t, "WalletBalanceChanged"); n != 2 {
		t.Fatalf("WalletBalanceChanged = %d, quer 2", n)
	}

	// REENTREGA: é exatamente o que acontece se o processo morrer depois do
	// commit e antes de remover a mensagem da fila.
	if err := processInbound(t, processor, msg); err != nil {
		t.Fatalf("reentrega deveria ser tratada como replay, veio: %v", err)
	}

	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo após reentrega = %s, quer 75.00 (nenhuma movimentação nova)", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos após reentrega = %d, quer 2", n)
	}
	if n := transactionRows(t, walletID); n != 2 {
		t.Fatalf("transações após reentrega = %d, quer 2", n)
	}
	total, completed = inboundRows(t, "msg-1")
	if total != 1 || completed != 1 {
		t.Fatalf("inbox após reentrega: total = %d, concluídas = %d; quer 1 e 1", total, completed)
	}
	if n := outboxRows(t, "WagerTransactionProcessed"); n != 2 {
		t.Fatalf("eventos após reentrega = %d, quer 2 (replay não publica de novo)", n)
	}
}

func TestRedeliveryWithDifferentContentIsRejected(t *testing.T) {
	store, process, walletID := setupInbound(t)
	processor := NewTransactionProcessor(process, testConsumerName)

	original := inboundFrom(t, inboundParams{
		messageID: "msg-1", walletID: walletID, externalID: "ext-1", kind: "BET", amount: "25.00",
	})
	if err := processInbound(t, processor, original); err != nil {
		t.Fatalf("tratamento original: %v", err)
	}

	// Mesmo messageId, conteúdo diferente: 30.00 em vez de 25.00.
	tampered := inboundFrom(t, inboundParams{
		messageID: "msg-1", walletID: walletID, externalID: "ext-1", kind: "BET", amount: "30.00",
	})
	err := processInbound(t, processor, tampered)

	if err == nil {
		t.Fatal("reentrega com conteúdo diferente deveria falhar")
	}
	if !domainerr.IsKind(err, domainerr.KindConflict) {
		t.Fatalf("erro = %v, quer KindConflict (falha PERMANENTE, vai para a DLQ)", err)
	}
	if !strings.Contains(err.Error(), usecase.CodeInboxPayloadHashMismatch) {
		t.Fatalf("erro = %v, deveria trazer o código %s", err, usecase.CodeInboxPayloadHashMismatch)
	}

	// Nada de domínio foi aplicado.
	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00 (entrega recusada não movimenta)", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos = %d, quer 2", n)
	}
	if total, _ := inboundRows(t, "msg-1"); total != 1 {
		t.Fatalf("linhas de inbox = %d, quer 1 (o registro do rollback não permanece)", total)
	}
}

// ── concorrência entre entregas da mesma mensagem ────────────────────

func TestConcurrentRedeliveryOfTheSameMessageMovesBalanceOnce(t *testing.T) {
	store, process, walletID := setupInbound(t)
	processor := NewTransactionProcessor(process, testConsumerName)
	msg := inboundFrom(t, inboundParams{
		messageID: "msg-1", walletID: walletID, externalID: "ext-1", kind: "BET", amount: "25.00",
	})

	const deliveries = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	start := make(chan struct{})

	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			parsed, err := ParseInboundTransaction(msg.Body)
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("entrega %d: %w", idx, err))
				mu.Unlock()
				return
			}
			<-start
			if err := processor.ProcessInbound(context.Background(), msg, parsed); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("entrega %d: %w", idx, err))
				mu.Unlock()
			}
		}(i)
	}
	close(start)
	wg.Wait()

	// Nenhuma entrega pode falhar: as perdedoras viram replay, não erro.
	if len(errs) > 0 {
		t.Fatalf("erros inesperados: %v", errs)
	}

	// E o efeito financeiro é único.
	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00 (um único débito)", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos = %d, quer 2", n)
	}
	if n := transactionRows(t, walletID); n != 2 {
		t.Fatalf("transações = %d, quer 2", n)
	}
	total, completed := inboundRows(t, "msg-1")
	if total != 1 || completed != 1 {
		t.Fatalf("inbox: total = %d, concluídas = %d; quer 1 e 1", total, completed)
	}
}

// ── pendência de referência ──────────────────────────────────────────

func TestPendingReferenceCompletesTheInboxMessage(t *testing.T) {
	store, process, walletID := setupInbound(t)
	processor := NewTransactionProcessor(process, testConsumerName)

	// REFUND cuja BET referenciada ainda não existe: vira PENDING_REFERENCE.
	msg := inboundFrom(t, inboundParams{
		messageID: "msg-refund", walletID: walletID, externalID: "ext-refund",
		kind: "REFUND", amount: "25.00", reference: "ext-bet-ainda-nao-chegou",
	})

	if err := processInbound(t, processor, msg); err != nil {
		t.Fatalf("tratamento do REFUND pendente: %v", err)
	}

	// README §6.5: a mensagem PODE ser concluída depois de a pendência estar
	// persistida — o worker de referências assume a continuidade.
	total, completed := inboundRows(t, "msg-refund")
	if total != 1 || completed != 1 {
		t.Fatalf("inbox: total = %d, concluídas = %d; quer 1 e 1", total, completed)
	}
	if n := outboxRows(t, "WagerTransactionPendingReference"); n != 1 {
		t.Fatalf("WagerTransactionPendingReference = %d, quer 1", n)
	}

	// A pendência foi persistida com estado e agendamento, sem movimentar saldo.
	status := testsupport.QueryString(t,
		`SELECT status FROM wager_transactions WHERE wallet_id = $1::uuid AND kind = 'REFUND'`, walletID)
	if status != "PENDING_REFERENCE" {
		t.Fatalf("status = %q, quer PENDING_REFERENCE", status)
	}
	if got := balanceOf(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00 (pendência não movimenta)", got)
	}
}

// ── interação entre HTTP e SQS ───────────────────────────────────────

func TestHTTPAndSQSShareTheSameOperationIdentity(t *testing.T) {
	store, process, walletID := setupInbound(t)
	processor := NewTransactionProcessor(process, testConsumerName)

	// A MESMA operação chega primeiro pelo caminho HTTP (sem inbox)...
	httpCommand := usecase.ProcessTransactionCommand{
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		IdempotencyKey:        "provider-a:ext-1",
		PlayerID:              "player-1",
		WalletID:              walletID,
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Money:                 brlMoney(t, "25.00"),
		CorrelationID:         "corr-http",
	}
	result, err := process.Execute(context.Background(), httpCommand)
	if err != nil {
		t.Fatalf("caminho HTTP: %v", err)
	}
	if result.Status != "PROCESSED" || result.IdempotentReplay {
		t.Fatalf("resultado HTTP = %+v", result)
	}

	// ...e depois pelo SQS, com outra identidade de MENSAGEM.
	msg := inboundFrom(t, inboundParams{
		messageID: "msg-1", walletID: walletID, externalID: "ext-1", kind: "BET", amount: "25.00",
	})
	if err := processInbound(t, processor, msg); err != nil {
		t.Fatalf("caminho SQS da mesma operação: %v", err)
	}

	// Um único débito, uma única transação — e a mensagem com sua linha de inbox
	// concluída apontando para a transação que o HTTP criou.
	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00 (operação compartilhada não duplica)", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos = %d, quer 2", n)
	}
	if n := transactionRows(t, walletID); n != 2 {
		t.Fatalf("transações = %d, quer 2", n)
	}
	total, completed := inboundRows(t, "msg-1")
	if total != 1 || completed != 1 {
		t.Fatalf("inbox: total = %d, concluídas = %d; quer 1 e 1 (a mensagem fica auditável)", total, completed)
	}
}

// ── helpers locais ───────────────────────────────────────────────────

func brlMoney(t *testing.T, value string) money.Money {
	t.Helper()
	parsed, err := money.ParseDecimal(value, "BRL")
	if err != nil {
		t.Fatalf("money(%q): %v", value, err)
	}
	return parsed
}
