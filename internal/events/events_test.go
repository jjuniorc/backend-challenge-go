package events

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
)

var fixedTime = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("setup money(%q): %v", s, err)
	}
	return m
}

func meta() Meta {
	return Meta{
		EventID:       "00000000-0000-7000-8000-0000000000a1",
		CorrelationID: "corr-1",
		OccurredAt:    fixedTime,
	}
}

func TestProcessedExactJSON(t *testing.T) {
	env, err := NewWagerTransactionProcessed(meta(), ProcessedParams{
		TransactionID: "tx-1",
		ProviderID:    "provider-a",
		WalletID:      "w-1",
		Kind:          wagertransaction.KindBet,
		Status:        wagertransaction.StatusProcessed,
		Money:         brl(t, "25.00"),
		Balance:       brl(t, "975.00"),
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	if env.EventType != TypeWagerTransactionProcessed {
		t.Fatalf("eventType = %s", env.EventType)
	}
	if env.Version != CurrentVersion {
		t.Fatalf("version = %d, quer %d", env.Version, CurrentVersion)
	}
	if env.AggregateID != "w-1" {
		t.Fatalf("aggregateId = %s, quer a carteira", env.AggregateID)
	}

	payload, err := env.Payload()
	if err != nil {
		t.Fatalf("Payload falhou: %v", err)
	}
	want := `{"eventId":"00000000-0000-7000-8000-0000000000a1","eventType":"WagerTransactionProcessed","aggregateId":"w-1","correlationId":"corr-1","occurredAt":"2026-09-08T12:00:00.000Z","version":1,"data":{"transactionId":"tx-1","providerId":"provider-a","walletId":"w-1","kind":"BET","status":"PROCESSED","money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"975.00","currency":"BRL"}}}`
	if string(payload) != want {
		t.Fatalf("payload divergente.\n got: %s\nwant: %s", payload, want)
	}
}

func TestWalletBalanceChangedExactJSON(t *testing.T) {
	env, err := NewWalletBalanceChanged(Meta{
		EventID:       "00000000-0000-7000-8000-0000000000a2",
		CorrelationID: "corr-1",
		CausationID:   "00000000-0000-7000-8000-0000000000a1",
		OccurredAt:    fixedTime,
	}, BalanceChangedParams{
		WalletID:      "w-1",
		TransactionID: "tx-1",
		Direction:     ledger.DirectionDebit,
		Money:         brl(t, "25.00"),
		BalanceBefore: brl(t, "100.00"),
		BalanceAfter:  brl(t, "75.00"),
		WalletVersion: 2,
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	payload, err := env.Payload()
	if err != nil {
		t.Fatalf("Payload falhou: %v", err)
	}
	want := `{"eventId":"00000000-0000-7000-8000-0000000000a2","eventType":"WalletBalanceChanged","aggregateId":"w-1","correlationId":"corr-1","causationId":"00000000-0000-7000-8000-0000000000a1","occurredAt":"2026-09-08T12:00:00.000Z","version":1,"data":{"walletId":"w-1","transactionId":"tx-1","direction":"DEBIT","money":{"amount":"25.00","currency":"BRL"},"balanceBefore":{"amount":"100.00","currency":"BRL"},"balanceAfter":{"amount":"75.00","currency":"BRL"},"walletVersion":2}}`
	if string(payload) != want {
		t.Fatalf("payload divergente.\n got: %s\nwant: %s", payload, want)
	}
}

// O payload de WalletBalanceChanged precisa conter exatamente os campos exigidos.
func TestWalletBalanceChangedRequiredKeys(t *testing.T) {
	env, err := NewWalletBalanceChanged(meta(), BalanceChangedParams{
		WalletID:      "w-1",
		TransactionID: "tx-1",
		Direction:     ledger.DirectionCredit,
		Money:         brl(t, "10.00"),
		BalanceBefore: brl(t, "0.00"),
		BalanceAfter:  brl(t, "10.00"),
		WalletVersion: 1,
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	var data map[string]any
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("data inválido: %v", err)
	}
	required := []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"}
	for _, key := range required {
		if _, ok := data[key]; !ok {
			t.Fatalf("payload deveria conter %q", key)
		}
	}
	if len(data) != len(required) {
		t.Fatalf("payload tem %d campos, quer exatamente %d: %v", len(data), len(required), data)
	}
}

func TestPendingReferenceExactJSON(t *testing.T) {
	env, err := NewWagerTransactionPendingReference(meta(), PendingReferenceParams{
		TransactionID:                  "tx-1",
		ProviderID:                     "provider-a",
		WalletID:                       "w-1",
		Kind:                           wagertransaction.KindRefund,
		ReferenceExternalTransactionID: "ext-bet-1",
		Attempts:                       1,
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	payload, err := env.Payload()
	if err != nil {
		t.Fatalf("Payload falhou: %v", err)
	}
	want := `{"eventId":"00000000-0000-7000-8000-0000000000a1","eventType":"WagerTransactionPendingReference","aggregateId":"w-1","correlationId":"corr-1","occurredAt":"2026-09-08T12:00:00.000Z","version":1,"data":{"transactionId":"tx-1","providerId":"provider-a","walletId":"w-1","kind":"REFUND","referenceExternalTransactionId":"ext-bet-1","attempts":1}}`
	if string(payload) != want {
		t.Fatalf("payload divergente.\n got: %s\nwant: %s", payload, want)
	}
}

func TestRejectedOmitsEmptyFailureMessage(t *testing.T) {
	env, err := NewWagerTransactionRejected(meta(), RejectedParams{
		TransactionID: "tx-1",
		ProviderID:    "provider-a",
		WalletID:      "w-1",
		Kind:          wagertransaction.KindBet,
		FailureCode:   "WALLET_INSUFFICIENT_FUNDS",
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	payload, err := env.Payload()
	if err != nil {
		t.Fatalf("Payload falhou: %v", err)
	}
	if strings.Contains(string(payload), "failureMessage") {
		t.Fatalf("failureMessage vazio deveria ser omitido: %s", payload)
	}
	if !strings.Contains(string(payload), `"failureCode":"WALLET_INSUFFICIENT_FUNDS"`) {
		t.Fatalf("failureCode ausente: %s", payload)
	}
}

// OPENING é interno: providerId não se aplica e deve ser omitido.
func TestProcessedOmitsProviderForOpening(t *testing.T) {
	env, err := NewWagerTransactionProcessed(meta(), ProcessedParams{
		TransactionID: "op-1",
		WalletID:      "w-1",
		Kind:          wagertransaction.KindOpening,
		Status:        wagertransaction.StatusProcessed,
		Money:         brl(t, "1000.00"),
		Balance:       brl(t, "1000.00"),
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	if strings.Contains(string(env.Data), "providerId") {
		t.Fatalf("providerId deveria ser omitido em OPENING: %s", env.Data)
	}
}

func TestOccurredAtIsNormalizedToUTC(t *testing.T) {
	loc := time.FixedZone("BRT", -3*60*60)
	env, err := NewWalletBalanceChanged(Meta{
		EventID:       "00000000-0000-7000-8000-0000000000a3",
		CorrelationID: "corr-1",
		OccurredAt:    time.Date(2026, 9, 8, 9, 0, 0, 0, loc),
	}, BalanceChangedParams{
		WalletID:      "w-1",
		TransactionID: "tx-1",
		Direction:     ledger.DirectionCredit,
		Money:         brl(t, "1.00"),
		BalanceBefore: brl(t, "0.00"),
		BalanceAfter:  brl(t, "1.00"),
		WalletVersion: 1,
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	if env.OccurredAt.Location() != time.UTC {
		t.Fatalf("occurredAt deveria estar em UTC, está em %v", env.OccurredAt.Location())
	}
	payload, err := env.Payload()
	if err != nil {
		t.Fatalf("Payload falhou: %v", err)
	}
	if !strings.Contains(string(payload), `"occurredAt":"2026-09-08T12:00:00.000Z"`) {
		t.Fatalf("occurredAt não serializado como esperado: %s", payload)
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	original, err := NewWalletBalanceChanged(meta(), BalanceChangedParams{
		WalletID:      "w-1",
		TransactionID: "tx-1",
		Direction:     ledger.DirectionDebit,
		Money:         brl(t, "25.00"),
		BalanceBefore: brl(t, "100.00"),
		BalanceAfter:  brl(t, "75.00"),
		WalletVersion: 2,
	})
	if err != nil {
		t.Fatalf("constructor falhou: %v", err)
	}
	payload, err := original.Payload()
	if err != nil {
		t.Fatalf("Payload falhou: %v", err)
	}

	var restored Envelope
	if err := json.Unmarshal(payload, &restored); err != nil {
		t.Fatalf("Unmarshal falhou: %v", err)
	}
	if err := restored.Validate(); err != nil {
		t.Fatalf("envelope restaurado inválido: %v", err)
	}
	if !restored.OccurredAt.Equal(original.OccurredAt) {
		t.Fatalf("occurredAt perdido: %v != %v", restored.OccurredAt, original.OccurredAt)
	}
	if restored.EventID != original.EventID || restored.EventType != original.EventType {
		t.Fatal("identidade do evento perdida no round-trip")
	}
	if string(restored.Data) != string(original.Data) {
		t.Fatalf("data divergente no round-trip: %s != %s", restored.Data, original.Data)
	}
}

func TestConstructorsRejectInvalidMeta(t *testing.T) {
	tests := []struct {
		name string
		meta Meta
	}{
		{"sem eventId", Meta{CorrelationID: "c", OccurredAt: fixedTime}},
		{"sem correlationId", Meta{EventID: "e", OccurredAt: fixedTime}},
		{"occurredAt zero", Meta{EventID: "e", CorrelationID: "c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewWalletBalanceChanged(tc.meta, BalanceChangedParams{
				WalletID:      "w-1",
				TransactionID: "tx-1",
				Direction:     ledger.DirectionCredit,
				Money:         brl(t, "1.00"),
				BalanceBefore: brl(t, "0.00"),
				BalanceAfter:  brl(t, "1.00"),
				WalletVersion: 1,
			})
			if err == nil {
				t.Fatal("deveria falhar com meta inválida")
			}
		})
	}
}

func TestConstructorsRejectMissingBusinessFields(t *testing.T) {
	tests := []struct {
		name   string
		build  func() (Envelope, error)
		target error
	}{
		{"processed sem transactionId", func() (Envelope, error) {
			return NewWagerTransactionProcessed(meta(), ProcessedParams{
				WalletID: "w-1", Kind: wagertransaction.KindBet, Status: wagertransaction.StatusProcessed,
				Money: brl(t, "1.00"), Balance: brl(t, "1.00"),
			})
		}, ErrMissingField},
		{"processed sem walletId", func() (Envelope, error) {
			return NewWagerTransactionProcessed(meta(), ProcessedParams{
				TransactionID: "tx-1", Kind: wagertransaction.KindBet, Status: wagertransaction.StatusProcessed,
				Money: brl(t, "1.00"), Balance: brl(t, "1.00"),
			})
		}, ErrMissingField},
		{"processed com money inválido", func() (Envelope, error) {
			return NewWagerTransactionProcessed(meta(), ProcessedParams{
				TransactionID: "tx-1", WalletID: "w-1", Kind: wagertransaction.KindBet,
				Status: wagertransaction.StatusProcessed, Balance: brl(t, "1.00"),
			})
		}, ErrInvalidMoney},
		{"rejected sem failureCode", func() (Envelope, error) {
			return NewWagerTransactionRejected(meta(), RejectedParams{
				TransactionID: "tx-1", WalletID: "w-1", Kind: wagertransaction.KindBet,
			})
		}, ErrMissingField},
		{"balanceChanged com direção inválida", func() (Envelope, error) {
			return NewWalletBalanceChanged(meta(), BalanceChangedParams{
				WalletID: "w-1", TransactionID: "tx-1", Direction: "SIDEWAYS",
				Money: brl(t, "1.00"), BalanceBefore: brl(t, "0.00"), BalanceAfter: brl(t, "1.00"), WalletVersion: 1,
			})
		}, nil},
		{"balanceChanged com walletVersion zero", func() (Envelope, error) {
			return NewWalletBalanceChanged(meta(), BalanceChangedParams{
				WalletID: "w-1", TransactionID: "tx-1", Direction: ledger.DirectionCredit,
				Money: brl(t, "1.00"), BalanceBefore: brl(t, "0.00"), BalanceAfter: brl(t, "1.00"), WalletVersion: 0,
			})
		}, nil},
		{"pending sem referência", func() (Envelope, error) {
			return NewWagerTransactionPendingReference(meta(), PendingReferenceParams{
				TransactionID: "tx-1", WalletID: "w-1", Kind: wagertransaction.KindRefund, Attempts: 1,
			})
		}, ErrMissingField},
		{"pending com attempts zero", func() (Envelope, error) {
			return NewWagerTransactionPendingReference(meta(), PendingReferenceParams{
				TransactionID: "tx-1", WalletID: "w-1", Kind: wagertransaction.KindRefund,
				ReferenceExternalTransactionID: "ext-1", Attempts: 0,
			})
		}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.build()
			if err == nil {
				t.Fatal("deveria falhar")
			}
			if tc.target != nil && !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestEnvelopeValidate(t *testing.T) {
	valid, err := NewWagerTransactionProcessed(meta(), ProcessedParams{
		TransactionID: "tx-1", WalletID: "w-1", Kind: wagertransaction.KindBet,
		Status: wagertransaction.StatusProcessed, Money: brl(t, "1.00"), Balance: brl(t, "1.00"),
	})
	if err != nil {
		t.Fatalf("setup falhou: %v", err)
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("envelope válido rejeitado: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Envelope)
		target error
	}{
		{"tipo desconhecido", func(e *Envelope) { e.EventType = "Foo" }, ErrUnknownType},
		{"sem aggregateId", func(e *Envelope) { e.AggregateID = "" }, ErrMissingField},
		{"versão zero", func(e *Envelope) { e.Version = 0 }, ErrInvalidVersion},
		{"data vazio", func(e *Envelope) { e.Data = nil }, ErrInvalidPayload},
		{"data inválido", func(e *Envelope) { e.Data = json.RawMessage(`{`) }, ErrInvalidPayload},
		{"sem eventId", func(e *Envelope) { e.EventID = "" }, ErrMissingField},
		{"occurredAt zero", func(e *Envelope) { e.OccurredAt = time.Time{} }, ErrInvalidTime},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := valid
			tc.mutate(&env)
			if err := env.Validate(); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestTypeIsKnown(t *testing.T) {
	for _, t2 := range []Type{
		TypeWagerTransactionProcessed,
		TypeWagerTransactionRejected,
		TypeWalletBalanceChanged,
		TypeWagerTransactionPendingReference,
	} {
		if !t2.IsKnown() {
			t.Fatalf("%s deveria ser conhecido", t2)
		}
	}
	if Type("Nope").IsKnown() {
		t.Fatal("tipo inventado não deveria ser conhecido")
	}
}
