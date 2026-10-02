package ledger

import (
	"errors"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
)

func testNow() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("setup money(%q): %v", s, err)
	}
	return m
}

func baseParams(t *testing.T) Params {
	t.Helper()
	return Params{
		ID:            "le-1",
		WalletID:      "w-1",
		TransactionID: "tx-1",
		Direction:     DirectionDebit,
		Amount:        brl(t, "25.00"),
		BalanceBefore: brl(t, "100.00"),
		BalanceAfter:  brl(t, "75.00"),
		CreatedAt:     testNow(),
	}
}

func TestNewDebit(t *testing.T) {
	e, err := New(baseParams(t))
	if err != nil {
		t.Fatalf("New falhou: %v", err)
	}
	if e.Direction() != DirectionDebit {
		t.Fatalf("direção = %s, quer DEBIT", e.Direction())
	}
	if got := e.BalanceAfter().String(); got != "75.00" {
		t.Fatalf("balanceAfter = %s, quer 75.00", got)
	}
	if !e.IsValid() {
		t.Fatal("lançamento deveria ser válido")
	}
}

func TestNewCredit(t *testing.T) {
	p := baseParams(t)
	p.Direction = DirectionCredit
	p.BalanceBefore = brl(t, "75.00")
	p.BalanceAfter = brl(t, "100.00")
	e, err := New(p)
	if err != nil {
		t.Fatalf("New falhou: %v", err)
	}
	if got := e.BalanceAfter().String(); got != "100.00" {
		t.Fatalf("balanceAfter = %s, quer 100.00", got)
	}
}

func TestRehydrateMatchesNew(t *testing.T) {
	p := baseParams(t)
	created, err := New(p)
	if err != nil {
		t.Fatalf("New falhou: %v", err)
	}
	restored, err := Rehydrate(p)
	if err != nil {
		t.Fatalf("Rehydrate falhou: %v", err)
	}
	if !created.BalanceAfter().Equal(restored.BalanceAfter()) {
		t.Fatal("reidratação deveria preservar o estado")
	}
}

func TestNewValidations(t *testing.T) {
	usd, err := money.ParseDecimal("25.00", "USD")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Params)
		target error
	}{
		{"id vazio", func(p *Params) { p.ID = " " }, ErrInvalidID},
		{"wallet vazio", func(p *Params) { p.WalletID = "" }, ErrInvalidWalletID},
		{"transaction vazio", func(p *Params) { p.TransactionID = "" }, ErrInvalidTransactionID},
		{"direcao invalida", func(p *Params) { p.Direction = "SIDEWAYS" }, ErrInvalidDirection},
		{"sem timestamp", func(p *Params) { p.CreatedAt = time.Time{} }, ErrInvalidTimestamp},
		{"valor zero", func(p *Params) { p.Amount = brl(t, "0.00") }, ErrNonPositiveAmount},
		{"valor negativo", func(p *Params) { p.Amount = brl(t, "-1.00") }, ErrNonPositiveAmount},
		{"moeda divergente", func(p *Params) { p.Amount = usd }, ErrCurrencyMismatch},
		{"saldo anterior nao inicializado", func(p *Params) { p.BalanceBefore = money.Money{} }, money.ErrUninitialized},
		{"balanceAfter inconsistente", func(p *Params) { p.BalanceAfter = brl(t, "74.99") }, ErrBalanceMismatch},
		{"debito que geraria saldo errado", func(p *Params) { p.BalanceAfter = brl(t, "100.00") }, ErrBalanceMismatch},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := baseParams(t)
			tc.mutate(&p)
			if _, err := New(p); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestCreditInconsistencyRejected(t *testing.T) {
	p := baseParams(t)
	p.Direction = DirectionCredit
	p.BalanceBefore = brl(t, "75.00")
	p.BalanceAfter = brl(t, "75.00")
	if _, err := New(p); !errors.Is(err, ErrBalanceMismatch) {
		t.Fatalf("erro = %v, quer ErrBalanceMismatch", err)
	}
}

func TestParseDirection(t *testing.T) {
	if d, err := ParseDirection(" debit "); err != nil || d != DirectionDebit {
		t.Fatalf("ParseDirection = %v/%v", d, err)
	}
	if d, err := ParseDirection("CREDIT"); err != nil || d != DirectionCredit {
		t.Fatalf("ParseDirection = %v/%v", d, err)
	}
	if _, err := ParseDirection("BOTH"); !errors.Is(err, ErrInvalidDirection) {
		t.Fatalf("ParseDirection inválido = %v", err)
	}
}
