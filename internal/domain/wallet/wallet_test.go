package wallet

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

func newWallet(t *testing.T, initial string) Wallet {
	t.Helper()
	w, err := New(NewParams{ID: "w-1", PlayerID: "p-1", InitialBalance: brl(t, initial), Now: testNow()})
	if err != nil {
		t.Fatalf("New() falhou: %v", err)
	}
	return w
}

func TestNewValid(t *testing.T) {
	w := newWallet(t, "1000.00")
	if w.Version() != InitialVersion {
		t.Fatalf("versão inicial = %d, quer %d", w.Version(), InitialVersion)
	}
	if w.Currency() != "BRL" {
		t.Fatalf("moeda = %s, quer BRL", w.Currency())
	}
	if !w.CreatedAt().Equal(testNow()) || !w.UpdatedAt().Equal(testNow()) {
		t.Fatal("timestamps deveriam ser iguais a Now em UTC")
	}
}

func TestNewAllowsZeroInitialBalance(t *testing.T) {
	w := newWallet(t, "0.00")
	if !w.Balance().IsZero() {
		t.Fatalf("saldo inicial deveria ser zero, got %s", w.Balance().String())
	}
}

func TestNewRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		params NewParams
		target error
	}{
		{"id vazio", NewParams{ID: "  ", PlayerID: "p-1", InitialBalance: brl(t, "1.00"), Now: testNow()}, ErrInvalidID},
		{"player vazio", NewParams{ID: "w-1", PlayerID: "", InitialBalance: brl(t, "1.00"), Now: testNow()}, ErrInvalidPlayerID},
		{"saldo negativo", NewParams{ID: "w-1", PlayerID: "p-1", InitialBalance: brl(t, "-1.00"), Now: testNow()}, ErrNegativeInitialBalance},
		{"money nao inicializado", NewParams{ID: "w-1", PlayerID: "p-1", InitialBalance: money.Money{}, Now: testNow()}, money.ErrUninitialized},
		{"sem timestamp", NewParams{ID: "w-1", PlayerID: "p-1", InitialBalance: brl(t, "1.00")}, ErrInvalidTimestamp},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.params); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestDebitHappyPath(t *testing.T) {
	w := newWallet(t, "100.00")
	if err := w.Debit(brl(t, "25.00"), testNow()); err != nil {
		t.Fatalf("Debit falhou: %v", err)
	}
	if got := w.Balance().String(); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00", got)
	}
	if w.Version() != InitialVersion+1 {
		t.Fatalf("versão = %d, quer %d", w.Version(), InitialVersion+1)
	}
}

func TestDebitToExactlyZeroIsAllowed(t *testing.T) {
	w := newWallet(t, "100.00")
	if err := w.Debit(brl(t, "100.00"), testNow()); err != nil {
		t.Fatalf("Debit até zero deveria ser aceito: %v", err)
	}
	if !w.Balance().IsZero() {
		t.Fatalf("saldo deveria ser zero, got %s", w.Balance().String())
	}
}

func TestDebitInsufficientFundsKeepsState(t *testing.T) {
	w := newWallet(t, "100.00")
	err := w.Debit(brl(t, "100.01"), testNow())
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("erro = %v, quer ErrInsufficientFunds", err)
	}
	if got := w.Balance().String(); got != "100.00" {
		t.Fatalf("saldo não deveria mudar, got %s", got)
	}
	if w.Version() != InitialVersion {
		t.Fatalf("versão não deveria mudar, got %d", w.Version())
	}
}

func TestDebitRejectsInvalidAmount(t *testing.T) {
	w := newWallet(t, "100.00")
	if err := w.Debit(brl(t, "0.00"), testNow()); !errors.Is(err, ErrNonPositiveMovement) {
		t.Fatalf("débito zero: erro = %v, quer ErrNonPositiveMovement", err)
	}
	usd, err := money.ParseDecimal("1.00", "USD")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := w.Debit(usd, testNow()); !errors.Is(err, ErrMovementCurrencyMismatch) {
		t.Fatalf("moeda distinta: erro = %v, quer ErrMovementCurrencyMismatch", err)
	}
	if err := w.Debit(brl(t, "1.00"), time.Time{}); !errors.Is(err, ErrInvalidTimestamp) {
		t.Fatalf("timestamp zero: erro = %v, quer ErrInvalidTimestamp", err)
	}
}

func TestCreditOverflow(t *testing.T) {
	w := newWallet(t, "92233720368547758.07")
	if err := w.Credit(brl(t, "0.01"), testNow()); !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("crédito no limite deveria estourar, got %v", err)
	}
}

func TestSufficientFunds(t *testing.T) {
	w := newWallet(t, "100.00")
	ok, err := w.SufficientFunds(brl(t, "100.00"))
	if err != nil || !ok {
		t.Fatalf("SufficientFunds(100.00) = %v/%v, quer true", ok, err)
	}
	ok, err = w.SufficientFunds(brl(t, "100.01"))
	if err != nil || ok {
		t.Fatalf("SufficientFunds(100.01) = %v/%v, quer false", ok, err)
	}
}

func TestRehydratePreservesState(t *testing.T) {
	created := testNow().Add(-time.Hour)
	w, err := Rehydrate(RehydrateParams{
		ID: "w-1", PlayerID: "p-1", Currency: "BRL", Balance: brl(t, "42.50"),
		Version: 7, CreatedAt: created, UpdatedAt: testNow(),
	})
	if err != nil {
		t.Fatalf("Rehydrate falhou: %v", err)
	}
	if w.Version() != 7 {
		t.Fatalf("versão = %d, quer 7 (reidratação não incrementa)", w.Version())
	}
	if got := w.Balance().String(); got != "42.50" {
		t.Fatalf("saldo = %s, quer 42.50", got)
	}
	if !w.CreatedAt().Equal(created) {
		t.Fatal("CreatedAt não preservado")
	}
}

func TestRehydrateRejectsInvalidState(t *testing.T) {
	usd, err := money.ParseDecimal("1.00", "USD")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	tests := []struct {
		name   string
		params RehydrateParams
		target error
	}{
		{"versao zero", RehydrateParams{ID: "w-1", PlayerID: "p-1", Currency: "BRL", Balance: brl(t, "1.00"), Version: 0, CreatedAt: testNow(), UpdatedAt: testNow()}, ErrInvalidVersion},
		{"saldo negativo", RehydrateParams{ID: "w-1", PlayerID: "p-1", Currency: "BRL", Balance: brl(t, "-1.00"), Version: 1, CreatedAt: testNow(), UpdatedAt: testNow()}, ErrNegativeBalance},
		{"moeda do saldo divergente", RehydrateParams{ID: "w-1", PlayerID: "p-1", Currency: "BRL", Balance: usd, Version: 1, CreatedAt: testNow(), UpdatedAt: testNow()}, ErrBalanceCurrencyMismatch},
		{"sem timestamps", RehydrateParams{ID: "w-1", PlayerID: "p-1", Currency: "BRL", Balance: brl(t, "1.00"), Version: 1}, ErrInvalidTimestamp},
		{"moeda invalida", RehydrateParams{ID: "w-1", PlayerID: "p-1", Currency: "brl", Balance: brl(t, "1.00"), Version: 1, CreatedAt: testNow(), UpdatedAt: testNow()}, money.ErrInvalidCurrency},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Rehydrate(tc.params); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}
