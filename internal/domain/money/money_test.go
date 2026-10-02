package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseDecimalValid(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		want     int64
		wantText string
	}{
		{"zero", "0.00", 0, "0.00"},
		{"inteiro sem decimais", "25", 2500, "25.00"},
		{"uma casa decimal", "25.5", 2550, "25.50"},
		{"duas casas", "25.00", 2500, "25.00"},
		{"um centavo", "0.01", 1, "0.01"},
		{"negativo", "-1.50", -150, "-1.50"},
		{"negativo sub-unitario", "-0.50", -50, "-0.50"},
		{"negativo zero", "-0.00", 0, "0.00"},
		{"zeros a esquerda", "007.10", 710, "7.10"},
		{"milhar", "1000.00", 100000, "1000.00"},
		{"maximo int64", "92233720368547758.07", math.MaxInt64, "92233720368547758.07"},
		{"minimo int64", "-92233720368547758.08", math.MinInt64, "-92233720368547758.08"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseDecimal(tc.in, "BRL")
			if err != nil {
				t.Fatalf("ParseDecimal(%q) erro inesperado: %v", tc.in, err)
			}
			if m.Amount() != tc.want {
				t.Fatalf("Amount() = %d, quer %d", m.Amount(), tc.want)
			}
			if got := m.String(); got != tc.wantText {
				t.Fatalf("String() = %q, quer %q", got, tc.wantText)
			}
		})
	}
}

func TestParseDecimalInvalid(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		target error
	}{
		{"vazio", "", ErrEmptyAmount},
		{"somente espacos", "   ", ErrEmptyAmount},
		{"espaco a direita", "25.00 ", ErrInvalidAmount},
		{"espaco a esquerda", " 25.00", ErrInvalidAmount},
		{"cientifica minuscula", "1e3", ErrInvalidAmount},
		{"cientifica maiuscula", "1E3", ErrInvalidAmount},
		{"cientifica negativa", "-1e-3", ErrInvalidAmount},
		{"NaN", "NaN", ErrInvalidAmount},
		{"Infinity", "Infinity", ErrInvalidAmount},
		{"menos Infinity", "-Infinity", ErrInvalidAmount},
		{"escala excedente", "25.000", ErrScaleExceeded},
		{"ponto sem fracao", "25.", ErrInvalidAmount},
		{"sem parte inteira", ".50", ErrInvalidAmount},
		{"virgula decimal", "25,00", ErrInvalidAmount},
		{"dois pontos", "1.2.3", ErrInvalidAmount},
		{"sinal positivo", "+25.00", ErrInvalidAmount},
		{"sinal solto", "-", ErrInvalidAmount},
		{"letras", "abc", ErrInvalidAmount},
		{"hex", "0x10", ErrInvalidAmount},
		{"underscore", "1_000.00", ErrInvalidAmount},
		{"overflow max mais um", "92233720368547758.08", ErrOverflow},
		{"overflow inteiro grande", "99999999999999999999.99", ErrOverflow},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseDecimal(tc.in, "BRL")
			if err == nil {
				t.Fatalf("ParseDecimal(%q) deveria falhar", tc.in)
			}
			if !errors.Is(err, tc.target) {
				t.Fatalf("ParseDecimal(%q) erro = %v, quer %v", tc.in, err, tc.target)
			}
		})
	}
}

func TestParseCurrency(t *testing.T) {
	for _, valid := range []string{"BRL", "USD", "EUR"} {
		if _, err := ParseCurrency(valid); err != nil {
			t.Fatalf("ParseCurrency(%q) deveria ser válido: %v", valid, err)
		}
	}
	for _, invalid := range []string{"", "brl", "Brl", "BR", "BRLL", "B1L", "REAIS"} {
		if _, err := ParseCurrency(invalid); !errors.Is(err, ErrInvalidCurrency) {
			t.Fatalf("ParseCurrency(%q) deveria falhar com ErrInvalidCurrency, got %v", invalid, err)
		}
	}
}

func TestParseExternalAmountRejectsNegative(t *testing.T) {
	if _, err := ParseDecimal("-1.00", "BRL"); err != nil {
		t.Fatalf("ParseDecimal deveria aceitar negativos (uso interno): %v", err)
	}
	if _, err := ParseExternalAmount("-1.00", "BRL"); !errors.Is(err, ErrNegativeNotAllowed) {
		t.Fatalf("ParseExternalAmount(-1.00) deveria falhar, got %v", err)
	}
	if _, err := ParseExternalAmount("0.00", "BRL"); err != nil {
		t.Fatalf("ParseExternalAmount(0.00) deveria ser aceito: %v", err)
	}
}

func TestZero(t *testing.T) {
	z, err := Zero("BRL")
	if err != nil {
		t.Fatalf("Zero() erro: %v", err)
	}
	if !z.IsZero() || z.IsNegative() || z.IsPositive() {
		t.Fatalf("Zero() deveria ser zero: %s", z.String())
	}
	if _, err := Zero("brl"); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatalf("Zero com moeda inválida deveria falhar, got %v", err)
	}
}

func mustMoney(t *testing.T, s string) Money {
	t.Helper()
	m, err := ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("ParseDecimal(%q) falhou: %v", s, err)
	}
	return m
}

func TestArithmetic(t *testing.T) {
	sum, err := mustMoney(t, "10.25").Add(mustMoney(t, "5.75"))
	if err != nil || sum.String() != "16.00" {
		t.Fatalf("Add = %v / %v, quer 16.00", sum, err)
	}

	diff, err := mustMoney(t, "10.00").Sub(mustMoney(t, "25.50"))
	if err != nil || diff.String() != "-15.50" {
		t.Fatalf("Sub = %v / %v, quer -15.50", diff, err)
	}

	neg, err := mustMoney(t, "1.50").Neg()
	if err != nil || neg.String() != "-1.50" {
		t.Fatalf("Neg = %v / %v, quer -1.50", neg, err)
	}
}

func TestArithmeticOverflow(t *testing.T) {
	max := mustMoney(t, "92233720368547758.07")
	one := mustMoney(t, "0.01")
	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Add no limite deveria estourar, got %v", err)
	}

	min := mustMoney(t, "-92233720368547758.08")
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Sub no limite deveria estourar, got %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Neg no limite deveria estourar, got %v", err)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := mustMoney(t, "10.00")
	usd, err := ParseDecimal("10.00", "USD")
	if err != nil {
		t.Fatalf("setup falhou: %v", err)
	}
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Add entre moedas distintas deveria falhar, got %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("Cmp entre moedas distintas deveria falhar, got %v", err)
	}
	if brl.Equal(usd) {
		t.Fatal("Equal entre moedas distintas deveria ser false")
	}
}

func TestUninitializedRejected(t *testing.T) {
	var empty Money
	if _, err := empty.Add(empty); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Add em Money não inicializado deveria falhar, got %v", err)
	}
	if _, err := empty.Neg(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Neg em Money não inicializado deveria falhar, got %v", err)
	}
	if _, err := empty.MarshalJSON(); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("Marshal de Money não inicializado deveria falhar, got %v", err)
	}
}

func TestCmpAndEqual(t *testing.T) {
	ten := mustMoney(t, "10.00")
	cmp, err := ten.Cmp(mustMoney(t, "20.00"))
	if err != nil || cmp != -1 {
		t.Fatalf("Cmp = %d/%v, quer -1", cmp, err)
	}
	cmp, err = ten.Cmp(mustMoney(t, "10.00"))
	if err != nil || cmp != 0 {
		t.Fatalf("Cmp = %d/%v, quer 0", cmp, err)
	}
	cmp, err = ten.Cmp(mustMoney(t, "5.00"))
	if err != nil || cmp != 1 {
		t.Fatalf("Cmp = %d/%v, quer 1", cmp, err)
	}
	if !ten.Equal(mustMoney(t, "10.00")) {
		t.Fatal("Equal deveria ser true")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	m := mustMoney(t, "975.00")
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal falhou: %v", err)
	}
	if got, want := string(b), `{"amount":"975.00","currency":"BRL"}`; got != want {
		t.Fatalf("Marshal = %s, quer %s", got, want)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("Unmarshal falhou: %v", err)
	}
	if !back.Equal(m) {
		t.Fatalf("round-trip perdeu valor: %s", back.String())
	}
}

func TestUnmarshalRejectsNonStringAmount(t *testing.T) {
	var m Money
	if err := json.Unmarshal([]byte(`{"amount":25.00,"currency":"BRL"}`), &m); !errors.Is(err, ErrAmountMustBeString) {
		t.Fatalf("amount numérico deveria falhar, got %v", err)
	}
	if err := json.Unmarshal([]byte(`{"amount":25,"currency":"BRL"}`), &m); !errors.Is(err, ErrAmountMustBeString) {
		t.Fatalf("amount inteiro deveria falhar, got %v", err)
	}
}

func TestUnmarshalRejectsMissingFields(t *testing.T) {
	var m Money
	if err := json.Unmarshal([]byte(`{"currency":"BRL"}`), &m); !errors.Is(err, ErrEmptyAmount) {
		t.Fatalf("amount ausente deveria falhar, got %v", err)
	}
	if err := json.Unmarshal([]byte(`{"amount":"25.00"}`), &m); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatalf("currency ausente deveria falhar, got %v", err)
	}
}
