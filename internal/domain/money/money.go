// Package money implementa o value object monetário.
//
// Representação: int64 em unidades mínimas (centavos), com escala fixa de 2
// casas e código de moeda ISO 4217. Nenhum caminho deste pacote usa float32 ou
// float64 — parsing, aritmética, comparação e serialização são inteiramente
// inteiros.
//
// Limites: |valor| <= 92233720368547758.07 (int64 em centavos). Overflow é
// sempre rejeitado com ErrOverflow, nunca truncado.
//
// Normalização documentada: formas equivalentes ("25", "25.5", "025.00") são
// aceitas e normalizadas para a forma canônica de 2 casas ("25.00"). O hash de
// idempotência (F2) usa SEMPRE a forma canônica devolvida por String().
package money

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
)

// Scale é a escala fixa (número de casas decimais) do contrato externo.
const Scale = 2

const minInt64 = int64(math.MinInt64)

// Erros sentinela. Classificáveis com errors.Is.
var (
	ErrUninitialized      = domainerr.New(domainerr.KindInvalid, "MONEY_UNINITIALIZED", "Money não inicializado (moeda ausente)")
	ErrEmptyAmount        = domainerr.New(domainerr.KindInvalid, "MONEY_EMPTY_AMOUNT", "valor monetário vazio")
	ErrInvalidAmount      = domainerr.New(domainerr.KindInvalid, "MONEY_INVALID_AMOUNT", "valor monetário inválido")
	ErrScaleExceeded      = domainerr.New(domainerr.KindInvalid, "MONEY_SCALE_EXCEEDED", "escala excedente: máximo de 2 casas decimais")
	ErrOverflow           = domainerr.New(domainerr.KindInvalid, "MONEY_OVERFLOW", "estouro de int64 em valor monetário")
	ErrInvalidCurrency    = domainerr.New(domainerr.KindInvalid, "MONEY_INVALID_CURRENCY", "código de moeda inválido (ISO 4217: 3 letras maiúsculas)")
	ErrCurrencyMismatch   = domainerr.New(domainerr.KindCurrencyMismatch, "MONEY_CURRENCY_MISMATCH", "moedas incompatíveis")
	ErrNegativeNotAllowed = domainerr.New(domainerr.KindInvalid, "MONEY_NEGATIVE_NOT_ALLOWED", "valor negativo não é aceito em entradas financeiras externas")
	ErrAmountMustBeString = domainerr.New(domainerr.KindInvalid, "MONEY_AMOUNT_MUST_BE_STRING", `campo "amount" deve ser string decimal (ex.: "25.00"), nunca número`)
	ErrInvalidJSON        = domainerr.New(domainerr.KindInvalid, "MONEY_INVALID_JSON", "Money inválido no JSON")
)

// Currency é um código ISO 4217.
//
// Limitação documentada: validamos a forma (3 letras A-Z), não a existência do
// código na tabela ISO 4217.
type Currency string

// ParseCurrency valida o formato do código de moeda.
func ParseCurrency(s string) (Currency, error) {
	if len(s) != 3 {
		return "", ErrInvalidCurrency
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 'A' || s[i] > 'Z' {
			return "", ErrInvalidCurrency
		}
	}
	return Currency(s), nil
}

// Money é um value object imutável: valor em unidades mínimas + moeda.
type Money struct {
	amount   int64
	currency Currency
}

// NewMoney cria Money a partir de unidades mínimas (centavos).
func NewMoney(amount int64, currency Currency) (Money, error) {
	if _, err := ParseCurrency(string(currency)); err != nil {
		return Money{}, err
	}
	return Money{amount: amount, currency: currency}, nil
}

// Zero devolve o zero da moeda informada.
func Zero(currency Currency) (Money, error) { return NewMoney(0, currency) }

// ParseDecimal interpreta uma string decimal com escala máxima de 2.
//
// Aceita sinal negativo, necessário para diferenças internas (por exemplo o
// campo "difference" da reconciliação). Entradas financeiras externas devem
// usar ParseExternalAmount, que rejeita negativos.
func ParseDecimal(s string, currency Currency) (Money, error) {
	amount, err := parseCents(s)
	if err != nil {
		return Money{}, err
	}
	return NewMoney(amount, currency)
}

// ParseExternalAmount é o parser do contrato externo (HTTP/SQS): mesma
// gramática de ParseDecimal, porém rejeita valores negativos.
func ParseExternalAmount(s string, currency Currency) (Money, error) {
	m, err := ParseDecimal(s, currency)
	if err != nil {
		return Money{}, err
	}
	if m.IsNegative() {
		return Money{}, ErrNegativeNotAllowed
	}
	return m, nil
}

// Amount devolve o valor em unidades mínimas.
func (m Money) Amount() int64 { return m.amount }

// Currency devolve a moeda.
func (m Money) Currency() Currency { return m.currency }

// IsValid informa se o Money foi inicializado.
func (m Money) IsValid() bool { return m.currency != "" }

// IsZero informa se o valor é exatamente zero.
func (m Money) IsZero() bool { return m.amount == 0 }

// IsNegative informa se o valor é menor que zero.
func (m Money) IsNegative() bool { return m.amount < 0 }

// IsPositive informa se o valor é maior que zero.
func (m Money) IsPositive() bool { return m.amount > 0 }

// Equal compara valor e moeda.
func (m Money) Equal(other Money) bool {
	return m.currency == other.currency && m.amount == other.amount
}

// Add soma dois valores da mesma moeda, rejeitando overflow.
func (m Money) Add(other Money) (Money, error) {
	if err := m.requireSameCurrency(other); err != nil {
		return Money{}, err
	}
	sum, overflow := addOverflow(m.amount, other.amount)
	if overflow {
		return Money{}, ErrOverflow
	}
	return Money{amount: sum, currency: m.currency}, nil
}

// Sub subtrai dois valores da mesma moeda, rejeitando overflow.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.requireSameCurrency(other); err != nil {
		return Money{}, err
	}
	diff, overflow := subOverflow(m.amount, other.amount)
	if overflow {
		return Money{}, ErrOverflow
	}
	return Money{amount: diff, currency: m.currency}, nil
}

// Neg devolve o valor com sinal invertido, rejeitando overflow.
func (m Money) Neg() (Money, error) {
	if err := m.requireValid(); err != nil {
		return Money{}, err
	}
	if m.amount == minInt64 {
		return Money{}, ErrOverflow
	}
	return Money{amount: -m.amount, currency: m.currency}, nil
}

// Cmp compara dois valores da mesma moeda: -1, 0 ou 1.
func (m Money) Cmp(other Money) (int, error) {
	if err := m.requireSameCurrency(other); err != nil {
		return 0, err
	}
	switch {
	case m.amount < other.amount:
		return -1, nil
	case m.amount > other.amount:
		return 1, nil
	default:
		return 0, nil
	}
}

// String devolve a forma canônica com 2 casas decimais.
func (m Money) String() string {
	whole := m.amount / 100
	frac := m.amount % 100
	if frac < 0 {
		frac = -frac
	}
	if m.amount < 0 && whole == 0 {
		return fmt.Sprintf("-0.%02d", frac)
	}
	return fmt.Sprintf("%d.%02d", whole, frac)
}

// MarshalJSON serializa como {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.requireValid(); err != nil {
		return nil, err
	}
	return json.Marshal(wire{Amount: m.String(), Currency: m.currency})
}

// UnmarshalJSON aceita apenas amount como STRING decimal. Números (inclusive
// 25.0) são rejeitados, impedindo que um float entre no domínio.
func (m *Money) UnmarshalJSON(data []byte) error {
	if m == nil {
		return ErrUninitialized
	}
	var raw struct {
		Amount   json.RawMessage `json:"amount"`
		Currency Currency        `json:"currency"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return domainerr.Wrap(domainerr.KindInvalid, ErrInvalidJSON.Code(), ErrInvalidJSON.Error(), err)
	}
	if len(raw.Amount) == 0 {
		return ErrEmptyAmount
	}
	var amountStr string
	if err := json.Unmarshal(raw.Amount, &amountStr); err != nil {
		return domainerr.Wrap(domainerr.KindInvalid, ErrAmountMustBeString.Code(), ErrAmountMustBeString.Error(), err)
	}
	parsed, err := ParseDecimal(amountStr, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}

type wire struct {
	Amount   string   `json:"amount"`
	Currency Currency `json:"currency"`
}

func (m Money) requireValid() error {
	if !m.IsValid() {
		return ErrUninitialized
	}
	return nil
}

func (m Money) requireSameCurrency(other Money) error {
	if err := m.requireValid(); err != nil {
		return err
	}
	if err := other.requireValid(); err != nil {
		return err
	}
	if m.currency != other.currency {
		return domainerr.New(domainerr.KindCurrencyMismatch, ErrCurrencyMismatch.Code(),
			fmt.Sprintf("moedas incompatíveis: %s e %s", m.currency, other.currency))
	}
	return nil
}

func invalid(msg string) *domainerr.Error {
	return domainerr.New(domainerr.KindInvalid, ErrInvalidAmount.Code(), msg)
}

// parseCents converte a string decimal em centavos, sem arredondar.
func parseCents(s string) (int64, error) {
	if s == "" || strings.TrimSpace(s) == "" {
		return 0, ErrEmptyAmount
	}
	if s != strings.TrimSpace(s) {
		return 0, invalid("valor monetário não pode conter espaços em branco")
	}

	body := s
	negative := false
	switch body[0] {
	case '-':
		negative = true
		body = body[1:]
	case '+':
		return 0, invalid(`sinal "+" não é aceito`)
	}
	if body == "" {
		return 0, ErrInvalidAmount
	}

	intPart, fracPart := body, ""
	if i := strings.IndexByte(body, '.'); i >= 0 {
		intPart, fracPart = body[:i], body[i+1:]
		if fracPart == "" {
			return 0, invalid("parte fracionária ausente após o separador decimal")
		}
		if strings.ContainsRune(fracPart, '.') {
			return 0, invalid("mais de um separador decimal")
		}
	}
	if intPart == "" {
		return 0, invalid("parte inteira ausente")
	}
	if len(fracPart) > Scale {
		return 0, ErrScaleExceeded
	}
	for len(fracPart) < Scale {
		fracPart += "0"
	}

	whole, err := parseUint(intPart)
	if err != nil {
		return 0, err
	}
	scaled, overflow := mulUint(whole, 100)
	if overflow {
		return 0, ErrOverflow
	}
	cents, err := parseUint(fracPart)
	if err != nil {
		return 0, err
	}
	magnitude, overflow := addUint(scaled, cents)
	if overflow {
		return 0, ErrOverflow
	}

	if negative {
		if magnitude == uint64(math.MaxInt64)+1 {
			return minInt64, nil
		}
		if magnitude > uint64(math.MaxInt64)+1 {
			return 0, ErrOverflow
		}
		return -int64(magnitude), nil
	}
	if magnitude > uint64(math.MaxInt64) {
		return 0, ErrOverflow
	}
	return int64(magnitude), nil
}

func parseUint(digits string) (uint64, error) {
	var n uint64
	for i := 0; i < len(digits); i++ {
		c := digits[i]
		if c < '0' || c > '9' {
			return 0, invalid("caractere inválido em valor monetário: " + strconv.QuoteRune(rune(c)))
		}
		d := uint64(c - '0')
		if n > (math.MaxUint64-d)/10 {
			return 0, ErrOverflow
		}
		n = n*10 + d
	}
	return n, nil
}

func addUint(a, b uint64) (uint64, bool) {
	s := a + b
	if s < a {
		return 0, true
	}
	return s, false
}

func mulUint(a, b uint64) (uint64, bool) {
	if a == 0 || b == 0 {
		return 0, false
	}
	p := a * b
	if p/b != a {
		return 0, true
	}
	return p, false
}

func addOverflow(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, true
	}
	return s, false
}

func subOverflow(a, b int64) (int64, bool) {
	if b == minInt64 {
		if a >= 0 {
			return 0, true
		}
		return a - b, false
	}
	return addOverflow(a, -b)
}
