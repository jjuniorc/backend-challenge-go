// Package ledger implementa o lançamento contábil da carteira.
//
// O lançamento é IMUTÁVEL: os campos são privados e não há setters. A construção
// valida balanceAfter = balanceBefore ± amount conforme a direção, garantindo
// que nenhum lançamento inconsistente entre no sistema.
//
// LOSS e operações rejeitadas não produzem lançamento — por isso o valor do
// lançamento precisa ser estritamente positivo.
//
// A imutabilidade é reforçada no banco (trigger que rejeita UPDATE/DELETE) e a
// unicidade de (walletId, transactionId) por constraint, conforme o README.
package ledger

import (
	"fmt"
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
)

// Direction é a direção do lançamento.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// Erros sentinela. Classificáveis com errors.Is.
var (
	ErrInvalidDirection     = domainerr.New(domainerr.KindInvalid, "LEDGER_INVALID_DIRECTION", "direção de lançamento inválida (use DEBIT ou CREDIT)")
	ErrInvalidID            = domainerr.New(domainerr.KindInvalid, "LEDGER_INVALID_ID", "identificador de lançamento inválido")
	ErrInvalidWalletID      = domainerr.New(domainerr.KindInvalid, "LEDGER_INVALID_WALLET_ID", "walletId inválido")
	ErrInvalidTransactionID = domainerr.New(domainerr.KindInvalid, "LEDGER_INVALID_TRANSACTION_ID", "transactionId inválido")
	ErrInvalidTimestamp     = domainerr.New(domainerr.KindInvalid, "LEDGER_INVALID_TIMESTAMP", "timestamp inválido")
	ErrNonPositiveAmount    = domainerr.New(domainerr.KindRuleViolation, "LEDGER_NON_POSITIVE_AMOUNT", "lançamento exige valor maior que zero")
	ErrCurrencyMismatch     = domainerr.New(domainerr.KindCurrencyMismatch, "LEDGER_CURRENCY_MISMATCH", "moedas incompatíveis entre valor e saldos")
	ErrBalanceMismatch      = domainerr.New(domainerr.KindInvalid, "LEDGER_BALANCE_MISMATCH", "balanceAfter inconsistente com balanceBefore e o valor")
)

// ParseDirection valida e normaliza a direção.
func ParseDirection(s string) (Direction, error) {
	switch Direction(strings.ToUpper(strings.TrimSpace(s))) {
	case DirectionDebit:
		return DirectionDebit, nil
	case DirectionCredit:
		return DirectionCredit, nil
	default:
		return "", domainerr.New(domainerr.KindInvalid, ErrInvalidDirection.Code(),
			fmt.Sprintf("direção desconhecida: %q", s))
	}
}

// Entry é o lançamento imutável.
type Entry struct {
	id            string
	walletID      string
	transactionID string
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// Params descreve a construção de um lançamento.
type Params struct {
	ID            string
	WalletID      string
	TransactionID string
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

// New constrói e valida um lançamento.
func New(p Params) (Entry, error) {
	return build(p)
}

// Rehydrate reconstrói um lançamento persistido.
//
// Um lançamento não tem máquina de estados nem eventos, então não há efeito
// colateral a suprimir: a validação é a mesma. A separação existe para deixar
// explícito o caminho de leitura em relação ao de criação.
func Rehydrate(p Params) (Entry, error) {
	return build(p)
}

func build(p Params) (Entry, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Entry{}, ErrInvalidID
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Entry{}, ErrInvalidWalletID
	}
	if strings.TrimSpace(p.TransactionID) == "" {
		return Entry{}, ErrInvalidTransactionID
	}
	if _, err := ParseDirection(string(p.Direction)); err != nil {
		return Entry{}, err
	}
	if p.CreatedAt.IsZero() {
		return Entry{}, ErrInvalidTimestamp
	}
	if !p.Amount.IsValid() || !p.BalanceBefore.IsValid() || !p.BalanceAfter.IsValid() {
		return Entry{}, money.ErrUninitialized
	}
	if p.Amount.Currency() != p.BalanceBefore.Currency() || p.Amount.Currency() != p.BalanceAfter.Currency() {
		return Entry{}, ErrCurrencyMismatch
	}
	if !p.Amount.IsPositive() {
		return Entry{}, ErrNonPositiveAmount
	}

	expected, err := apply(p.Direction, p.BalanceBefore, p.Amount)
	if err != nil {
		return Entry{}, err
	}
	if !expected.Equal(p.BalanceAfter) {
		return Entry{}, domainerr.New(domainerr.KindInvalid, ErrBalanceMismatch.Code(),
			fmt.Sprintf("balanceAfter inconsistente: esperado %s, recebido %s", expected.String(), p.BalanceAfter.String()))
	}

	return Entry{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		balanceBefore: p.BalanceBefore,
		balanceAfter:  p.BalanceAfter,
		createdAt:     p.CreatedAt.UTC(),
	}, nil
}

func apply(direction Direction, before, amount money.Money) (money.Money, error) {
	switch direction {
	case DirectionDebit:
		return before.Sub(amount)
	case DirectionCredit:
		return before.Add(amount)
	default:
		return money.Money{}, ErrInvalidDirection
	}
}

// IsValid informa se o lançamento foi inicializado.
func (e Entry) IsValid() bool { return e.id != "" && e.walletID != "" }

func (e Entry) ID() string                 { return e.id }
func (e Entry) WalletID() string           { return e.walletID }
func (e Entry) TransactionID() string      { return e.transactionID }
func (e Entry) Direction() Direction       { return e.direction }
func (e Entry) Amount() money.Money        { return e.amount }
func (e Entry) BalanceBefore() money.Money { return e.balanceBefore }
func (e Entry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e Entry) CreatedAt() time.Time       { return e.createdAt }
