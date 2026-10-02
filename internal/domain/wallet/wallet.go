// Package wallet implementa a raiz do agregado financeiro.
//
// A carteira encapsula identidade, jogador, moeda, saldo, versão e instantes de
// criação/atualização. Toda mudança de saldo passa por Debit/Credit, que
// preservam a invariante balance >= 0 e incrementam a versão APENAS quando há
// mudança efetiva de saldo.
//
// A coordenação entre processos (lock pessimista por linha, atualização
// condicionada ou controle otimista) é responsabilidade da camada de
// persistência (F2); este pacote garante as invariantes do agregado, e o banco
// reforça a de não negatividade com CHECK.
package wallet

import (
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
)

// InitialVersion é a versão de uma carteira recém-criada.
const InitialVersion int64 = 1

// Erros sentinela. Classificáveis com errors.Is.
var (
	ErrInvalidID                = domainerr.New(domainerr.KindInvalid, "WALLET_INVALID_ID", "identificador de carteira inválido")
	ErrInvalidPlayerID          = domainerr.New(domainerr.KindInvalid, "WALLET_INVALID_PLAYER_ID", "identificador de jogador inválido")
	ErrNegativeInitialBalance   = domainerr.New(domainerr.KindRuleViolation, "WALLET_NEGATIVE_INITIAL_BALANCE", "saldo inicial não pode ser negativo")
	ErrNegativeBalance          = domainerr.New(domainerr.KindRuleViolation, "WALLET_NEGATIVE_BALANCE", "saldo da carteira não pode ser negativo")
	ErrBalanceCurrencyMismatch  = domainerr.New(domainerr.KindCurrencyMismatch, "WALLET_BALANCE_CURRENCY_MISMATCH", "saldo não corresponde à moeda da carteira")
	ErrMovementCurrencyMismatch = domainerr.New(domainerr.KindCurrencyMismatch, "WALLET_MOVEMENT_CURRENCY_MISMATCH", "moeda da movimentação difere da moeda da carteira")
	ErrInvalidVersion           = domainerr.New(domainerr.KindInvalid, "WALLET_INVALID_VERSION", "versão da carteira inválida")
	ErrInvalidTimestamp         = domainerr.New(domainerr.KindInvalid, "WALLET_INVALID_TIMESTAMP", "timestamp inválido")
	ErrNonPositiveMovement      = domainerr.New(domainerr.KindRuleViolation, "WALLET_NON_POSITIVE_MOVEMENT", "movimentação exige valor maior que zero")
	// ErrInsufficientFunds é a rejeição de uma aposta/rollback sem saldo. O
	// código é estável e distinto de REVERSAL_INSUFFICIENT_FUNDS (F2), conforme
	// exigido para diferenciar falhas no contrato.
	ErrInsufficientFunds = domainerr.New(domainerr.KindInsufficientFunds, "WALLET_INSUFFICIENT_FUNDS", "saldo insuficiente")
)

// Wallet é o agregado financeiro.
type Wallet struct {
	id        string
	playerID  string
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// NewParams descreve a criação de uma carteira nova.
type NewParams struct {
	ID             string
	PlayerID       string
	InitialBalance money.Money
	Now            time.Time
}

// New cria uma carteira com saldo inicial maior ou igual a zero.
//
// Saldo inicial zero é aceito; nesse caso o caso de uso não cria OPENING, ledger
// nem eventos financeiros (F2).
func New(p NewParams) (Wallet, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Wallet{}, ErrInvalidID
	}
	if strings.TrimSpace(p.PlayerID) == "" {
		return Wallet{}, ErrInvalidPlayerID
	}
	if !p.InitialBalance.IsValid() {
		return Wallet{}, money.ErrUninitialized
	}
	if p.InitialBalance.IsNegative() {
		return Wallet{}, ErrNegativeInitialBalance
	}
	if p.Now.IsZero() {
		return Wallet{}, ErrInvalidTimestamp
	}
	now := p.Now.UTC()
	return Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  p.InitialBalance.Currency(),
		balance:   p.InitialBalance,
		version:   InitialVersion,
		createdAt: now,
		updatedAt: now,
	}, nil
}

// RehydrateParams descreve a reconstrução de uma carteira persistida.
type RehydrateParams struct {
	ID        string
	PlayerID  string
	Currency  money.Currency
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate reconstrói a carteira a partir do estado persistido.
//
// Não reaplica movimentações, não incrementa versão e não emite eventos.
func Rehydrate(p RehydrateParams) (Wallet, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Wallet{}, ErrInvalidID
	}
	if strings.TrimSpace(p.PlayerID) == "" {
		return Wallet{}, ErrInvalidPlayerID
	}
	currency, err := money.ParseCurrency(string(p.Currency))
	if err != nil {
		return Wallet{}, err
	}
	if !p.Balance.IsValid() {
		return Wallet{}, money.ErrUninitialized
	}
	if p.Balance.Currency() != currency {
		return Wallet{}, ErrBalanceCurrencyMismatch
	}
	if p.Balance.IsNegative() {
		return Wallet{}, ErrNegativeBalance
	}
	if p.Version < InitialVersion {
		return Wallet{}, ErrInvalidVersion
	}
	if p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
		return Wallet{}, ErrInvalidTimestamp
	}
	return Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  currency,
		balance:   p.Balance,
		version:   p.Version,
		createdAt: p.CreatedAt.UTC(),
		updatedAt: p.UpdatedAt.UTC(),
	}, nil
}

// Debit debita amount, preservando balance >= 0 e incrementando a versão.
func (w *Wallet) Debit(amount money.Money, now time.Time) error {
	if err := w.guardMovement(amount, now); err != nil {
		return err
	}
	next, err := w.balance.Sub(amount)
	if err != nil {
		return err
	}
	if next.IsNegative() {
		return ErrInsufficientFunds
	}
	w.balance = next
	w.touch(now)
	return nil
}

// Credit credita amount, rejeitando overflow e incrementando a versão.
func (w *Wallet) Credit(amount money.Money, now time.Time) error {
	if err := w.guardMovement(amount, now); err != nil {
		return err
	}
	next, err := w.balance.Add(amount)
	if err != nil {
		return err
	}
	w.balance = next
	w.touch(now)
	return nil
}

// SufficientFunds informa se a carteira comporta um débito de amount.
func (w Wallet) SufficientFunds(amount money.Money) (bool, error) {
	if err := w.guardCurrency(amount); err != nil {
		return false, err
	}
	cmp, err := w.balance.Cmp(amount)
	if err != nil {
		return false, err
	}
	return cmp >= 0, nil
}

// IsValid informa se a carteira foi inicializada.
func (w Wallet) IsValid() bool { return w.id != "" && w.currency != "" }

func (w Wallet) ID() string               { return w.id }
func (w Wallet) PlayerID() string         { return w.playerID }
func (w Wallet) Currency() money.Currency { return w.currency }
func (w Wallet) Balance() money.Money     { return w.balance }
func (w Wallet) Version() int64           { return w.version }
func (w Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w Wallet) UpdatedAt() time.Time     { return w.updatedAt }

func (w Wallet) guardMovement(amount money.Money, now time.Time) error {
	if err := w.guardCurrency(amount); err != nil {
		return err
	}
	if !amount.IsPositive() {
		return ErrNonPositiveMovement
	}
	if now.IsZero() {
		return ErrInvalidTimestamp
	}
	return nil
}

func (w Wallet) guardCurrency(amount money.Money) error {
	if !w.IsValid() {
		return ErrInvalidID
	}
	if !amount.IsValid() {
		return money.ErrUninitialized
	}
	if amount.Currency() != w.currency {
		return ErrMovementCurrencyMismatch
	}
	return nil
}

func (w *Wallet) touch(now time.Time) {
	w.version++
	w.updatedAt = now.UTC()
}
