// Package wagertransaction modela as operações financeiras dos provedores.
//
// Separação explícita:
//   - NewExternal: operação enviada por HTTP/SQS (BET, WIN, LOSS, REFUND, ROLLBACK);
//   - NewOpening:  crédito inicial de carteira, interno (OPENING);
//   - Rehydrate:   reconstrução do estado persistido, sem transições nem eventos.
//
// A máquina de estados é validada aqui; a persistência (F2) usa o status para
// decidir retomada durável e replay.
package wagertransaction

import (
	"fmt"
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
)

// Kind é o tipo da operação.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// Origin distingue a origem interna (abertura de carteira) da externa.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// Status é o estado da transação.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// Movement descreve a natureza do movimento de saldo da operação.
type Movement string

const (
	MovementNone     Movement = "NONE"
	MovementDebit    Movement = "DEBIT"
	MovementCredit   Movement = "CREDIT"
	MovementOpposite Movement = "OPPOSITE"
)

// Erros sentinela. Classificáveis com errors.Is.
var (
	ErrInvalidKind              = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_KIND", "tipo de operação inválido")
	ErrInvalidStatus            = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_STATUS", "estado de transação inválido")
	ErrInvalidOrigin            = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_ORIGIN", "origem de transação inválida")
	ErrInvalidID                = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_ID", "identificador de transação inválido")
	ErrInvalidProviderID        = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_PROVIDER_ID", "providerId inválido")
	ErrInvalidExternalID        = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_EXTERNAL_ID", "externalTransactionId inválido")
	ErrInvalidIdempotencyKey    = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_IDEMPOTENCY_KEY", "idempotencyKey inválida")
	ErrInvalidPayloadHash       = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_PAYLOAD_HASH", "payloadHash inválido")
	ErrInvalidWalletID          = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_WALLET_ID", "walletId inválido")
	ErrInvalidPlayerID          = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_PLAYER_ID", "playerId inválido")
	ErrInvalidRoundID           = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_ROUND_ID", "roundId inválido")
	ErrInvalidGameID            = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_GAME_ID", "gameId inválido")
	ErrInvalidReferenceID       = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_REFERENCE_ID", "referência interna inválida")
	ErrInvalidFailureCode       = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_FAILURE_CODE", "failureCode inválido")
	ErrInvalidTimestamp         = domainerr.New(domainerr.KindInvalid, "WAGER_INVALID_TIMESTAMP", "timestamp inválido")
	ErrOpeningNotExternal       = domainerr.New(domainerr.KindRuleViolation, "WAGER_OPENING_NOT_EXTERNAL", "OPENING é reservado à abertura interna de carteira")
	ErrOpeningNonPositiveAmount = domainerr.New(domainerr.KindRuleViolation, "WAGER_OPENING_NON_POSITIVE_AMOUNT", "abertura de carteira exige valor maior que zero")
	ErrAmountMustBePositive     = domainerr.New(domainerr.KindRuleViolation, "WAGER_AMOUNT_MUST_BE_POSITIVE", "operação exige valor maior que zero")
	ErrLossRequiresZeroAmount   = domainerr.New(domainerr.KindRuleViolation, "WAGER_LOSS_REQUIRES_ZERO_AMOUNT", `LOSS exige money.amount igual a "0.00"`)
	ErrReferenceRequired        = domainerr.New(domainerr.KindRuleViolation, "WAGER_REFERENCE_REQUIRED", "operação exige referenceExternalTransactionId")
	ErrReferenceNotAllowed      = domainerr.New(domainerr.KindRuleViolation, "WAGER_REFERENCE_NOT_ALLOWED", "operação não aceita referenceExternalTransactionId")
	ErrReferenceSelf            = domainerr.New(domainerr.KindRuleViolation, "WAGER_REFERENCE_SELF", "referência não pode apontar para a própria transação")
	ErrReferenceNotApplicable   = domainerr.New(domainerr.KindRuleViolation, "WAGER_REFERENCE_NOT_APPLICABLE", "operação não depende de referência")
	ErrTerminalState            = domainerr.New(domainerr.KindInvalidState, "WAGER_TERMINAL_STATE", "transação em estado terminal não aceita novas transições")
	ErrInvalidTransition        = domainerr.New(domainerr.KindInvalidState, "WAGER_INVALID_TRANSITION", "transição de estado inválida")
	ErrResultCurrencyMismatch   = domainerr.New(domainerr.KindCurrencyMismatch, "WAGER_RESULT_CURRENCY_MISMATCH", "saldo resultante difere da moeda da operação")
	ErrRollbackUnsupportedRef   = domainerr.New(domainerr.KindRuleViolation, "WAGER_ROLLBACK_UNSUPPORTED_REFERENCE", "ROLLBACK não se aplica a esta referência")
)

// ParseKind valida e normaliza o tipo.
func ParseKind(s string) (Kind, error) {
	switch Kind(strings.ToUpper(strings.TrimSpace(s))) {
	case KindOpening:
		return KindOpening, nil
	case KindBet:
		return KindBet, nil
	case KindWin:
		return KindWin, nil
	case KindLoss:
		return KindLoss, nil
	case KindRefund:
		return KindRefund, nil
	case KindRollback:
		return KindRollback, nil
	default:
		return "", domainerr.New(domainerr.KindInvalid, ErrInvalidKind.Code(),
			fmt.Sprintf("tipo de operação desconhecido: %q", s))
	}
}

// ParseStatus valida e normaliza o estado.
func ParseStatus(s string) (Status, error) {
	switch Status(strings.ToUpper(strings.TrimSpace(s))) {
	case StatusPending:
		return StatusPending, nil
	case StatusPendingReference:
		return StatusPendingReference, nil
	case StatusProcessed:
		return StatusProcessed, nil
	case StatusRejected:
		return StatusRejected, nil
	case StatusFailed:
		return StatusFailed, nil
	default:
		return "", domainerr.New(domainerr.KindInvalid, ErrInvalidStatus.Code(),
			fmt.Sprintf("estado desconhecido: %q", s))
	}
}

// ParseOrigin valida e normaliza a origem.
func ParseOrigin(s string) (Origin, error) {
	switch Origin(strings.ToUpper(strings.TrimSpace(s))) {
	case OriginInternal:
		return OriginInternal, nil
	case OriginExternal:
		return OriginExternal, nil
	default:
		return "", domainerr.New(domainerr.KindInvalid, ErrInvalidOrigin.Code(),
			fmt.Sprintf("origem desconhecida: %q", s))
	}
}

// IsExternal informa se o tipo é enviado por HTTP/SQS.
func (k Kind) IsExternal() bool { return k != KindOpening }

// IsReversal informa se o tipo reverte outra operação.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// RequiresReference informa se a referência externa é obrigatória.
func (k Kind) RequiresReference() bool { return k.IsReversal() }

// AllowsReference informa se a referência externa é permitida (opcional em WIN).
func (k Kind) AllowsReference() bool { return k.IsReversal() || k == KindWin }

// RequiresPositiveAmount informa se o valor precisa ser maior que zero.
func (k Kind) RequiresPositiveAmount() bool {
	switch k {
	case KindBet, KindWin, KindRefund, KindRollback:
		return true
	default:
		return false
	}
}

// RequiresZeroAmount informa se o valor precisa ser exatamente "0.00" (LOSS).
func (k Kind) RequiresZeroAmount() bool { return k == KindLoss }

// Movement devolve a natureza do movimento de saldo do tipo.
//
// ROLLBACK devolve MovementOpposite: o movimento concreto depende do tipo
// referenciado e é resolvido por OppositeOf.
func (k Kind) Movement() Movement {
	switch k {
	case KindOpening, KindWin, KindRefund:
		return MovementCredit
	case KindBet:
		return MovementDebit
	case KindLoss:
		return MovementNone
	case KindRollback:
		return MovementOpposite
	default:
		return MovementNone
	}
}

// OppositeOf devolve o movimento contrário ao da operação referenciada.
//
// Apenas BET, WIN e REFUND podem ser revertidas por ROLLBACK.
func OppositeOf(referenced Kind) (Movement, error) {
	switch referenced {
	case KindBet:
		return MovementCredit, nil
	case KindWin, KindRefund:
		return MovementDebit, nil
	default:
		return "", domainerr.New(domainerr.KindRuleViolation, ErrRollbackUnsupportedRef.Code(),
			fmt.Sprintf("ROLLBACK não se aplica a %s", referenced))
	}
}

// IsTerminal informa se o estado não aceita novas transições.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Transaction é a operação financeira.
type Transaction struct {
	id     string
	origin Origin
	kind   Kind
	status Status

	// Identificadores externos (vazios em OPENING).
	providerID            string
	externalTransactionID string
	idempotencyKey        string
	payloadHash           string
	roundID               string
	gameID                string

	// Comuns.
	walletID string
	playerID string
	amount   money.Money

	// Referências e resultado.
	externalReferenceID    string
	referenceTransactionID string
	failureCode            string
	failureMessage         string
	resultBalance          money.Money

	occurredAt time.Time
	createdAt  time.Time
	updatedAt  time.Time
}

// ExternalParams descreve uma operação enviada por HTTP/SQS.
type ExternalParams struct {
	ID                    string
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PayloadHash           string
	WalletID              string
	PlayerID              string
	RoundID               string
	GameID                string
	Kind                  Kind
	Money                 money.Money
	ReferenceExternalID   string
	Now                   time.Time
}

// NewExternal cria uma operação externa no estado PENDING.
func NewExternal(p ExternalParams) (Transaction, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Transaction{}, ErrInvalidID
	}
	if !isKnownKind(p.Kind) {
		return Transaction{}, ErrInvalidKind
	}
	if p.Kind == KindOpening {
		return Transaction{}, ErrOpeningNotExternal
	}
	if strings.TrimSpace(p.ProviderID) == "" {
		return Transaction{}, ErrInvalidProviderID
	}
	if strings.TrimSpace(p.ExternalTransactionID) == "" {
		return Transaction{}, ErrInvalidExternalID
	}
	if strings.TrimSpace(p.IdempotencyKey) == "" {
		return Transaction{}, ErrInvalidIdempotencyKey
	}
	if strings.TrimSpace(p.PayloadHash) == "" {
		return Transaction{}, ErrInvalidPayloadHash
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Transaction{}, ErrInvalidWalletID
	}
	if strings.TrimSpace(p.PlayerID) == "" {
		return Transaction{}, ErrInvalidPlayerID
	}
	if strings.TrimSpace(p.RoundID) == "" {
		return Transaction{}, ErrInvalidRoundID
	}
	if strings.TrimSpace(p.GameID) == "" {
		return Transaction{}, ErrInvalidGameID
	}
	if p.Now.IsZero() {
		return Transaction{}, ErrInvalidTimestamp
	}
	if !p.Money.IsValid() {
		return Transaction{}, money.ErrUninitialized
	}
	if err := validateAmountForKind(p.Kind, p.Money); err != nil {
		return Transaction{}, err
	}
	if err := validateReferenceForKind(p.Kind, p.ReferenceExternalID); err != nil {
		return Transaction{}, err
	}
	if ref := strings.TrimSpace(p.ReferenceExternalID); ref != "" && ref == strings.TrimSpace(p.ExternalTransactionID) {
		return Transaction{}, ErrReferenceSelf
	}

	now := p.Now.UTC()
	return Transaction{
		id:                    p.ID,
		origin:                OriginExternal,
		kind:                  p.Kind,
		status:                StatusPending,
		providerID:            p.ProviderID,
		externalTransactionID: p.ExternalTransactionID,
		idempotencyKey:        p.IdempotencyKey,
		payloadHash:           p.PayloadHash,
		walletID:              p.WalletID,
		playerID:              p.PlayerID,
		roundID:               p.RoundID,
		gameID:                p.GameID,
		amount:                p.Money,
		externalReferenceID:   p.ReferenceExternalID,
		occurredAt:            now,
		createdAt:             now,
		updatedAt:             now,
	}, nil
}

// OpeningParams descreve o crédito inicial interno.
type OpeningParams struct {
	ID       string
	WalletID string
	PlayerID string
	Money    money.Money
	Now      time.Time
}

// NewOpening cria a abertura interna já em PROCESSED.
//
// O valor precisa ser maior que zero: saldo inicial zero não gera OPENING.
func NewOpening(p OpeningParams) (Transaction, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Transaction{}, ErrInvalidID
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Transaction{}, ErrInvalidWalletID
	}
	if strings.TrimSpace(p.PlayerID) == "" {
		return Transaction{}, ErrInvalidPlayerID
	}
	if p.Now.IsZero() {
		return Transaction{}, ErrInvalidTimestamp
	}
	if !p.Money.IsValid() {
		return Transaction{}, money.ErrUninitialized
	}
	if !p.Money.IsPositive() {
		return Transaction{}, ErrOpeningNonPositiveAmount
	}

	now := p.Now.UTC()
	return Transaction{
		id:       p.ID,
		origin:   OriginInternal,
		kind:     KindOpening,
		status:   StatusProcessed,
		walletID: p.WalletID,
		playerID: p.PlayerID,
		amount:   p.Money,
		// A abertura credita o valor integral sobre saldo zero, então o saldo
		// resultante É o valor da abertura. Preencher aqui é o que torna uma
		// OPENING persistida reidratável (Rehydrate exige resultBalance em PROCESSED).
		resultBalance: p.Money,
		occurredAt:    now,
		createdAt:     now,
		updatedAt:     now,
	}, nil
}

// RehydrateParams descreve a reconstrução do estado persistido.
type RehydrateParams struct {
	ID                     string
	Origin                 Origin
	Kind                   Kind
	Status                 Status
	ProviderID             string
	ExternalTransactionID  string
	IdempotencyKey         string
	PayloadHash            string
	WalletID               string
	PlayerID               string
	RoundID                string
	GameID                 string
	Amount                 money.Money
	ExternalReferenceID    string
	ReferenceTransactionID string
	FailureCode            string
	FailureMessage         string
	ResultBalance          money.Money
	OccurredAt             time.Time
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// Rehydrate reconstrói a transação sem reaplicar movimentações nem emitir eventos.
func Rehydrate(p RehydrateParams) (Transaction, error) {
	if strings.TrimSpace(p.ID) == "" {
		return Transaction{}, ErrInvalidID
	}
	if !isKnownKind(p.Kind) {
		return Transaction{}, ErrInvalidKind
	}
	if !isKnownStatus(p.Status) {
		return Transaction{}, ErrInvalidStatus
	}
	if p.Origin != OriginInternal && p.Origin != OriginExternal {
		return Transaction{}, ErrInvalidOrigin
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Transaction{}, ErrInvalidWalletID
	}
	if strings.TrimSpace(p.PlayerID) == "" {
		return Transaction{}, ErrInvalidPlayerID
	}
	if !p.Amount.IsValid() {
		return Transaction{}, money.ErrUninitialized
	}
	if p.OccurredAt.IsZero() || p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
		return Transaction{}, ErrInvalidTimestamp
	}
	if p.Origin == OriginExternal {
		if p.Kind == KindOpening {
			return Transaction{}, ErrOpeningNotExternal
		}
		if strings.TrimSpace(p.ProviderID) == "" {
			return Transaction{}, ErrInvalidProviderID
		}
		if strings.TrimSpace(p.ExternalTransactionID) == "" {
			return Transaction{}, ErrInvalidExternalID
		}
		if strings.TrimSpace(p.IdempotencyKey) == "" {
			return Transaction{}, ErrInvalidIdempotencyKey
		}
		if strings.TrimSpace(p.PayloadHash) == "" {
			return Transaction{}, ErrInvalidPayloadHash
		}
	}
	if p.Status == StatusProcessed {
		if !p.ResultBalance.IsValid() {
			return Transaction{}, money.ErrUninitialized
		}
		if p.ResultBalance.Currency() != p.Amount.Currency() {
			return Transaction{}, ErrResultCurrencyMismatch
		}
	}
	if (p.Status == StatusRejected || p.Status == StatusFailed) && strings.TrimSpace(p.FailureCode) == "" {
		return Transaction{}, ErrInvalidFailureCode
	}

	return Transaction{
		id:                     p.ID,
		origin:                 p.Origin,
		kind:                   p.Kind,
		status:                 p.Status,
		providerID:             p.ProviderID,
		externalTransactionID:  p.ExternalTransactionID,
		idempotencyKey:         p.IdempotencyKey,
		payloadHash:            p.PayloadHash,
		walletID:               p.WalletID,
		playerID:               p.PlayerID,
		roundID:                p.RoundID,
		gameID:                 p.GameID,
		amount:                 p.Amount,
		externalReferenceID:    p.ExternalReferenceID,
		referenceTransactionID: p.ReferenceTransactionID,
		failureCode:            p.FailureCode,
		failureMessage:         p.FailureMessage,
		resultBalance:          p.ResultBalance,
		occurredAt:             p.OccurredAt.UTC(),
		createdAt:              p.CreatedAt.UTC(),
		updatedAt:              p.UpdatedAt.UTC(),
	}, nil
}

// MarkProcessed conclui a operação registrando o saldo observado.
//
// O saldo resultante é persistido para que o replay devolva o valor do
// processamento original, mesmo após novas movimentações na carteira.
func (t *Transaction) MarkProcessed(resultBalance money.Money, now time.Time) error {
	if !resultBalance.IsValid() {
		return money.ErrUninitialized
	}
	if resultBalance.Currency() != t.amount.Currency() {
		return ErrResultCurrencyMismatch
	}
	if err := t.transition(StatusProcessed, now); err != nil {
		return err
	}
	t.resultBalance = resultBalance
	return nil
}

// MarkPendingReference registra a espera por uma referência indisponível.
//
// O estado terminal é verificado ANTES da aplicabilidade da referência: uma
// transação já encerrada não pode ser reportada como "referência inaplicável",
// pois nenhuma transição é válida a partir de um estado terminal.
func (t *Transaction) MarkPendingReference(now time.Time) error {
	if t.status.IsTerminal() {
		return domainerr.New(domainerr.KindInvalidState, ErrTerminalState.Code(),
			fmt.Sprintf("transação em %s não aceita transição para %s", t.status, StatusPendingReference))
	}
	if !t.kind.AllowsReference() {
		return ErrReferenceNotApplicable
	}
	return t.transition(StatusPendingReference, now)
}

// ResolveReference registra a referência interna resolvida.
func (t *Transaction) ResolveReference(internalReferenceID string, now time.Time) error {
	if t.status.IsTerminal() {
		return domainerr.New(domainerr.KindInvalidState, ErrTerminalState.Code(),
			fmt.Sprintf("transação em %s não aceita resolução de referência", t.status))
	}
	// A referência pode ser resolvida em PENDING (quando ela já está
	// disponível no momento do processamento) ou em PENDING_REFERENCE (quando
	// a operação precisou esperar). O estado NÃO muda aqui: quem decide o
	// desfecho é MarkProcessed/MarkRejected.
	if t.status != StatusPending && t.status != StatusPendingReference {
		return domainerr.New(domainerr.KindInvalidState, ErrInvalidTransition.Code(),
			fmt.Sprintf("resolução de referência exige %s ou %s, atual %s",
				StatusPending, StatusPendingReference, t.status))
	}
	if strings.TrimSpace(internalReferenceID) == "" {
		return ErrInvalidReferenceID
	}
	if now.IsZero() {
		return ErrInvalidTimestamp
	}
	t.referenceTransactionID = internalReferenceID
	t.updatedAt = now.UTC()
	return nil
}

// MarkRejected encerra a operação como rejeição de negócio.
func (t *Transaction) MarkRejected(failureCode, failureMessage string, now time.Time) error {
	if strings.TrimSpace(failureCode) == "" {
		return ErrInvalidFailureCode
	}
	if err := t.transition(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = failureCode
	t.failureMessage = failureMessage
	return nil
}

// MarkFailed encerra a operação como falha permanente de infraestrutura.
func (t *Transaction) MarkFailed(failureCode, failureMessage string, now time.Time) error {
	if strings.TrimSpace(failureCode) == "" {
		return ErrInvalidFailureCode
	}
	if err := t.transition(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = failureCode
	t.failureMessage = failureMessage
	return nil
}

// IsTerminal informa se a transação está em estado terminal.
func (t Transaction) IsTerminal() bool { return t.status.IsTerminal() }

// IsReversal informa se a transação reverte outra operação.
func (t Transaction) IsReversal() bool { return t.kind.IsReversal() }

// IsExternal informa se a transação veio de HTTP/SQS.
func (t Transaction) IsExternal() bool { return t.origin == OriginExternal }

func (t Transaction) ID() string                    { return t.id }
func (t Transaction) Origin() Origin                { return t.origin }
func (t Transaction) Kind() Kind                    { return t.kind }
func (t Transaction) Status() Status                { return t.status }
func (t Transaction) ProviderID() string            { return t.providerID }
func (t Transaction) ExternalTransactionID() string { return t.externalTransactionID }
func (t Transaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t Transaction) PayloadHash() string           { return t.payloadHash }
func (t Transaction) WalletID() string              { return t.walletID }
func (t Transaction) PlayerID() string              { return t.playerID }
func (t Transaction) RoundID() string               { return t.roundID }
func (t Transaction) GameID() string                { return t.gameID }
func (t Transaction) Amount() money.Money           { return t.amount }
func (t Transaction) Currency() money.Currency      { return t.amount.Currency() }
func (t Transaction) ExternalReferenceID() string   { return t.externalReferenceID }
func (t Transaction) ReferenceTransactionID() string {
	return t.referenceTransactionID
}
func (t Transaction) FailureCode() string        { return t.failureCode }
func (t Transaction) FailureMessage() string     { return t.failureMessage }
func (t Transaction) ResultBalance() money.Money { return t.resultBalance }
func (t Transaction) OccurredAt() time.Time      { return t.occurredAt }
func (t Transaction) CreatedAt() time.Time       { return t.createdAt }
func (t Transaction) UpdatedAt() time.Time       { return t.updatedAt }

func (t *Transaction) transition(to Status, now time.Time) error {
	if t.status.IsTerminal() {
		return domainerr.New(domainerr.KindInvalidState, ErrTerminalState.Code(),
			fmt.Sprintf("transação em estado terminal %s não aceita transição para %s", t.status, to))
	}
	if !allowedTransition(t.status, to) {
		return domainerr.New(domainerr.KindInvalidState, ErrInvalidTransition.Code(),
			fmt.Sprintf("transição inválida: %s -> %s", t.status, to))
	}
	if now.IsZero() {
		return ErrInvalidTimestamp
	}
	t.status = to
	t.updatedAt = now.UTC()
	return nil
}

func allowedTransition(from, to Status) bool {
	switch from {
	case StatusPending:
		switch to {
		case StatusProcessed, StatusPendingReference, StatusRejected, StatusFailed:
			return true
		}
	case StatusPendingReference:
		switch to {
		case StatusProcessed, StatusRejected, StatusFailed:
			return true
		}
	}
	return false
}

func validateAmountForKind(kind Kind, amount money.Money) error {
	if kind.RequiresZeroAmount() && !amount.IsZero() {
		return ErrLossRequiresZeroAmount
	}
	if kind.RequiresPositiveAmount() && !amount.IsPositive() {
		return domainerr.New(domainerr.KindRuleViolation, ErrAmountMustBePositive.Code(),
			fmt.Sprintf("%s exige valor maior que zero", kind))
	}
	return nil
}

func validateReferenceForKind(kind Kind, referenceExternalID string) error {
	reference := strings.TrimSpace(referenceExternalID)
	if kind.RequiresReference() && reference == "" {
		return domainerr.New(domainerr.KindRuleViolation, ErrReferenceRequired.Code(),
			fmt.Sprintf("%s exige referenceExternalTransactionId", kind))
	}
	if !kind.AllowsReference() && reference != "" {
		return domainerr.New(domainerr.KindRuleViolation, ErrReferenceNotAllowed.Code(),
			fmt.Sprintf("%s não aceita referenceExternalTransactionId", kind))
	}
	return nil
}

func isKnownKind(k Kind) bool {
	switch k {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	default:
		return false
	}
}

func isKnownStatus(s Status) bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	default:
		return false
	}
}
