// Package events define o envelope e os tipos concretos dos eventos de
// integração publicados pela aplicação.
//
// Regras de contrato implementadas aqui:
//
//   - o tipo e a versão do evento são definidos pelo CONSTRUTOR, nunca pelo
//     chamador, evitando evento com tipo/versão inconsistentes;
//   - occurredAt é sempre normalizado para UTC e serializado em RFC 3339 com
//     milissegundos (ex.: "2026-09-08T12:00:00.000Z");
//   - valores monetários saem como strings decimais (nunca número);
//   - aggregateId é a carteira, que é a raiz do agregado financeiro; isso
//     alinha o evento com o MessageGroupId do SQS (ordem por carteira).
//
// O payload da outbox é o snapshot imutável do envelope: uma vez construído,
// o evento não é mutado.
package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
)

// CurrentVersion é a versão do esquema do evento.
const CurrentVersion = 1

// TimeLayout é o formato de serialização de occurredAt.
const TimeLayout = "2006-01-02T15:04:05.000Z"

// Type identifica o tipo do evento.
type Type string

const (
	TypeWagerTransactionProcessed        Type = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         Type = "WagerTransactionRejected"
	TypeWalletBalanceChanged             Type = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference Type = "WagerTransactionPendingReference"
)

// IsKnown informa se o tipo é um dos tipos suportados.
func (t Type) IsKnown() bool {
	switch t {
	case TypeWagerTransactionProcessed,
		TypeWagerTransactionRejected,
		TypeWalletBalanceChanged,
		TypeWagerTransactionPendingReference:
		return true
	default:
		return false
	}
}

// Erros sentinela. Classificáveis com errors.Is.
var (
	ErrMissingField   = domainerr.New(domainerr.KindInvalid, "EVENT_MISSING_FIELD", "campo obrigatório ausente no evento")
	ErrUnknownType    = domainerr.New(domainerr.KindInvalid, "EVENT_UNKNOWN_TYPE", "tipo de evento desconhecido")
	ErrInvalidVersion = domainerr.New(domainerr.KindInvalid, "EVENT_INVALID_VERSION", "versão de evento inválida")
	ErrInvalidTime    = domainerr.New(domainerr.KindInvalid, "EVENT_INVALID_TIMESTAMP", "occurredAt ausente ou zero")
	ErrInvalidMoney   = domainerr.New(domainerr.KindInvalid, "EVENT_INVALID_MONEY", "valor monetário inválido no evento")
	ErrInvalidPayload = domainerr.New(domainerr.KindInvalid, "EVENT_INVALID_PAYLOAD", "data do evento inválido")
)

// Meta são os metadados de rastreio e tempo do evento.
type Meta struct {
	EventID       string
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

func (m Meta) validate() error {
	if strings.TrimSpace(m.EventID) == "" {
		return missing("eventId")
	}
	if strings.TrimSpace(m.CorrelationID) == "" {
		return missing("correlationId")
	}
	if m.OccurredAt.IsZero() {
		return ErrInvalidTime
	}
	return nil
}

// Envelope é o evento de integração.
type Envelope struct {
	EventID       string          `json:"eventId"`
	EventType     Type            `json:"eventType"`
	AggregateID   string          `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

// Validate confere a integridade do envelope.
func (e Envelope) Validate() error {
	if err := (Meta{EventID: e.EventID, CorrelationID: e.CorrelationID, OccurredAt: e.OccurredAt}).validate(); err != nil {
		return err
	}
	if !e.EventType.IsKnown() {
		return domainerr.New(domainerr.KindInvalid, ErrUnknownType.Code(),
			fmt.Sprintf("tipo de evento desconhecido: %q", e.EventType))
	}
	if strings.TrimSpace(e.AggregateID) == "" {
		return missing("aggregateId")
	}
	if e.Version < 1 {
		return domainerr.New(domainerr.KindInvalid, ErrInvalidVersion.Code(),
			fmt.Sprintf("versão de evento deve ser >= 1, recebido %d", e.Version))
	}
	if len(e.Data) == 0 || !json.Valid(e.Data) {
		return ErrInvalidPayload
	}
	return nil
}

// Payload serializa o envelope para publicação (snapshot imutável).
func (e Envelope) Payload() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// MarshalJSON serializa occurredAt em RFC 3339 com milissegundos e sufixo Z.
//
// A struct de serialização é declarada explicitamente, sem embedding, porque a
// ordem de saída do encoding/json segue a POSIÇÃO do campo na struct: um campo
// embutido colocaria occurredAt no fim do objeto. A ordem estável das chaves é
// parte do contrato do evento (payload determinístico para assinatura e
// auditoria).
func (e Envelope) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		EventID       string          `json:"eventId"`
		EventType     Type            `json:"eventType"`
		AggregateID   string          `json:"aggregateId"`
		CorrelationID string          `json:"correlationId"`
		CausationID   string          `json:"causationId,omitempty"`
		OccurredAt    string          `json:"occurredAt"`
		Version       int             `json:"version"`
		Data          json.RawMessage `json:"data"`
	}{
		EventID:       e.EventID,
		EventType:     e.EventType,
		AggregateID:   e.AggregateID,
		CorrelationID: e.CorrelationID,
		CausationID:   e.CausationID,
		OccurredAt:    e.OccurredAt.UTC().Format(TimeLayout),
		Version:       e.Version,
		Data:          e.Data,
	})
}

// UnmarshalJSON aceita occurredAt em RFC 3339 e normaliza para UTC.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var raw struct {
		EventID       string          `json:"eventId"`
		EventType     Type            `json:"eventType"`
		AggregateID   string          `json:"aggregateId"`
		CorrelationID string          `json:"correlationId"`
		CausationID   string          `json:"causationId"`
		OccurredAt    string          `json:"occurredAt"`
		Version       int             `json:"version"`
		Data          json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, raw.OccurredAt)
	if err != nil {
		return domainerr.Wrap(domainerr.KindInvalid, ErrInvalidTime.Code(),
			"occurredAt não está em RFC 3339", err)
	}
	*e = Envelope{
		EventID:       raw.EventID,
		EventType:     raw.EventType,
		AggregateID:   raw.AggregateID,
		CorrelationID: raw.CorrelationID,
		CausationID:   raw.CausationID,
		OccurredAt:    parsed.UTC(),
		Version:       raw.Version,
		Data:          raw.Data,
	}
	return nil
}

// ── WagerTransactionProcessed ────────────────────────────────────────

// ProcessedData é o payload de WagerTransactionProcessed.
type ProcessedData struct {
	TransactionID string                  `json:"transactionId"`
	ProviderID    string                  `json:"providerId,omitempty"`
	WalletID      string                  `json:"walletId"`
	Kind          wagertransaction.Kind   `json:"kind"`
	Status        wagertransaction.Status `json:"status"`
	Money         money.Money             `json:"money"`
	Balance       money.Money             `json:"balance"`
}

// ProcessedParams descreve a construção de WagerTransactionProcessed.
type ProcessedParams struct {
	TransactionID string
	ProviderID    string // vazio em OPENING (origem interna)
	WalletID      string
	Kind          wagertransaction.Kind
	Status        wagertransaction.Status
	Money         money.Money
	Balance       money.Money
}

// NewWagerTransactionProcessed constrói o evento de conclusão bem-sucedida,
// incluindo LOSS.
func NewWagerTransactionProcessed(meta Meta, p ProcessedParams) (Envelope, error) {
	if err := meta.validate(); err != nil {
		return Envelope{}, err
	}
	if strings.TrimSpace(p.TransactionID) == "" {
		return Envelope{}, missing("transactionId")
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Envelope{}, missing("walletId")
	}
	if p.Kind == "" {
		return Envelope{}, missing("kind")
	}
	if p.Status == "" {
		return Envelope{}, missing("status")
	}
	if !p.Money.IsValid() || !p.Balance.IsValid() {
		return Envelope{}, ErrInvalidMoney
	}
	return build(meta, TypeWagerTransactionProcessed, p.WalletID, ProcessedData{
		TransactionID: p.TransactionID,
		ProviderID:    strings.TrimSpace(p.ProviderID),
		WalletID:      p.WalletID,
		Kind:          p.Kind,
		Status:        p.Status,
		Money:         p.Money,
		Balance:       p.Balance,
	})
}

// ── WagerTransactionRejected ─────────────────────────────────────────

// RejectedData é o payload de WagerTransactionRejected.
type RejectedData struct {
	TransactionID  string                `json:"transactionId"`
	ProviderID     string                `json:"providerId,omitempty"`
	WalletID       string                `json:"walletId"`
	Kind           wagertransaction.Kind `json:"kind"`
	FailureCode    string                `json:"failureCode"`
	FailureMessage string                `json:"failureMessage,omitempty"`
}

// RejectedParams descreve a construção de WagerTransactionRejected.
type RejectedParams struct {
	TransactionID  string
	ProviderID     string
	WalletID       string
	Kind           wagertransaction.Kind
	FailureCode    string
	FailureMessage string
}

// NewWagerTransactionRejected constrói o evento de rejeição definitiva por
// regra de negócio.
func NewWagerTransactionRejected(meta Meta, p RejectedParams) (Envelope, error) {
	if err := meta.validate(); err != nil {
		return Envelope{}, err
	}
	if strings.TrimSpace(p.TransactionID) == "" {
		return Envelope{}, missing("transactionId")
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Envelope{}, missing("walletId")
	}
	if p.Kind == "" {
		return Envelope{}, missing("kind")
	}
	if strings.TrimSpace(p.FailureCode) == "" {
		return Envelope{}, missing("failureCode")
	}
	return build(meta, TypeWagerTransactionRejected, p.WalletID, RejectedData{
		TransactionID:  p.TransactionID,
		ProviderID:     strings.TrimSpace(p.ProviderID),
		WalletID:       p.WalletID,
		Kind:           p.Kind,
		FailureCode:    p.FailureCode,
		FailureMessage: p.FailureMessage,
	})
}

// ── WalletBalanceChanged ─────────────────────────────────────────────

// BalanceChangedData é o payload de WalletBalanceChanged.
type BalanceChangedData struct {
	WalletID      string           `json:"walletId"`
	TransactionID string           `json:"transactionId"`
	Direction     ledger.Direction `json:"direction"`
	Money         money.Money      `json:"money"`
	BalanceBefore money.Money      `json:"balanceBefore"`
	BalanceAfter  money.Money      `json:"balanceAfter"`
	WalletVersion int64            `json:"walletVersion"`
}

// BalanceChangedParams descreve a construção de WalletBalanceChanged.
type BalanceChangedParams struct {
	WalletID      string
	TransactionID string
	Direction     ledger.Direction
	Money         money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
}

// NewWalletBalanceChanged constrói o evento de alteração efetiva de saldo.
func NewWalletBalanceChanged(meta Meta, p BalanceChangedParams) (Envelope, error) {
	if err := meta.validate(); err != nil {
		return Envelope{}, err
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Envelope{}, missing("walletId")
	}
	if strings.TrimSpace(p.TransactionID) == "" {
		return Envelope{}, missing("transactionId")
	}
	if _, err := ledger.ParseDirection(string(p.Direction)); err != nil {
		return Envelope{}, domainerr.Wrap(domainerr.KindInvalid, "EVENT_INVALID_DIRECTION", "direção inválida em WalletBalanceChanged", err)
	}
	if !p.Money.IsValid() || !p.BalanceBefore.IsValid() || !p.BalanceAfter.IsValid() {
		return Envelope{}, ErrInvalidMoney
	}
	if p.WalletVersion < 1 {
		return Envelope{}, domainerr.New(domainerr.KindInvalid, "EVENT_INVALID_WALLET_VERSION",
			fmt.Sprintf("walletVersion deve ser >= 1, recebido %d", p.WalletVersion))
	}
	return build(meta, TypeWalletBalanceChanged, p.WalletID, BalanceChangedData{
		WalletID:      p.WalletID,
		TransactionID: p.TransactionID,
		Direction:     p.Direction,
		Money:         p.Money,
		BalanceBefore: p.BalanceBefore,
		BalanceAfter:  p.BalanceAfter,
		WalletVersion: p.WalletVersion,
	})
}

// ── WagerTransactionPendingReference ─────────────────────────────────

// PendingReferenceData é o payload de WagerTransactionPendingReference.
type PendingReferenceData struct {
	TransactionID                  string                `json:"transactionId"`
	ProviderID                     string                `json:"providerId,omitempty"`
	WalletID                       string                `json:"walletId"`
	Kind                           wagertransaction.Kind `json:"kind"`
	ReferenceExternalTransactionID string                `json:"referenceExternalTransactionId"`
	Attempts                       int                   `json:"attempts"`
}

// PendingReferenceParams descreve a construção de WagerTransactionPendingReference.
type PendingReferenceParams struct {
	TransactionID                  string
	ProviderID                     string
	WalletID                       string
	Kind                           wagertransaction.Kind
	ReferenceExternalTransactionID string
	Attempts                       int
}

// NewWagerTransactionPendingReference constrói o evento de registro de espera
// pela referência.
func NewWagerTransactionPendingReference(meta Meta, p PendingReferenceParams) (Envelope, error) {
	if err := meta.validate(); err != nil {
		return Envelope{}, err
	}
	if strings.TrimSpace(p.TransactionID) == "" {
		return Envelope{}, missing("transactionId")
	}
	if strings.TrimSpace(p.WalletID) == "" {
		return Envelope{}, missing("walletId")
	}
	if p.Kind == "" {
		return Envelope{}, missing("kind")
	}
	if strings.TrimSpace(p.ReferenceExternalTransactionID) == "" {
		return Envelope{}, missing("referenceExternalTransactionId")
	}
	if p.Attempts < 1 {
		return Envelope{}, domainerr.New(domainerr.KindInvalid, "EVENT_INVALID_ATTEMPTS",
			fmt.Sprintf("attempts deve ser >= 1, recebido %d", p.Attempts))
	}
	return build(meta, TypeWagerTransactionPendingReference, p.WalletID, PendingReferenceData{
		TransactionID:                  p.TransactionID,
		ProviderID:                     strings.TrimSpace(p.ProviderID),
		WalletID:                       p.WalletID,
		Kind:                           p.Kind,
		ReferenceExternalTransactionID: p.ReferenceExternalTransactionID,
		Attempts:                       p.Attempts,
	})
}

// ── helpers ──────────────────────────────────────────────────────────

func build(meta Meta, t Type, aggregateID string, data any) (Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, domainerr.Wrap(domainerr.KindInternal, "EVENT_ENCODE_FAILED", "falha ao serializar o data do evento", err)
	}
	env := Envelope{
		EventID:       meta.EventID,
		EventType:     t,
		AggregateID:   aggregateID,
		CorrelationID: meta.CorrelationID,
		CausationID:   strings.TrimSpace(meta.CausationID),
		OccurredAt:    meta.OccurredAt.UTC(),
		Version:       CurrentVersion,
		Data:          raw,
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func missing(field string) *domainerr.Error {
	return domainerr.New(domainerr.KindInvalid, ErrMissingField.Code(),
		fmt.Sprintf("campo obrigatório ausente: %s", field))
}
