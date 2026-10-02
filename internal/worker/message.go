package worker

// Este arquivo define o CONTRATO DE ENTRADA do consumidor e o parser que o
// traduz para o MESMO comando do caso de uso usado pela camada HTTP.
//
// Decisão central: HTTP e SQS produzem o mesmo
// usecase.ProcessTransactionCommand. Logo compartilham o caso de uso, o hash de
// idempotência (internal/payloadhash) e TODAS as garantias financeiras. Este
// adaptador não tem regra de negócio: ele só traduz transporte.
//
// São dois hashes, com propósitos diferentes — e a distinção é deliberada:
//
//   - payloadhash.Compute: hash dos CAMPOS DE NEGÓCIO. Impressão digital da
//     operação financeira; idêntico entre HTTP e SQS; detecta reutilização de
//     chave de idempotência com conteúdo diferente.
//   - PayloadHashOf: hash do CORPO BRUTO da mensagem. Usado pela inbox para
//     verificar que uma reentrega é a MESMA mensagem. Um corpo reencodado com
//     formatação diferente é tratado como mensagem DIFERENTE (fail-safe: vai
//     para a DLQ e nunca movimenta saldo duas vezes).
//
// Classificação de erro: tudo que nasce aqui é falha PERMANENTE (KindInvalid ou
// KindConflict) — repetir a entrega não muda o resultado. Erro transitório vem
// do banco ou do broker, nunca do parser.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// TypeTransactionRequested é o único tipo de mensagem de ENTRADA aceito.
//
// É o contrato de REQUISIÇÃO recebido pelo consumidor, e não um evento
// publicado por esta aplicação: os eventos de saída estão em internal/events.
const TypeTransactionRequested = "WagerTransactionRequested"

// Códigos de falha da entrada por SQS, estáveis e documentados.
//
// Aparecem nos logs, nas métricas e na razão pela qual a mensagem foi
// encaminhada para a DLQ.
const (
	CodeSQSInvalidEnvelope = "SQS_INVALID_ENVELOPE"
	CodeSQSInvalidData     = "SQS_INVALID_DATA"
	CodeSQSUnknownType     = "SQS_UNKNOWN_TYPE"
	CodeSQSMissingField    = "SQS_MISSING_FIELD"
	CodeSQSInvalidTime     = "SQS_INVALID_OCCURRED_AT"
	CodeSQSInvalidMoney    = "SQS_INVALID_MONEY"
	CodeSQSInvalidKind     = "SQS_INVALID_KIND"
	// CodeInboxHashMismatch é emitido pelo caso de uso quando a inbox já
	// registrou a mesma identidade de mensagem com conteúdo diferente.
	// Referencia a constante do caso de uso para que exista UMA definição do
	// código: dois literais iguais em pacotes diferentes divergiriam em silêncio.
	CodeInboxHashMismatch = usecase.CodeInboxPayloadHashMismatch
)

// Erros sentinela da entrada, classificáveis com errors.Is (comparação por
// código, como no resto do projeto).
var (
	ErrInvalidEnvelope   = domainerr.New(domainerr.KindInvalid, CodeSQSInvalidEnvelope, "corpo da mensagem não é um envelope JSON válido")
	ErrInvalidData       = domainerr.New(domainerr.KindInvalid, CodeSQSInvalidData, "data da mensagem inválido")
	ErrUnknownType       = domainerr.New(domainerr.KindInvalid, CodeSQSUnknownType, "tipo de mensagem não aceito pelo consumidor")
	ErrMissingField      = domainerr.New(domainerr.KindInvalid, CodeSQSMissingField, "campo obrigatório ausente na mensagem")
	ErrInvalidOccurredAt = domainerr.New(domainerr.KindInvalid, CodeSQSInvalidTime, "occurredAt ausente ou em formato inválido")
	ErrInvalidMoney      = domainerr.New(domainerr.KindInvalid, CodeSQSInvalidMoney, "valor monetário inválido: amount deve ser string decimal (ex.: \"25.00\"), nunca número")
	ErrInvalidKind       = domainerr.New(domainerr.KindInvalid, CodeSQSInvalidKind, "tipo de operação inválido na mensagem")
	ErrInboxHashMismatch = domainerr.New(domainerr.KindConflict, CodeInboxHashMismatch, "reentrega com conteúdo diferente sob a mesma identidade de mensagem")
)

// InboundEnvelope é o envelope da mensagem de entrada.
type InboundEnvelope struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

// InboundTransactionData são os campos de negócio da mensagem de entrada.
//
// Os nomes correspondem EXATAMENTE aos do contrato HTTP, com uma diferença: no
// HTTP a chave vem no header Idempotency-Key; no SQS ela é data.idempotencyKey.
// Todos os demais campos entram no hash de idempotência (a chave, não).
type InboundTransactionData struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	IdempotencyKey                 string      `json:"idempotencyKey"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
}

// InboundTransaction é o resultado do parsing.
type InboundTransaction struct {
	MessageID  string
	OccurredAt time.Time
	Data       InboundTransactionData
	// PayloadHash é o hash do CORPO BRUTO recebido. É a identidade de transporte
	// que a inbox grava e verifica em reentregas — distinta do hash de negócio
	// usado pelo caso de uso (payloadhash.Compute).
	PayloadHash string
	// Command é o comando do caso de uso, idêntico ao montado pela camada HTTP.
	Command usecase.ProcessTransactionCommand
}

// ParseInboundTransaction traduz o corpo bruto de uma mensagem para o comando do
// caso de uso.
//
// O correlationId recebe o messageId: é o que liga a mensagem do broker aos
// eventos gerados por ela. Ele NÃO entra no hash de idempotência (o payloadhash
// exclui metadados de transporte), então o hash continua idêntico ao da entrada
// HTTP equivalente.
func ParseInboundTransaction(body []byte) (InboundTransaction, error) {
	var env InboundEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return InboundTransaction{}, domainerr.Wrap(domainerr.KindInvalid, CodeSQSInvalidEnvelope,
			"corpo da mensagem não é um envelope JSON válido", err)
	}

	messageID := strings.TrimSpace(env.MessageID)
	if messageID == "" {
		return InboundTransaction{}, missingField("messageId")
	}
	if got := strings.TrimSpace(env.Type); got != TypeTransactionRequested {
		return InboundTransaction{}, domainerr.New(domainerr.KindInvalid, CodeSQSUnknownType,
			fmt.Sprintf("tipo de mensagem não aceito: %q (esperado %q)", got, TypeTransactionRequested))
	}

	occurredAt, err := parseOccurredAt(env.OccurredAt)
	if err != nil {
		return InboundTransaction{}, err
	}
	if len(env.Data) == 0 {
		return InboundTransaction{}, missingField("data")
	}

	data, err := parseInboundData(env.Data)
	if err != nil {
		return InboundTransaction{}, err
	}

	return InboundTransaction{
		MessageID:   messageID,
		OccurredAt:  occurredAt,
		Data:        data,
		PayloadHash: PayloadHashOf(body),
		Command: usecase.ProcessTransactionCommand{
			ProviderID:            data.ProviderID,
			ExternalTransactionID: data.ExternalTransactionID,
			IdempotencyKey:        data.IdempotencyKey,
			PlayerID:              data.PlayerID,
			WalletID:              data.WalletID,
			RoundID:               data.RoundID,
			GameID:                data.GameID,
			Kind:                  data.Kind,
			Money:                 data.Money,
			ReferenceExternalID:   data.ReferenceExternalTransactionID,
			CorrelationID:         messageID,
		},
	}, nil
}

// PayloadHashOf devolve o hash do corpo BRUTO da mensagem.
//
// É a identidade de transporte verificada pela inbox em reentregas. SHA-256
// sobre os bytes recebidos: o broker preserva o corpo, então uma reentrega
// legítima produz o mesmo hash.
func PayloadHashOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// parseInboundData decodifica data com classificação PRECISA de erro.
//
// money tem tratamento próprio: valor monetário é ponto crítico do desafio (a
// entrada em ponto flutuante é critério eliminatório), e "campo ausente" precisa
// ser distinguível de "campo inválido" para o diagnóstico da DLQ.
func parseInboundData(raw json.RawMessage) (InboundTransactionData, error) {
	var probe struct {
		ProviderID                     string          `json:"providerId"`
		ExternalTransactionID          string          `json:"externalTransactionId"`
		IdempotencyKey                 string          `json:"idempotencyKey"`
		PlayerID                       string          `json:"playerId"`
		WalletID                       string          `json:"walletId"`
		RoundID                        string          `json:"roundId"`
		GameID                         string          `json:"gameId"`
		Kind                           string          `json:"kind"`
		Money                          json.RawMessage `json:"money"`
		ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return InboundTransactionData{}, domainerr.Wrap(domainerr.KindInvalid, CodeSQSInvalidData, "data da mensagem inválido", err)
	}

	// roundId e gameId são exigidos aqui porque já são exigidos no HTTP: eles
	// entram no hash de idempotência, então uma mensagem sem eles nunca poderia
	// ser processada — melhor falhar com diagnóstico claro do que no hash.
	required := []struct{ name, value string }{
		{"providerId", probe.ProviderID},
		{"externalTransactionId", probe.ExternalTransactionID},
		{"idempotencyKey", probe.IdempotencyKey},
		{"playerId", probe.PlayerID},
		{"walletId", probe.WalletID},
		{"roundId", probe.RoundID},
		{"gameId", probe.GameID},
		{"kind", probe.Kind},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return InboundTransactionData{}, missingField(field.name)
		}
	}

	if len(probe.Money) == 0 {
		return InboundTransactionData{}, missingField("money")
	}
	var amount money.Money
	if err := json.Unmarshal(probe.Money, &amount); err != nil {
		return InboundTransactionData{}, domainerr.Wrap(domainerr.KindInvalid, CodeSQSInvalidMoney,
			"money inválido na mensagem: amount deve ser string decimal (ex.: \"25.00\"), nunca número", err)
	}
	if !amount.IsValid() {
		return InboundTransactionData{}, domainerr.New(domainerr.KindInvalid, CodeSQSInvalidMoney,
			"money inválido na mensagem: valor não inicializado")
	}

	// Valida o tipo de operação aqui para que um kind inválido seja uma falha
	// PERMANENTE identificada na entrada, e não um erro genérico do caso de uso.
	if _, err := wagertransaction.ParseKind(probe.Kind); err != nil {
		return InboundTransactionData{}, domainerr.Wrap(domainerr.KindInvalid, CodeSQSInvalidKind,
			fmt.Sprintf("kind inválido na mensagem: %q", strings.TrimSpace(probe.Kind)), err)
	}

	return InboundTransactionData{
		ProviderID:                     strings.TrimSpace(probe.ProviderID),
		ExternalTransactionID:          strings.TrimSpace(probe.ExternalTransactionID),
		IdempotencyKey:                 strings.TrimSpace(probe.IdempotencyKey),
		PlayerID:                       strings.TrimSpace(probe.PlayerID),
		WalletID:                       strings.TrimSpace(probe.WalletID),
		RoundID:                        strings.TrimSpace(probe.RoundID),
		GameID:                         strings.TrimSpace(probe.GameID),
		Kind:                           strings.TrimSpace(probe.Kind),
		Money:                          amount,
		ReferenceExternalTransactionID: strings.TrimSpace(probe.ReferenceExternalTransactionID),
	}, nil
}

// parseOccurredAt aceita o formato documentado (RFC 3339 com milissegundos) e,
// por tolerância, RFC 3339 completo. Normaliza para UTC.
func parseOccurredAt(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, ErrInvalidOccurredAt
	}
	if parsed, err := time.Parse(events.TimeLayout, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, domainerr.New(domainerr.KindInvalid, CodeSQSInvalidTime,
		fmt.Sprintf("occurredAt não está no formato %q: %q", events.TimeLayout, trimmed))
}

func missingField(field string) *domainerr.Error {
	return domainerr.New(domainerr.KindInvalid, CodeSQSMissingField,
		fmt.Sprintf("campo obrigatório ausente: %s", field))
}
