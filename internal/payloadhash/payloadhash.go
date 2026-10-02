// Package payloadhash calcula o hash determinístico dos campos de negócio de uma
// operação externa. É a impressão digital usada para detectar reutilização de
// chave de idempotência com conteúdo diferente, com resultado IDÊNTICO para
// entradas via HTTP e via SQS.
//
// # Algoritmo
//
// SHA-256 sobre o JSON canônico dos campos de negócio, codificado em
// hexadecimal minúsculo (64 caracteres).
//
// # Excluídos do cálculo
//
//   - a chave de idempotência (header Idempotency-Key / data.idempotencyKey);
//   - metadados de transporte: messageId do envelope SQS, headers HTTP,
//     instante de recebimento, correlationId.
//
// # Normalizações aplicadas ANTES do hash
//
//   - campos textuais são aparados (trim);
//   - kind é normalizado para maiúsculas (" bet " e "BET" produzem "BET");
//   - amount é normalizado para a forma canônica de 2 casas do value object
//     Money ("25" e "25.00" produzem "25.00");
//   - referência externa ausente e string vazia são equivalentes ("");
//   - as chaves do JSON saem ordenadas alfabeticamente.
package payloadhash

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
)

// Algorithm identifica o algoritmo usado, para documentação e auditoria.
const Algorithm = "sha256"

// Erros sentinela. Classificáveis com errors.Is.
var (
	ErrMissingField = domainerr.New(domainerr.KindInvalid, "PAYLOAD_HASH_MISSING_FIELD", "campo de negócio obrigatório ausente no hash de idempotência")
	ErrInvalidMoney = domainerr.New(domainerr.KindInvalid, "PAYLOAD_HASH_INVALID_MONEY", "valor monetário inválido no hash de idempotência")
	ErrInvalidKind  = domainerr.New(domainerr.KindInvalid, "PAYLOAD_HASH_INVALID_KIND", "tipo de operação inválido no hash de idempotência")
	ErrInternalKind = domainerr.New(domainerr.KindInvalid, "PAYLOAD_HASH_INTERNAL_KIND", "OPENING não possui hash de payload externo")
)

// Input são os campos de negócio que entram no hash.
//
// Não há campo para chave de idempotência, messageId ou timestamps de
// transporte: a ausência é estrutural, não depende de quem chama.
type Input struct {
	ProviderID            string
	ExternalTransactionID string
	PlayerID              string
	WalletID              string
	RoundID               string
	GameID                string
	Kind                  string
	Money                 money.Money
	ExternalReferenceID   string
}

// CanonicalJSON devolve os bytes canônicos sobre os quais o hash é calculado.
// Exposto para documentação, depuração e testes de contrato.
func CanonicalJSON(in Input) ([]byte, error) {
	normalized, err := normalize(in)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Sem escape de HTML: mantém os bytes estáveis e legíveis.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(normalized); err != nil {
		return nil, domainerr.Wrap(domainerr.KindInternal, "PAYLOAD_HASH_ENCODE_FAILED", "falha ao serializar o payload canônico", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Compute devolve o hash SHA-256 em hexadecimal minúsculo.
func Compute(in Input) (string, error) {
	payload, err := CanonicalJSON(in)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func normalize(in Input) (map[string]string, error) {
	kind, err := wagertransaction.ParseKind(in.Kind)
	if err != nil {
		return nil, domainerr.Wrap(domainerr.KindInvalid, ErrInvalidKind.Code(), ErrInvalidKind.Error(), err)
	}
	if kind == wagertransaction.KindOpening {
		return nil, ErrInternalKind
	}
	if !in.Money.IsValid() {
		return nil, ErrInvalidMoney
	}

	fields := map[string]string{
		"providerId":                     in.ProviderID,
		"externalTransactionId":          in.ExternalTransactionID,
		"playerId":                       in.PlayerID,
		"walletId":                       in.WalletID,
		"roundId":                        in.RoundID,
		"gameId":                         in.GameID,
		"kind":                           string(kind),
		"amount":                         in.Money.String(),
		"currency":                       string(in.Money.Currency()),
		"externalReferenceTransactionId": in.ExternalReferenceID,
	}

	required := []string{
		"providerId",
		"externalTransactionId",
		"playerId",
		"walletId",
		"roundId",
		"gameId",
	}
	for _, name := range required {
		if trimSpace(fields[name]) == "" {
			return nil, domainerr.New(domainerr.KindInvalid, ErrMissingField.Code(),
				fmt.Sprintf("campo obrigatório ausente: %s", name))
		}
	}

	// Trim em todos os campos textuais para que variações de espaço não
	// produzam hashes diferentes.
	for k, v := range fields {
		fields[k] = trimSpace(v)
	}
	return fields, nil
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && isSpace(s[start]) {
		start++
	}
	end := len(s)
	for end > start && isSpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\v' || b == '\f'
}
