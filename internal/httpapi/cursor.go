package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// O cursor é OPACO para o cliente: base64url de um JSON com a posição na
// ordenação (created_at, id). Isso permite mudar a estratégia de paginação sem
// quebrar contrato.

type cursorPayload struct {
	CreatedAt string `json:"t"`
	ID        string `json:"i"`
}

var errInvalidCursor = domainerr.New(domainerr.KindInvalid, "INVALID_CURSOR", "cursor de paginação inválido")

// encodeCursor serializa a posição.
func encodeCursor(cursor ports.LedgerCursor) string {
	raw, err := json.Marshal(cursorPayload{
		CreatedAt: cursor.CreatedAt.UTC().Format(time.RFC3339Nano),
		ID:        cursor.ID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeCursor desserializa a posição, rejeitando qualquer cursor malformado.
func decodeCursor(value string) (ports.LedgerCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return ports.LedgerCursor{}, errInvalidCursor
	}
	var payload cursorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ports.LedgerCursor{}, errInvalidCursor
	}
	if payload.ID == "" || payload.CreatedAt == "" {
		return ports.LedgerCursor{}, errInvalidCursor
	}
	createdAt, err := time.Parse(time.RFC3339Nano, payload.CreatedAt)
	if err != nil {
		return ports.LedgerCursor{}, errInvalidCursor
	}
	return ports.LedgerCursor{CreatedAt: createdAt.UTC(), ID: payload.ID}, nil
}
