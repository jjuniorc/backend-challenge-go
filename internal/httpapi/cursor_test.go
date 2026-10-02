package httpapi

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

func TestCursorRoundTrip(t *testing.T) {
	original := ports.LedgerCursor{
		CreatedAt: time.Date(2026, 5, 1, 12, 0, 0, 123456789, time.UTC),
		ID:        "00000000-0000-7000-8000-000000000001",
	}
	encoded := encodeCursor(original)
	if encoded == "" {
		t.Fatal("cursor não deveria ser vazio")
	}
	decoded, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !decoded.CreatedAt.Equal(original.CreatedAt) || decoded.ID != original.ID {
		t.Fatalf("cursor = %+v, quer %+v", decoded, original)
	}
}

// Os casos malformados são construídos com base64 no próprio teste: escrever
// base64 à mão é justamente o que produziu um caso inválido na versão anterior.
func TestDecodeCursorRejectsMalformed(t *testing.T) {
	encode := func(raw string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(raw))
	}
	const validID = "00000000-0000-7000-8000-000000000001"

	cases := []struct {
		name  string
		value string
	}{
		{"vazio", ""},
		{"nao e base64", "nao-e-base64!!"},
		{"json invalido", encode("{")},
		{"sem id", encode(`{"t":"2026-05-01T12:00:00Z"}`)},
		{"sem timestamp", encode(`{"i":"` + validID + `"}`)},
		{"timestamp invalido", encode(`{"t":"data-invalida","i":"` + validID + `"}`)},
		{"id vazio", encode(`{"t":"2026-05-01T12:00:00Z","i":""}`)},
		{"timestamp com tipo errado", encode(`{"t":123,"i":"` + validID + `"}`)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeCursor(tc.value); !errors.Is(err, errInvalidCursor) {
				t.Fatalf("decodeCursor(%q) erro = %v, quer errInvalidCursor", tc.value, err)
			}
		})
	}
}

func TestEncodeCursorIsOpaque(t *testing.T) {
	encoded := encodeCursor(ports.LedgerCursor{
		CreatedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		ID:        "00000000-0000-7000-8000-000000000001",
	})
	if strings.Contains(encoded, "2026-05-01") || strings.Contains(encoded, "00000000-0000-7000") {
		t.Fatalf("cursor deveria ser opaco: %s", encoded)
	}
}
