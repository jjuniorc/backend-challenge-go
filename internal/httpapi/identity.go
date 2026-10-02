// Package httpapi expõe os casos de uso via HTTP.
//
// Regra estrutural: o providerId autorizado vem SEMPRE da identidade
// autenticada (contexto), nunca do corpo da requisição. É isso que garante o
// isolamento entre provedores, inclusive em replays.
package httpapi

import (
	"context"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

type contextKey string

const (
	identityContextKey    contextKey = "wager.identity"
	correlationContextKey contextKey = "wager.correlationId"
)

func withIdentity(ctx context.Context, identity ports.Identity) context.Context {
	return context.WithValue(ctx, identityContextKey, identity)
}

// IdentityFrom devolve a identidade autenticada do contexto.
func IdentityFrom(ctx context.Context) (ports.Identity, bool) {
	identity, ok := ctx.Value(identityContextKey).(ports.Identity)
	return identity, ok
}

func withCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationContextKey, correlationID)
}

// CorrelationIDFrom devolve o correlationId da requisição.
func CorrelationIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(correlationContextKey).(string)
	return value
}
