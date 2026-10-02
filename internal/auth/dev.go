// Package auth implementa a validação de credenciais exigida por ports.Authenticator.
//
// A implementação de DESENVOLVIMENTO existe para que a camada HTTP, o isolamento
// entre provedores e a autorização por papel possam ser construídos e testados
// antes da integração com o IdP. Ela é deliberadamente impossível de confundir
// com autenticação real: exige o prefixo `dev:` e recusa ser construída fora
// dos ambientes locais.
//
// Formato do token de desenvolvimento:
//
//	Authorization: Bearer dev:<subject>:<providerId|->:<role[,role]>
//
// Exemplos:
//
//	Bearer dev:svc-provider-a:provider-a:provider
//	Bearer dev:svc-internal:-:internal
package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// DevTokenPrefix identifica um token de desenvolvimento.
const DevTokenPrefix = "dev:"

// DevAuthenticator valida tokens de desenvolvimento. NÃO usar em produção.
type DevAuthenticator struct{}

// NewDevAuthenticator recusa a construção fora dos ambientes locais.
func NewDevAuthenticator(env string) (*DevAuthenticator, error) {
	switch env {
	case "local", "dev", "test":
		return &DevAuthenticator{}, nil
	default:
		return nil, fmt.Errorf("autenticador de desenvolvimento não pode ser usado com APP_ENV=%q", env)
	}
}

// Authenticate valida o header Authorization.
func (a *DevAuthenticator) Authenticate(_ context.Context, authorizationHeader string) (ports.Identity, error) {
	header := strings.TrimSpace(authorizationHeader)
	if header == "" {
		return ports.Identity{}, ports.ErrMissingCredentials
	}
	const bearer = "Bearer "
	if !strings.HasPrefix(header, bearer) {
		return ports.Identity{}, ports.ErrInvalidCredentials
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, bearer))
	if !strings.HasPrefix(token, DevTokenPrefix) {
		return ports.Identity{}, ports.ErrInvalidCredentials
	}

	parts := strings.Split(strings.TrimPrefix(token, DevTokenPrefix), ":")
	if len(parts) != 3 {
		return ports.Identity{}, ports.ErrInvalidCredentials
	}
	subject := strings.TrimSpace(parts[0])
	providerID := strings.TrimSpace(parts[1])
	rolesRaw := parts[2]
	if subject == "" {
		return ports.Identity{}, ports.ErrInvalidCredentials
	}
	if providerID == "-" {
		providerID = ""
	}

	roles := make([]string, 0, 2)
	for _, role := range strings.Split(rolesRaw, ",") {
		if role = strings.TrimSpace(role); role != "" {
			roles = append(roles, role)
		}
	}
	if len(roles) == 0 {
		return ports.Identity{}, ports.ErrInvalidCredentials
	}

	return ports.Identity{Subject: subject, ProviderID: providerID, Roles: roles}, nil
}

var _ ports.Authenticator = (*DevAuthenticator)(nil)
