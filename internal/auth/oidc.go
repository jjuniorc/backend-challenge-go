package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// OIDCAuthenticator valida tokens emitidos por um IdP OAuth 2.0/OIDC.
//
// Fluxo de `client_credentials`: o serviço/cliente obtém o token diretamente do
// IdP e o apresenta em `Authorization: Bearer`. A identidade autenticada (papéis
// e providerId) vem das claims do token, nunca do corpo da requisição.
type OIDCAuthenticator struct {
	verifier *Verifier
}

// NewOIDCAuthenticator constrói o autenticador, carregando o JWKS do IdP.
func NewOIDCAuthenticator(ctx context.Context, cfg VerifierConfig) (*OIDCAuthenticator, error) {
	verifier, err := NewVerifier(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &OIDCAuthenticator{verifier: verifier}, nil
}

// Authenticate valida o header Authorization e devolve a identidade.
func (a *OIDCAuthenticator) Authenticate(ctx context.Context, authorizationHeader string) (ports.Identity, error) {
	header := strings.TrimSpace(authorizationHeader)
	if header == "" {
		return ports.Identity{}, ports.ErrMissingCredentials
	}
	const bearerPrefix = "Bearer "
	if len(header) <= len(bearerPrefix) || !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return ports.Identity{}, fmt.Errorf("%w: esquema de autorização não suportado", ports.ErrInvalidCredentials)
	}
	token := strings.TrimSpace(header[len(bearerPrefix):])
	if token == "" {
		return ports.Identity{}, fmt.Errorf("%w: token vazio", ports.ErrInvalidCredentials)
	}

	claims, err := a.verifier.Verify(ctx, token)
	if err != nil {
		return ports.Identity{}, err
	}

	identity := ports.Identity{
		Subject:    strings.TrimSpace(claims.Subject),
		ProviderID: claims.ProviderID,
		Roles:      claims.Roles,
	}
	if identity.Subject == "" {
		identity.Subject = claims.ProviderID
	}
	// Um token com papel de provedor precisa carregar o providerId autorizado:
	// sem ele não haveria como restringir o provedor às suas próprias operações.
	if identity.HasRole(ports.RoleProvider) && identity.ProviderID == "" {
		return ports.Identity{}, fmt.Errorf("%w: token de provedor sem a claim providerId", ports.ErrInvalidCredentials)
	}
	return identity, nil
}

var _ ports.Authenticator = (*OIDCAuthenticator)(nil)
