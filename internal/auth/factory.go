package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// NewAuthenticator escolhe a implementação de autenticação a partir da
// configuração.
//
// `AUTH_MODE=oidc` (padrão) valida tokens do IdP. `AUTH_MODE=dev` habilita o
// autenticador de desenvolvimento, que só é aceito em ambientes locais — o
// próprio construtor recusa os demais, de modo que credenciais de teste não
// podem chegar a um ambiente real por descuido de configuração.
func NewAuthenticator(cfg config.Config) (ports.Authenticator, error) {
	switch cfg.Auth.Mode {
	case config.AuthModeDev:
		return NewDevAuthenticator(cfg.Env)

	case config.AuthModeOIDC:
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// Espera paciente: o IdP pode levar dezenas de segundos para publicar o
		// JWKS num ambiente recém-criado. Esgotado o prazo, a aplicação não sobe.
		return NewOIDCAuthenticator(ctx, VerifierConfig{
			Issuer:         cfg.OIDC.Issuer,
			JWKSURL:        cfg.OIDC.JWKSURL,
			Audience:       cfg.OIDC.Audience,
			StartupTimeout: 2 * time.Minute,
		})

	default:
		return nil, fmt.Errorf("auth: AUTH_MODE inválido: %q (use %q ou %q)",
			cfg.Auth.Mode, config.AuthModeOIDC, config.AuthModeDev)
	}
}
