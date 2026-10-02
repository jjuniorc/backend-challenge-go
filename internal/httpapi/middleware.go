package httpapi

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// RequestContext propaga (ou gera) o correlationId e o publica no contexto e no
// header de resposta.
func RequestContext(ids ports.IDGenerator) gin.HandlerFunc {
	return func(c *gin.Context) {
		correlationID := strings.TrimSpace(c.GetHeader("X-Correlation-Id"))
		if correlationID == "" {
			if generated, err := ids.NewID(); err == nil {
				correlationID = generated
			}
		}
		c.Request = c.Request.WithContext(withCorrelationID(c.Request.Context(), correlationID))
		c.Header("X-Correlation-Id", correlationID)
		c.Next()
	}
}

// Authenticate valida as credenciais e injeta a identidade no contexto.
func Authenticate(authenticator ports.Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		identity, err := authenticator.Authenticate(c.Request.Context(), c.GetHeader("Authorization"))
		if err != nil {
			writeError(c, err)
			return
		}
		c.Request = c.Request.WithContext(withIdentity(c.Request.Context(), identity))
		c.Next()
	}
}

// RequireRole restringe a rota a um papel.
func RequireRole(role string) gin.HandlerFunc {
	return func(c *gin.Context) {
		identity, ok := IdentityFrom(c.Request.Context())
		if !ok {
			writeError(c, ports.ErrMissingCredentials)
			return
		}
		if !identity.HasRole(role) {
			writeError(c, forbidden(fmt.Sprintf("a rota exige o papel %q", role)))
			return
		}
		c.Next()
	}
}

// AccessLog registra cada requisição em JSON, com os identificadores úteis para
// rastrear a operação. Nunca registra corpo nem credenciais.
func AccessLog(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		c.Next()

		logger.Info("requisição HTTP",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"durationMs", time.Since(started).Milliseconds(),
			"correlationId", CorrelationIDFrom(c.Request.Context()),
		)
	}
}

// Recovery converte panic em resposta no formato do contrato.
func Recovery(logger *slog.Logger) gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered any) {
		logger.Error("panic recuperado no handler HTTP",
			"err", fmt.Sprint(recovered),
			"path", c.Request.URL.Path,
			"correlationId", CorrelationIDFrom(c.Request.Context()),
		)
		writeError(c, domainerr.New(domainerr.KindInternal, "INTERNAL", "erro interno"))
	})
}
