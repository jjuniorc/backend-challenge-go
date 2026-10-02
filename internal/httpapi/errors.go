package httpapi

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// errorResponse é o corpo de erro do contrato.
type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code          string `json:"code"`
	Message       string `json:"message"`
	CorrelationID string `json:"correlationId,omitempty"`
}

// Classificação de erro por situação do contrato:
//
//	400 entrada inválida          401 credencial ausente/inválida
//	403 acesso não autorizado     404 recurso inexistente (ou de outro provedor)
//	409 conflito de idempotência  422 rejeição de negócio
//	503 indisponibilidade transitória
func statusFor(err error) (status int, code string, safeMessage string) {
	switch {
	case errors.Is(err, ports.ErrNotFound):
		return http.StatusNotFound, "NOT_FOUND", "recurso não encontrado"
	case errors.Is(err, ports.ErrIdempotencyConflict):
		return http.StatusConflict, usecase.CodeIdempotencyKeyMismatch, err.Error()
	case errors.Is(err, ports.ErrWalletAlreadyExists):
		return http.StatusConflict, usecase.CodeWalletAlreadyExists, err.Error()
	case errors.Is(err, ports.ErrOpeningAlreadyExists):
		return http.StatusConflict, "OPENING_ALREADY_EXISTS", err.Error()
	case errors.Is(err, ports.ErrVersionConflict):
		return http.StatusServiceUnavailable, "CONCURRENCY_CONFLICT", "conflito de concorrência; tente novamente"
	case errors.Is(err, ports.ErrDuplicate),
		errors.Is(err, ports.ErrConstraintViolation),
		errors.Is(err, ports.ErrImmutable):
		return http.StatusInternalServerError, "INTERNAL", "erro interno"
	}

	if code := domainerr.CodeOf(err); code != "" {
		switch domainerr.KindOf(err) {
		case domainerr.KindInvalid:
			return http.StatusBadRequest, code, err.Error()
		case domainerr.KindConflict:
			return http.StatusConflict, code, err.Error()
		case domainerr.KindNotFound:
			return http.StatusNotFound, code, err.Error()
		case domainerr.KindInsufficientFunds, domainerr.KindCurrencyMismatch, domainerr.KindRuleViolation:
			return http.StatusUnprocessableEntity, code, err.Error()
		case domainerr.KindInvalidState:
			return http.StatusConflict, code, err.Error()
		case domainerr.KindUnauthenticated:
			return http.StatusUnauthorized, code, err.Error()
		case domainerr.KindForbidden:
			return http.StatusForbidden, code, err.Error()
		case domainerr.KindTransient:
			return http.StatusServiceUnavailable, code, "dependência temporariamente indisponível"
		default:
			return http.StatusInternalServerError, code, "erro interno"
		}
	}

	return http.StatusInternalServerError, "INTERNAL", "erro interno"
}

// writeError traduz o erro e encerra a requisição. O corpo NUNCA expõe detalhes
// internos em respostas 5xx.
func writeError(c *gin.Context, err error) {
	status, code, message := statusFor(err)
	if c.Writer.Written() {
		c.Abort()
		return
	}
	c.AbortWithStatusJSON(status, errorResponse{Error: errorDetail{
		Code:          code,
		Message:       message,
		CorrelationID: CorrelationIDFrom(c.Request.Context()),
	}})
}

func forbidden(message string) error {
	return domainerr.New(domainerr.KindForbidden, "AUTH_FORBIDDEN", message)
}

func invalid(message string) error {
	return domainerr.New(domainerr.KindInvalid, "INVALID_REQUEST", message)
}
