package httpapi

import (
	"errors"
	"net/http"
	"testing"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

func TestStatusFor(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"não encontrado", ports.ErrNotFound, http.StatusNotFound, "NOT_FOUND"},
		{"conflito de idempotência", ports.ErrIdempotencyConflict, http.StatusConflict, usecase.CodeIdempotencyKeyMismatch},
		{"carteira duplicada", ports.ErrWalletAlreadyExists, http.StatusConflict, usecase.CodeWalletAlreadyExists},
		{"conflito de concorrência", ports.ErrVersionConflict, http.StatusServiceUnavailable, "CONCURRENCY_CONFLICT"},
		{"violação de constraint", ports.ErrConstraintViolation, http.StatusInternalServerError, "INTERNAL"},
		{"ledger imutável", ports.ErrImmutable, http.StatusInternalServerError, "INTERNAL"},

		{"entrada inválida", domainerr.New(domainerr.KindInvalid, "INVALID_REQUEST_BODY", "x"), http.StatusBadRequest, "INVALID_REQUEST_BODY"},
		{"conflito de domínio", domainerr.New(domainerr.KindConflict, usecase.CodeIdempotencyKeyReused, "x"), http.StatusConflict, usecase.CodeIdempotencyKeyReused},
		{"não encontrado no domínio", domainerr.New(domainerr.KindNotFound, "WALLET_NOT_FOUND", "x"), http.StatusNotFound, "WALLET_NOT_FOUND"},
		{"saldo insuficiente", domainerr.New(domainerr.KindInsufficientFunds, usecase.CodeWalletInsufficientFunds, "x"), http.StatusUnprocessableEntity, usecase.CodeWalletInsufficientFunds},
		{"regra de negócio", domainerr.New(domainerr.KindRuleViolation, usecase.CodeReferenceAlreadyReversed, "x"), http.StatusUnprocessableEntity, usecase.CodeReferenceAlreadyReversed},
		{"moeda incompatível", domainerr.New(domainerr.KindCurrencyMismatch, usecase.CodeReferenceCurrencyMismatch, "x"), http.StatusUnprocessableEntity, usecase.CodeReferenceCurrencyMismatch},
		{"estado inválido", domainerr.New(domainerr.KindInvalidState, "WAGER_TERMINAL_STATE", "x"), http.StatusConflict, "WAGER_TERMINAL_STATE"},
		{"sem credencial", ports.ErrMissingCredentials, http.StatusUnauthorized, "AUTH_MISSING_CREDENTIALS"},
		{"credencial inválida", ports.ErrInvalidCredentials, http.StatusUnauthorized, "AUTH_INVALID_CREDENTIALS"},
		{"proibido", ports.ErrForbidden, http.StatusForbidden, "AUTH_FORBIDDEN"},
		{"transitório", domainerr.New(domainerr.KindTransient, "DB_UNAVAILABLE", "x"), http.StatusServiceUnavailable, "DB_UNAVAILABLE"},
		{"desconhecido", errors.New("erro qualquer"), http.StatusInternalServerError, "INTERNAL"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := statusFor(tc.err)
			if status != tc.wantStatus {
				t.Fatalf("status = %d, quer %d", status, tc.wantStatus)
			}
			if code != tc.wantCode {
				t.Fatalf("code = %q, quer %q", code, tc.wantCode)
			}
			if message == "" {
				t.Fatal("mensagem não pode ser vazia")
			}
		})
	}
}

func TestStatusForDoesNotLeakInternalDetails(t *testing.T) {
	err := domainerr.New(domainerr.KindInternal, "INTERNAL", "detalhe sensível do banco")
	status, _, message := statusFor(err)
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d", status)
	}
	if message == err.Error() {
		t.Fatalf("resposta 5xx não pode ecoar a mensagem interna: %q", message)
	}
}
