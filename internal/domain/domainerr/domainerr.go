// Package domainerr define os erros de domínio classificáveis.
//
// Todo erro de negócio é um *Error, que carrega:
//   - Kind: classificação ampla (mapeada para HTTP/SQS e para métricas);
//   - Code: código estável e documentado (é o failureCode persistido);
//   - Msg:  mensagem legível, sem dados sensíveis.
//
// A classificação é obtida com errors.Is (comparando Code) e com errors.As /
// KindOf / CodeOf (comparando o tipo), permitindo que os adaptadores mapeiem o
// erro sem inspecionar strings. Panic nunca é usado para rejeição de negócio.
package domainerr

import (
	"errors"
	"fmt"
)

// Kind é a classificação ampla do erro.
type Kind string

const (
	KindInvalid           Kind = "INVALID"
	KindConflict          Kind = "CONFLICT"
	KindNotFound          Kind = "NOT_FOUND"
	KindInsufficientFunds Kind = "INSUFFICIENT_FUNDS"
	KindCurrencyMismatch  Kind = "CURRENCY_MISMATCH"
	KindImmutable         Kind = "IMMUTABLE"
	KindInvalidState      Kind = "INVALID_STATE"
	KindRuleViolation     Kind = "RULE_VIOLATION"
	KindTransient         Kind = "TRANSIENT"
	KindUnauthenticated   Kind = "UNAUTHENTICATED"
	KindForbidden         Kind = "FORBIDDEN"
	KindInternal          Kind = "INTERNAL"
)

// Error é o erro de domínio.
type Error struct {
	kind  Kind
	code  string
	msg   string
	cause error
}

// New cria um erro de domínio sem causa.
func New(kind Kind, code, msg string) *Error {
	return &Error{kind: kind, code: code, msg: msg}
}

// Wrap cria um erro de domínio com causa. Se cause for nil, equivale a New.
func Wrap(kind Kind, code, msg string, cause error) *Error {
	if cause == nil {
		return New(kind, code, msg)
	}
	return &Error{kind: kind, code: code, msg: msg, cause: cause}
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.code, e.msg, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.code, e.msg)
}

// Unwrap expõe a causa para errors.Is/errors.As.
func (e *Error) Unwrap() error { return e.cause }

// Kind devolve a classificação ampla.
func (e *Error) Kind() Kind { return e.kind }

// Code devolve o código estável.
func (e *Error) Code() string { return e.code }

// Is compara pelo Code, de modo que errors.Is(err, pkg.ErrX) funcione tanto
// para o sentinel quanto para um erro derivado com o mesmo Code.
func (e *Error) Is(target error) bool {
	var t *Error
	if errors.As(target, &t) {
		return t.code != "" && t.code == e.code
	}
	return false
}

// KindOf devolve a classificação de err, ou KindInternal se não for domínio.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.kind
	}
	return KindInternal
}

// CodeOf devolve o Code de err, ou "" se não for erro de domínio.
func CodeOf(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.code
	}
	return ""
}

// IsKind informa se err pertence à classificação k.
func IsKind(err error, k Kind) bool { return KindOf(err) == k }
