package domainerr

import (
	"errors"
	"fmt"
	"testing"
)

var (
	errSentinel = New(KindRuleViolation, "TEST_RULE", "regra violada")
	errOther    = New(KindInvalid, "TEST_INVALID", "entrada inválida")
)

func TestIsComparesByCode(t *testing.T) {
	derived := New(KindRuleViolation, "TEST_RULE", "outra mensagem, mesmo código")
	if !errors.Is(derived, errSentinel) {
		t.Fatal("erro derivado com mesmo Code deveria casar com o sentinel")
	}
	if errors.Is(derived, errOther) {
		t.Fatal("erro com Code diferente não deveria casar")
	}
}

func TestIsThroughWrap(t *testing.T) {
	wrapped := fmt.Errorf("camada externa: %w", errSentinel)
	if !errors.Is(wrapped, errSentinel) {
		t.Fatal("errors.Is deveria atravessar o wrap")
	}
}

func TestKindOfAndCodeOf(t *testing.T) {
	if got := KindOf(errSentinel); got != KindRuleViolation {
		t.Fatalf("KindOf = %v, quer %v", got, KindRuleViolation)
	}
	if got := CodeOf(errSentinel); got != "TEST_RULE" {
		t.Fatalf("CodeOf = %q, quer %q", got, "TEST_RULE")
	}
	if got := KindOf(errors.New("erro comum")); got != KindInternal {
		t.Fatalf("KindOf(erro comum) = %v, quer %v", got, KindInternal)
	}
	if got := CodeOf(errors.New("erro comum")); got != "" {
		t.Fatalf("CodeOf(erro comum) = %q, quer vazio", got)
	}
}

func TestWrapCarriesCause(t *testing.T) {
	cause := errors.New("causa")
	err := Wrap(KindTransient, "TEST_TRANSIENT", "falha transitória", cause)
	if !errors.Is(err, cause) {
		t.Fatal("errors.Is deveria encontrar a causa")
	}
	if IsKind(err, KindTransient) == false {
		t.Fatal("IsKind deveria reconhecer KindTransient")
	}
}
