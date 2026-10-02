package app

import (
	"testing"

	"go.uber.org/fx"
)

// TestModuleIsValid valida o GRAFO de dependências sem executar construtores,
// sem banco e sem rede. É o teste que o README pede para a composição Fx.
func TestModuleIsValid(t *testing.T) {
	// fx.NopLogger evita o dump do grafo no log dos testes.
	if err := fx.ValidateApp(Module, fx.NopLogger); err != nil {
		t.Fatalf("grafo do Fx inválido: %v", err)
	}
}
