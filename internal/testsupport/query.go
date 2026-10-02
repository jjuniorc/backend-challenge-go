//go:build integration

package testsupport

import (
	"context"
	"testing"
)

// QueryString le um unico valor textual. Complementa QueryInt, que cobre
// contagens: os testes de inbox precisam conferir o estado persistido (status,
// transaction_id) e nao havia helper para texto.
//
// Usa a mesma conexao que QueryInt (connect + DSN do pacote), de modo que o
// banco de teste e o do pacote de teste, e nao o do servidor.
func QueryString(t *testing.T, sqlText string, args ...any) string {
	t.Helper()

	conn := connect(t)
	defer func() { _ = conn.Close(context.Background()) }()

	var value string
	if err := conn.QueryRow(context.Background(), sqlText, args...).Scan(&value); err != nil {
		t.Fatalf("consultando SQL de suporte: %v\nSQL: %s", err, sqlText)
	}
	return value
}
