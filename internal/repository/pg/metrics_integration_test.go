//go:build integration

package pg_test

import (
	"context"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
)

// TestCountByStatusAndFailureCode prova as duas consultas que alimentam as
// métricas derivadas do BANCO.
//
// O que importa aqui não é só o número: é que a MÉTRICA e a RECONCILIAÇÃO leem a
// mesma fonte. Um contador em memória poderia divergir do banco; uma contagem
// sobre a tabela, não.
func TestCountByStatusAndFailureCode(t *testing.T) {
	store := testsupport.NewStore(t)
	ctx := context.Background()
	const walletID = "00000000-0000-7000-8000-0000000000e1"

	insertWallet(t, store, walletID, "player-metrics", "1000.00")
	now := time.Now().UTC()

	// Uma operação concluída.
	processed := newExternal(t, "00000000-0000-7000-8000-0000000000e2", walletID,
		"ext-metrics-1", "key-metrics-1", wagertransaction.KindBet, "25.00")
	if err := processed.MarkProcessed(brl(t, "975.00"), now); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := store.Transactions().Insert(ctx, processed); err != nil {
		t.Fatalf("insert PROCESSED: %v", err)
	}

	// Uma rejeição por regra de negócio.
	rejected := newExternal(t, "00000000-0000-7000-8000-0000000000e3", walletID,
		"ext-metrics-2", "key-metrics-2", wagertransaction.KindBet, "25.00")
	if err := rejected.MarkRejected("WALLET_INSUFFICIENT_FUNDS", "saldo insuficiente", now); err != nil {
		t.Fatalf("MarkRejected: %v", err)
	}
	if err := store.Transactions().Insert(ctx, rejected); err != nil {
		t.Fatalf("insert REJECTED: %v", err)
	}

	// Um conflito de idempotência: é assim que ele fica observável.
	conflict := newExternal(t, "00000000-0000-7000-8000-0000000000e4", walletID,
		"ext-metrics-3", "key-metrics-3", wagertransaction.KindBet, "25.00")
	if err := conflict.MarkRejected("IDEMPOTENCY_KEY_MISMATCH", "chave reutilizada com outro conteúdo", now); err != nil {
		t.Fatalf("MarkRejected conflito: %v", err)
	}
	if err := store.Transactions().Insert(ctx, conflict); err != nil {
		t.Fatalf("insert conflito: %v", err)
	}

	byStatus, err := store.Transactions().CountByStatus(ctx)
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if byStatus["PROCESSED"] != 1 {
		t.Fatalf("PROCESSED = %d, quer 1", byStatus["PROCESSED"])
	}
	if byStatus["REJECTED"] != 2 {
		t.Fatalf("REJECTED = %d, quer 2", byStatus["REJECTED"])
	}

	byCode, err := store.Transactions().CountByFailureCode(ctx)
	if err != nil {
		t.Fatalf("CountByFailureCode: %v", err)
	}
	if byCode["WALLET_INSUFFICIENT_FUNDS"] != 1 {
		t.Fatalf("WALLET_INSUFFICIENT_FUNDS = %d, quer 1", byCode["WALLET_INSUFFICIENT_FUNDS"])
	}
	// O conflito de idempotência aparece como código de falha: é o que torna a
	// taxa de conflito observável sem instrumentar o caminho quente.
	if byCode["IDEMPOTENCY_KEY_MISMATCH"] != 1 {
		t.Fatalf("IDEMPOTENCY_KEY_MISMATCH = %d, quer 1 (conflito observável)", byCode["IDEMPOTENCY_KEY_MISMATCH"])
	}
	// Uma operação CONCLUÍDA não tem código de falha, e portanto não entra aqui.
	if _, present := byCode[""]; present {
		t.Fatal("operação sem falha não deveria aparecer na contagem por código")
	}
}
