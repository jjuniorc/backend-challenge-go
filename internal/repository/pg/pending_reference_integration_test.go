//go:build integration

package pg_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/repository/pg"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
)

// Estes testes provam a reserva da retomada de PENDING_REFERENCE contra
// PostgreSQL REAL. O que importa aqui:
//
//   - só entra o que VENCEU (next_attempt_at <= now), e o mais antigo primeiro;
//   - duas instâncias nunca recebem a mesma operação (FOR UPDATE SKIP LOCKED);
//   - a reserva exige transação: é o lock que a sustenta, não o statement.

// newPendingRefund cria um REFUND externo já em PENDING_REFERENCE, apontando
// para uma referência que não existe — exatamente o estado que o worker retoma.
//
// O construtor do domínio é usado em vez de SQL cru: assim a linha nasce com
// todas as invariantes que o domínio exige, e o teste falha por motivo
// financeiro, nunca por INSERT malformado.
func newPendingRefund(t *testing.T, id, walletID, externalID, key, amount, referenceExternalID string, scheduledAt time.Time) wagertransaction.Transaction {
	t.Helper()

	// Guard de formato ANTES de qualquer INSERT: um UUID literal malformado
	// não falha aqui — falha no INSERT, com SQLSTATE 22P02, e o erro aparece
	// como se fosse defeito do repositório. Aconteceu duas vezes neste arquivo,
	// as duas por contagem visual do último grupo.
	requireTestUUID(t, "id da operação", id)
	requireTestUUID(t, "walletId", walletID)

	refund, err := wagertransaction.NewExternal(wagertransaction.ExternalParams{
		ID:                    id,
		ProviderID:            "provider-a",
		ExternalTransactionID: externalID,
		IdempotencyKey:        key,
		PayloadHash:           "hash-" + id,
		WalletID:              walletID,
		PlayerID:              "player-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  wagertransaction.KindRefund,
		Money:                 brl(t, amount),
		ReferenceExternalID:   referenceExternalID,
		Now:                   baseTime,
	})
	if err != nil {
		t.Fatalf("NewExternal(%s): %v", id, err)
	}
	if err := refund.MarkPendingReference(baseTime); err != nil {
		t.Fatalf("MarkPendingReference(%s): %v", id, err)
	}
	return refund
}

// schedulePending insere a operação e agenda a tentativa para o instante dado.
func schedulePending(t *testing.T, store *pg.DB, refund wagertransaction.Transaction, nextAttemptAt time.Time) {
	t.Helper()
	ctx := context.Background()

	if err := store.Transactions().Insert(ctx, refund); err != nil {
		t.Fatalf("insert %s: %v", refund.ID(), err)
	}
	if err := store.Transactions().ScheduleReferenceRetry(ctx, refund.ID(), 1, nextAttemptAt); err != nil {
		t.Fatalf("ScheduleReferenceRetry(%s): %v", refund.ID(), err)
	}
}

// claimNext executa UMA tentativa como o worker faz: reserva a operação e, no
// MESMO commit, grava o próximo agendamento — o caminho "a referência ainda não
// chegou", que reagenda sem tocar em saldo.
//
// Isso não é conveniência de teste, é o contrato: a reserva sozinha NÃO é
// durável. O lock do FOR UPDATE SKIP LOCKED termina no commit e o worker de
// referências não usa lease (a tentativa inteira cabe numa transação), então uma
// reserva confirmada SEM gravar desfecho devolve a operação à disputa. O worker
// nunca faz isso: ou resolve, ou reagenda, ou esgota — sempre escrevendo o
// resultado no mesmo commit.
func claimNext(t *testing.T, store *pg.DB, now time.Time, deferFor time.Duration) (ports.PendingReference, bool) {
	t.Helper()

	var (
		pending ports.PendingReference
		found   bool
	)
	err := store.RunInTx(context.Background(), func(ctx context.Context, tx ports.Tx) error {
		var err error
		pending, found, err = tx.Transactions().ClaimDueReference(ctx, now)
		if err != nil || !found {
			return err
		}
		// Empurra a próxima tentativa para além da janela: a reserva passa a ter
		// efeito durável e a operação sai da disputa.
		return tx.Transactions().ScheduleReferenceRetry(ctx, pending.TransactionID, pending.Attempts+1, now.Add(deferFor))
	})
	if err != nil {
		t.Fatalf("ClaimDueReference: %v", err)
	}
	return pending, found
}

// ── o que entra e o que não entra ────────────────────────────────────

func TestClaimDueReferenceIgnoresFutureAndNonPending(t *testing.T) {
	store := testsupport.NewStore(t)
	const walletID = "00000000-0000-7000-8000-0000000000d1"

	insertWallet(t, store, walletID, "player-pending-1", "1000.00")
	now := time.Now().UTC()

	// Agendada para o futuro: não pode ser reservada ainda.
	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000000e1", walletID, "ext-r1", "key-r1", "10.00", "ext-bet-x", now),
		now.Add(time.Hour))

	if _, found := claimNext(t, store, now, time.Hour); found {
		t.Fatal("uma operação com agendamento futuro não deveria ser reservada")
	}

	// Vencida: é reservada.
	past := now.Add(-time.Minute)
	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000000e2", walletID, "ext-r2", "key-r2", "10.00", "ext-bet-x", past),
		past)

	pending, found := claimNext(t, store, now, time.Hour)
	if !found {
		t.Fatal("uma operação vencida deveria ser reservada")
	}
	if pending.TransactionID != "00000000-0000-7000-8000-0000000000e2" {
		t.Fatalf("reservou %s, quer a operação vencida", pending.TransactionID)
	}

	// Em PENDING_REFERENCE só entra quem está nesse estado: uma operação
	// concluída não pode ser retomada.
	if _, found := claimNext(t, store, now, time.Hour); found {
		t.Fatal("não deveria haver mais nada vencido")
	}
}

func TestClaimDueReferenceReturnsTheOldestFirst(t *testing.T) {
	store := testsupport.NewStore(t)
	const walletID = "00000000-0000-7000-8000-0000000000d2"

	insertWallet(t, store, walletID, "player-pending-2", "1000.00")
	now := time.Now().UTC()

	// Três vencidas, inseridas fora de ordem: a ordem de reserva tem de ser a do
	// vencimento, não a da inserção.
	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000000f1", walletID, "ext-o1", "key-o1", "10.00", "ext-bet-x", now),
		now.Add(-2*time.Minute))
	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000000f2", walletID, "ext-o2", "key-o2", "10.00", "ext-bet-x", now),
		now.Add(-10*time.Minute))
	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000000f3", walletID, "ext-o3", "key-o3", "10.00", "ext-bet-x", now),
		now.Add(-5*time.Minute))

	order := []string{
		"00000000-0000-7000-8000-0000000000f2", // -10m
		"00000000-0000-7000-8000-0000000000f3", // -5m
		"00000000-0000-7000-8000-0000000000f1", // -2m
	}
	for i, want := range order {
		pending, found := claimNext(t, store, now, time.Hour)
		if !found {
			t.Fatalf("reserva %d: nada vencido", i+1)
		}
		if pending.TransactionID != want {
			t.Fatalf("reserva %d = %s, quer %s (ordem do vencimento)", i+1, pending.TransactionID, want)
		}
	}
}

func TestClaimDueReferenceReportsAttemptsAgeAndReference(t *testing.T) {
	store := testsupport.NewStore(t)
	const walletID = "00000000-0000-7000-8000-0000000000d3"

	insertWallet(t, store, walletID, "player-pending-3", "1000.00")
	now := time.Now().UTC()
	past := now.Add(-time.Minute)

	refund := newPendingRefund(t, "00000000-0000-7000-8000-0000000000f4", walletID,
		"ext-meta", "key-meta", "12.34", "ext-bet-que-nao-existe", past)
	schedulePending(t, store, refund, past)

	// Uma tentativa anterior já registrada.
	if err := store.Transactions().ScheduleReferenceRetry(context.Background(), refund.ID(), 3, past); err != nil {
		t.Fatalf("ScheduleReferenceRetry: %v", err)
	}

	pending, found := claimNext(t, store, now, time.Hour)
	if !found {
		t.Fatal("a operação vencida deveria ser reservada")
	}
	if pending.Attempts != 3 {
		t.Fatalf("attempts = %d, quer 3", pending.Attempts)
	}
	if pending.Kind != wagertransaction.KindRefund {
		t.Fatalf("kind = %s, quer REFUND", pending.Kind)
	}
	if pending.ReferenceExternalID != "ext-bet-que-nao-existe" {
		t.Fatalf("referência externa = %q", pending.ReferenceExternalID)
	}
	if pending.ProviderID != "provider-a" {
		t.Fatalf("providerId = %q, quer provider-a", pending.ProviderID)
	}
	if pending.WalletID != walletID {
		t.Fatalf("walletId = %q, quer %q", pending.WalletID, walletID)
	}
	// A idade é medida a partir de created_at (baseTime, no passado): tem de ser
	// positiva e cobrir ao menos o intervalo conhecido.
	if pending.Age <= 0 {
		t.Fatalf("age = %s, deveria ser positiva", pending.Age)
	}
	if pending.NextAttemptAt.IsZero() {
		t.Fatal("nextAttemptAt zerado")
	}
}

// ── exclusividade entre instâncias ───────────────────────────────────

// TestClaimDueReferenceIsExclusiveBetweenWorkers mantém as transações ABERTAS
// enquanto todos tentam reservar. É o que prova que a exclusão vem do lock
// (FOR UPDATE SKIP LOCKED) e não da ordem de execução: sem a transação aberta, o
// PostgreSQL libera o lock ao fim do statement e o teste não provaria nada.
func TestClaimDueReferenceIsExclusiveBetweenWorkers(t *testing.T) {
	store := testsupport.NewStore(t)
	const walletID = "00000000-0000-7000-8000-0000000000d4"

	insertWallet(t, store, walletID, "player-pending-4", "1000.00")
	now := time.Now().UTC()
	past := now.Add(-time.Minute)

	// UUIDs válidos: as colunas são UUID no schema.
	ids := []string{
		"00000000-0000-7000-8000-00000000a001",
		"00000000-0000-7000-8000-00000000a002",
		"00000000-0000-7000-8000-00000000a003",
	}
	for i, id := range ids {
		schedulePending(t, store,
			newPendingRefund(t, id, walletID, fmt.Sprintf("ext-x%d", i+1), fmt.Sprintf("key-x%d", i+1), "10.00", "ext-bet-x", past),
			past)
	}

	const workers = 4 // mais tentativas do que operações: uma tem de sair vazia

	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		claimed = make(chan string, workers)
		release = make(chan struct{})
		errs    = make([]error, workers)
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = store.RunInTx(context.Background(), func(ctx context.Context, tx ports.Tx) error {
				pending, found, err := tx.Transactions().ClaimDueReference(ctx, now)
				if err != nil {
					return err
				}
				if found {
					claimed <- pending.TransactionID
				}
				// Segura o lock até todos terem tentado: sem isso, o primeiro
				// commit liberaria a linha e o teste não mediria exclusão.
				<-release
				return nil
			})
		}(i)
	}

	close(start)

	// Exatamente as 3 operações existentes devem ser reservadas.
	got := make(map[string]int, len(ids))
	for i := 0; i < len(ids); i++ {
		select {
		case id := <-claimed:
			got[id]++
		case <-time.After(15 * time.Second):
			t.Fatalf("esperava %d reservas, recebi %d (travou)", len(ids), len(got))
		}
	}
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	if len(got) != len(ids) {
		t.Fatalf("operações distintas reservadas = %d, quer %d", len(got), len(ids))
	}
	for _, id := range ids {
		if got[id] != 1 {
			t.Fatalf("operação %s reservada %d vez(es), quer exatamente 1", id, got[id])
		}
	}
}

// TestClaimDueReferenceOutsideTransactionDoesNotHoldTheLock documenta o
// contrato: fora de uma transação o lock não sobrevive ao statement. O teste
// existe para que a exigência esteja escrita em código, e não só no comentário.
func TestClaimDueReferenceOutsideTransactionDoesNotHoldTheLock(t *testing.T) {
	store := testsupport.NewStore(t)
	const walletID = "00000000-0000-7000-8000-0000000000d5"

	insertWallet(t, store, walletID, "player-pending-5", "1000.00")
	now := time.Now().UTC()
	past := now.Add(-time.Minute)

	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000a0101", walletID, "ext-old", "key-old", "10.00", "ext-bet-x", past),
		past)

	ctx := context.Background()
	// Em autocommit: a linha é devolvida, mas o lock cai ao fim do statement —
	// logo a MESMA operação ainda aparece como vencida para quem consultar.
	first, found, err := store.Transactions().ClaimDueReference(ctx, now)
	if err != nil {
		t.Fatalf("ClaimDueReference em autocommit: %v", err)
	}
	if !found {
		t.Fatal("a operação vencida deveria aparecer")
	}

	second, found, err := store.Transactions().ClaimDueReference(ctx, now)
	if err != nil {
		t.Fatalf("segundo ClaimDueReference: %v", err)
	}
	if !found || second.TransactionID != first.TransactionID {
		t.Fatalf("em autocommit o lock não deveria sobreviver: segunda reserva = %+v (found=%v)", second, found)
	}
}

// TestClaimDueReferenceCommittedWithoutOutcomeLeavesTheRowDue documenta a
// armadilha central do desenho: a reserva NÃO é durável por si só.
//
// O lock termina no commit e o worker de referências não usa lease (a tentativa
// inteira cabe numa transação). Logo, uma reserva confirmada SEM gravar desfecho
// devolve a operação à disputa — o que é CORRETO e desejado, porque é isso que
// garante que nunca exista estado "reservado e indefinido". Esse é precisamente
// o estado que exigiria recuperação de reserva abandonada, e é por isso que a
// outbox (efeito externo, fora da transação) precisa de lease e a resolução de
// referência (trabalho local, dentro da transação) não.
//
// A consequência para quem implementa: reservar e gravar o desfecho têm de ser o
// MESMO commit. Um worker que reserve e processe fora da transação estaria
// repetindo o padrão da outbox sem o lease que ele exige.
func TestClaimDueReferenceCommittedWithoutOutcomeLeavesTheRowDue(t *testing.T) {
	store := testsupport.NewStore(t)
	const walletID = "00000000-0000-7000-8000-0000000000d6"

	insertWallet(t, store, walletID, "player-pending-6", "1000.00")
	now := time.Now().UTC()
	past := now.Add(-time.Minute)

	schedulePending(t, store,
		newPendingRefund(t, "00000000-0000-7000-8000-0000000a0201", walletID, "ext-reclaim", "key-reclaim", "10.00", "ext-bet-x", past),
		past)

	// Reserva SEM gravar desfecho: transação vazia, só para liberar o lock.
	if err := store.RunInTx(context.Background(), func(ctx context.Context, tx ports.Tx) error {
		_, _, err := tx.Transactions().ClaimDueReference(ctx, now)
		return err
	}); err != nil {
		t.Fatalf("reserva sem desfecho: %v", err)
	}

	// A operação continua vencida e é reservável de novo.
	reclaimed, found := claimNext(t, store, now, time.Hour)
	if !found {
		t.Fatal("a operação deveria continuar vencida após uma reserva sem desfecho")
	}
	if reclaimed.TransactionID != "00000000-0000-7000-8000-0000000a0201" {
		t.Fatalf("reservou %s, quer a mesma operação", reclaimed.TransactionID)
	}
}

// requireTestUUID falha o teste se o literal não for um UUID bem formado.
//
// Formato: 8-4-4-4-12 caracteres hexadecimais. É a mesma checagem que o
// PostgreSQL faria (SQLSTATE 22P02), mas feita AQUI — onde a mensagem aponta o
// literal e o motivo, em vez de sugerir falha do repositório.
func requireTestUUID(t *testing.T, what, id string) {
	t.Helper()

	if len(id) != 36 {
		t.Fatalf("%s inválido no teste: %q tem %d caracteres; o formato UUID exige 36", what, id, len(id))
	}
	for i, r := range id {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				t.Fatalf("%s inválido no teste: %q deveria ter '-' na posição %d", what, id, i)
			}
		default:
			isHex := (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
			if !isHex {
				t.Fatalf("%s inválido no teste: %q tem caractere não hexadecimal na posição %d", what, id, i)
			}
		}
	}
}
