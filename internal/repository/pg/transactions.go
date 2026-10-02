package pg

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

type transactionRepo struct{ q querier }

// transactionColumns normaliza a leitura para tipos simples do Go:
//   - UUID -> text;
//   - colunas opcionais -> COALESCE para string vazia;
//   - resulting_balance_minor -> COALESCE para 0, com presença decidida pelo
//     status (o schema garante NOT NULL se e somente se status = 'PROCESSED').
const transactionColumns = `
	id::text, origin, kind, status, wallet_id::text, player_id, currency, amount_minor,
	COALESCE(provider_id, ''), COALESCE(external_transaction_id, ''),
	COALESCE(idempotency_key, ''), COALESCE(payload_hash, ''),
	COALESCE(round_id, ''), COALESCE(game_id, ''),
	COALESCE(external_reference_id, ''), COALESCE(reference_transaction_id::text, ''),
	COALESCE(failure_code, ''), COALESCE(failure_message, ''),
	COALESCE(resulting_balance_minor, 0),
	occurred_at, created_at, updated_at`

func (r *transactionRepo) Insert(ctx context.Context, t wagertransaction.Transaction) error {
	_, err := r.q.Exec(ctx, `
INSERT INTO wager_transactions (
    id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
    provider_id, external_transaction_id, idempotency_key, payload_hash,
    round_id, game_id, external_reference_id, reference_transaction_id,
    failure_code, failure_message, resulting_balance_minor,
    occurred_at, created_at, updated_at
) VALUES (
    $1::uuid, $2, $3, $4, $5::uuid, $6, $7, $8,
    $9, $10, $11, $12,
    $13, $14, $15, $16::uuid,
    $17, $18, $19,
    $20, $21, $22
)`,
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()),
		t.WalletID(), t.PlayerID(), string(t.Currency()), t.Amount().Amount(),
		nullableString(t.ProviderID()), nullableString(t.ExternalTransactionID()),
		nullableString(t.IdempotencyKey()), nullableString(t.PayloadHash()),
		nullableString(t.RoundID()), nullableString(t.GameID()),
		nullableString(t.ExternalReferenceID()), nullableString(t.ReferenceTransactionID()),
		nullableString(t.FailureCode()), nullableString(t.FailureMessage()),
		nullableInt64(t.ResultBalance().Amount(), t.ResultBalance().IsValid()),
		t.OccurredAt(), t.CreatedAt(), t.UpdatedAt(),
	)
	return translate(err)
}

// Update altera apenas os campos mutáveis. Identidade, tipo, valor e hashes são
// imutáveis por desenho: uma transação nunca muda de valor nem de chave.
func (r *transactionRepo) Update(ctx context.Context, t wagertransaction.Transaction) error {
	tag, err := r.q.Exec(ctx, `
UPDATE wager_transactions
   SET status = $2,
       failure_code = $3,
       failure_message = $4,
       resulting_balance_minor = $5,
       reference_transaction_id = $6::uuid,
       updated_at = $7
 WHERE id = $1::uuid`,
		t.ID(), string(t.Status()),
		nullableString(t.FailureCode()), nullableString(t.FailureMessage()),
		nullableInt64(t.ResultBalance().Amount(), t.ResultBalance().IsValid()),
		nullableString(t.ReferenceTransactionID()), t.UpdatedAt())
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrNotFound
	}
	return nil
}

func (r *transactionRepo) GetByID(ctx context.Context, id string) (wagertransaction.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM wager_transactions WHERE id = $1::uuid`, id))
}

func (r *transactionRepo) FindByProviderAndIdempotencyKey(ctx context.Context, providerID, idempotencyKey string) (wagertransaction.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx,
		`SELECT `+transactionColumns+`
		   FROM wager_transactions
		  WHERE origin = 'EXTERNAL' AND provider_id = $1 AND idempotency_key = $2`,
		providerID, idempotencyKey))
}

func (r *transactionRepo) FindByProviderAndExternalID(ctx context.Context, providerID, externalID string) (wagertransaction.Transaction, error) {
	return scanTransaction(r.q.QueryRow(ctx,
		`SELECT `+transactionColumns+`
		   FROM wager_transactions
		  WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`,
		providerID, externalID))
}

func (r *transactionRepo) HasSuccessfulReversal(ctx context.Context, referenceTransactionID string) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM wager_transactions
     WHERE reference_transaction_id = $1::uuid
       AND status = 'PROCESSED'
       AND kind IN ('REFUND','ROLLBACK')
)`, referenceTransactionID).Scan(&exists)
	if err != nil {
		return false, translate(err)
	}
	return exists, nil
}

func (r *transactionRepo) ScheduleReferenceRetry(ctx context.Context, transactionID string, attempts int, nextAttemptAt time.Time) error {
	tag, err := r.q.Exec(ctx, `
UPDATE wager_transactions
   SET reference_attempts = $2, next_attempt_at = $3
 WHERE id = $1::uuid AND status = 'PENDING_REFERENCE'`,
		transactionID, attempts, nextAttemptAt)
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrNotFound
	}
	return nil
}

// claimDueReferenceQuery escolhe a próxima operação pendente a tentar.
//
// Escolhas:
//
//   - ORDER BY next_attempt_at, id: ordem determinística e estável. O vencimento
//     mais antigo primeiro evita que uma operação fique para trás enquanto
//     novas chegam; o id desempata.
//   - LIMIT 1: uma tentativa por transação. Cada chamada abre e fecha a sua, e
//     um lote maior apenas alongaria o lock.
//   - FOR UPDATE SKIP LOCKED: duas instâncias nunca pegam a mesma linha, e
//     nenhuma espera pela outra.
//
// O índice parcial wt_pending_reference_idx (next_attempt_at, id) WHERE status =
// 'PENDING_REFERENCE' cobre exatamente este filtro e esta ordenação.
const claimDueReferenceQuery = `
SELECT id::text, COALESCE(provider_id, ''), wallet_id::text, kind,
       COALESCE(external_reference_id, ''), reference_attempts,
       created_at, next_attempt_at
  FROM wager_transactions
 WHERE status = 'PENDING_REFERENCE'
   AND next_attempt_at IS NOT NULL
   AND next_attempt_at <= $1
 ORDER BY next_attempt_at, id
 LIMIT 1
 FOR UPDATE SKIP LOCKED`

// ClaimDueReference reserva a operação pendente mais antiga que já venceu.
func (r *transactionRepo) ClaimDueReference(ctx context.Context, now time.Time) (ports.PendingReference, bool, error) {
	var (
		pending     ports.PendingReference
		kind        string
		createdAt   time.Time
		nextAttempt time.Time
	)

	err := r.q.QueryRow(ctx, claimDueReferenceQuery, now).Scan(
		&pending.TransactionID, &pending.ProviderID, &pending.WalletID, &kind,
		&pending.ReferenceExternalID, &pending.Attempts, &createdAt, &nextAttempt,
	)
	// A ausência de linha é tratada ANTES do translate, e não por ele: aqui
	// "nada vencido" é o caso NORMAL (found=false), enquanto o resto do pacote
	// usa translate para converter erro do driver em sentinela de ports. Se a
	// ordem fosse invertida, o worker receberia ports.ErrNotFound toda vez que
	// não houvesse trabalho — e trataria o caso normal como falha.
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.PendingReference{}, false, nil
	}
	if err != nil {
		return ports.PendingReference{}, false, translate(err)
	}

	pending.Kind = wagertransaction.Kind(kind)
	pending.NextAttemptAt = nextAttempt
	pending.Age = now.Sub(createdAt)
	if pending.Age < 0 {
		// Relógio do host atrás do banco: idade negativa faria o TTL nunca
		// vencer, e a operação ficaria pendente para sempre.
		pending.Age = 0
	}
	return pending, true, nil
}

// CountByStatus conta as operações por estado.
//
// Consulta agregada feita na RASPAGEM de métricas. O custo é um group by sobre
// wager_transactions — barato no volume do desafio, e coberto pelo índice parcial
// quando o filtro é por estado pendente. Se a tabela crescer a ponto de a
// raspagem ficar cara, o caminho é materializar a contagem numa tabela de
// agregados atualizada no commit; fica registrado no ARCHITECTURE.md.
func (r *transactionRepo) CountByStatus(ctx context.Context) (map[string]int64, error) {
	return r.countByColumn(ctx, `SELECT status, count(*) FROM wager_transactions GROUP BY status`)
}

// CountByFailureCode conta as operações com falha registrada, por código.
func (r *transactionRepo) CountByFailureCode(ctx context.Context) (map[string]int64, error) {
	return r.countByColumn(ctx, `
SELECT failure_code, count(*)
  FROM wager_transactions
 WHERE failure_code IS NOT NULL
 GROUP BY failure_code`)
}

// countByColumn executa uma contagem agrupada por uma coluna textual.
func (r *transactionRepo) countByColumn(ctx context.Context, query string) (map[string]int64, error) {
	rows, err := r.q.Query(ctx, query)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var (
			key   string
			count int64
		)
		if err := rows.Scan(&key, &count); err != nil {
			return nil, translate(err)
		}
		counts[key] = count
	}
	if err := rows.Err(); err != nil {
		return nil, translate(err)
	}
	return counts, nil
}

func scanTransaction(row pgx.Row) (wagertransaction.Transaction, error) {
	var (
		id, origin, kind, status, walletID, playerID, currency string
		amountMinor                                            int64
		providerID, externalID, idempotencyKey, payloadHash    string
		roundID, gameID                                        string
		externalReferenceID, referenceTransactionID            string
		failureCode, failureMessage                            string
		resultingBalanceMinor                                  int64
		occurredAt, createdAt, updatedAt                       time.Time
	)
	if err := row.Scan(
		&id, &origin, &kind, &status, &walletID, &playerID, &currency, &amountMinor,
		&providerID, &externalID, &idempotencyKey, &payloadHash,
		&roundID, &gameID,
		&externalReferenceID, &referenceTransactionID,
		&failureCode, &failureMessage,
		&resultingBalanceMinor,
		&occurredAt, &createdAt, &updatedAt,
	); err != nil {
		return wagertransaction.Transaction{}, translate(err)
	}

	amount, err := money.NewMoney(amountMinor, money.Currency(currency))
	if err != nil {
		return wagertransaction.Transaction{}, err
	}

	// resulting_balance_minor só existe em PROCESSED (garantido pelo schema).
	var resultBalance money.Money
	if wagertransaction.Status(status) == wagertransaction.StatusProcessed {
		resultBalance, err = money.NewMoney(resultingBalanceMinor, money.Currency(currency))
		if err != nil {
			return wagertransaction.Transaction{}, err
		}
	}

	return wagertransaction.Rehydrate(wagertransaction.RehydrateParams{
		ID:                     id,
		Origin:                 wagertransaction.Origin(origin),
		Kind:                   wagertransaction.Kind(kind),
		Status:                 wagertransaction.Status(status),
		ProviderID:             providerID,
		ExternalTransactionID:  externalID,
		IdempotencyKey:         idempotencyKey,
		PayloadHash:            payloadHash,
		WalletID:               walletID,
		PlayerID:               playerID,
		RoundID:                roundID,
		GameID:                 gameID,
		Amount:                 amount,
		ExternalReferenceID:    externalReferenceID,
		ReferenceTransactionID: referenceTransactionID,
		FailureCode:            failureCode,
		FailureMessage:         failureMessage,
		ResultBalance:          resultBalance,
		OccurredAt:             occurredAt,
		CreatedAt:              createdAt,
		UpdatedAt:              updatedAt,
	})
}
