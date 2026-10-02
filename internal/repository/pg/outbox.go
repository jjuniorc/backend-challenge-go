package pg

import (
	"context"

	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

type outboxRepo struct{ q querier }

// Insert grava o evento pendente de publicação.
//
// ON CONFLICT (event_id) DO NOTHING torna a operação idempotente: uma
// republicação ou uma retomada após falha preserva o mesmo eventId e não cria
// registro duplicado. O payload é o envelope completo; event_type, event_version
// e aggregate_id ficam denormalizados para consulta e roteamento.
func (r *outboxRepo) Insert(ctx context.Context, e events.Envelope) error {
	payload, err := e.Payload()
	if err != nil {
		return err
	}
	_, err = r.q.Exec(ctx, `
INSERT INTO outbox_events (
    event_id, aggregate_id, event_type, event_version, payload,
    occurred_at, next_attempt_at, created_at
) VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6, now(), now())
ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.AggregateID, string(e.EventType), e.Version, string(payload), e.OccurredAt)
	return translate(err)
}

var _ ports.OutboxRepository = (*outboxRepo)(nil)

// claimBatchQuery reserva um lote de eventos pendentes.
//
// Três garantias em uma consulta:
//
//  1. FOR UPDATE SKIP LOCKED: dois publicadores que disputam o mesmo lote
//     recebem conjuntos DISJUNTOS — o segundo não espera nem recebe o que o
//     primeiro já reservou. É o que permite múltiplos publishers.
//  2. locked_until vencido volta a ser elegível: se uma instância cair entre o
//     commit da transação de negócio e a publicação, a reserva expira e outra
//     instância assume o registro.
//  3. next_attempt_at <= now: respeita o backoff de falhas anteriores.
const claimBatchQuery = `
WITH claimed AS (
    SELECT id
      FROM outbox_events
     WHERE published_at IS NULL
       AND next_attempt_at <= $1
       AND (locked_until IS NULL OR locked_until < $1)
     ORDER BY next_attempt_at, id
     LIMIT $2
     FOR UPDATE SKIP LOCKED
)
UPDATE outbox_events o
   SET locked_until = $3, locked_by = $4
  FROM claimed
 WHERE o.id = claimed.id
RETURNING o.id, o.event_id::text, o.aggregate_id, o.event_type,
          o.event_version, o.payload, o.occurred_at, o.attempts`

// ClaimBatch reserva até Limit eventos pendentes pelo período de Lease.
func (r *outboxRepo) ClaimBatch(ctx context.Context, req ports.ClaimRequest) ([]ports.OutboxRecord, error) {
	rows, err := r.q.Query(ctx, claimBatchQuery,
		req.Now, req.Limit, req.Now.Add(req.Lease), req.ClaimerID)
	if err != nil {
		return nil, translate(err)
	}
	defer rows.Close()

	var records []ports.OutboxRecord
	for rows.Next() {
		var (
			record  ports.OutboxRecord
			payload string
		)
		// O payload é JSONB; ler como string é inequívoco e evita depender do
		// codec de JSON do driver.
		if err := rows.Scan(&record.ID, &record.EventID, &record.AggregateID,
			&record.EventType, &record.Version, &payload,
			&record.OccurredAt, &record.Attempts); err != nil {
			return nil, translate(err)
		}
		record.Payload = []byte(payload)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, translate(err)
	}
	return records, nil
}

// MarkPublished confirma a publicação.
//
// O lock é liberado junto com published_at porque o schema exige que um evento
// publicado não mantenha reserva ativa
// (outbox_published_releases_lock).
func (r *outboxRepo) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.q.Exec(ctx, `
UPDATE outbox_events
   SET published_at = now(), locked_until = NULL, locked_by = NULL, last_error = NULL
 WHERE id = ANY($1)`, ids)
	return translate(err)
}

// MarkFailed reagenda o evento e libera a reserva para outra tentativa.
func (r *outboxRepo) MarkFailed(ctx context.Context, id int64, failure ports.OutboxFailure) error {
	_, err := r.q.Exec(ctx, `
UPDATE outbox_events
   SET attempts = $2, next_attempt_at = $3, last_error = $4,
       locked_until = NULL, locked_by = NULL
 WHERE id = $1`, id, failure.Attempts, failure.NextAttemptAt, failure.LastError)
	return translate(err)
}

// CountPending devolve quantos eventos aguardam publicação.
func (r *outboxRepo) CountPending(ctx context.Context) (int64, error) {
	var count int64
	if err := r.q.QueryRow(ctx,
		`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&count); err != nil {
		return 0, translate(err)
	}
	return count, nil
}
