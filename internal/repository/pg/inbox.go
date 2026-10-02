package pg

import (
	"context"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

type inboxRepo struct{ q querier }

func (r *inboxRepo) Insert(ctx context.Context, m ports.InboxMessage) (bool, error) {
	tag, err := r.q.Exec(ctx, `
INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, transaction_id)
VALUES ($1, $2, $3, $4, $5::uuid)
ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		m.ConsumerName, m.MessageID, m.PayloadHash, m.ReceivedAt, nullableString(m.TransactionID))
	if err != nil {
		return false, translate(err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *inboxRepo) Get(ctx context.Context, consumerName, messageID string) (ports.InboxMessage, error) {
	var (
		msg           ports.InboxMessage
		transactionID string
		completedAt   *time.Time
	)
	err := r.q.QueryRow(ctx, `
SELECT consumer_name, message_id, payload_hash, received_at,
       COALESCE(transaction_id::text, ''), completed_at
  FROM inbox_messages
 WHERE consumer_name = $1 AND message_id = $2`, consumerName, messageID).
		Scan(&msg.ConsumerName, &msg.MessageID, &msg.PayloadHash, &msg.ReceivedAt, &transactionID, &completedAt)
	if err != nil {
		return ports.InboxMessage{}, translate(err)
	}
	msg.TransactionID = transactionID
	msg.Completed = completedAt != nil
	return msg, nil
}

func (r *inboxRepo) Complete(ctx context.Context, consumerName, messageID, transactionID string) error {
	tag, err := r.q.Exec(ctx, `
UPDATE inbox_messages
   SET completed_at = now(),
       transaction_id = COALESCE($3::uuid, transaction_id)
 WHERE consumer_name = $1 AND message_id = $2`,
		consumerName, messageID, nullableString(transactionID))
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrNotFound
	}
	return nil
}

var _ ports.InboxRepository = (*inboxRepo)(nil)
