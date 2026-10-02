//go:build integration

package testsupport

import (
	"context"
	"testing"
	"time"
)

// QueryOutbox lê os campos de auditoria de um evento da outbox: tentativas,
// último erro e próximo agendamento. Usado pelos testes de mensageria para
// verificar backoff e reagendamento.
func QueryOutbox(ctx context.Context, t *testing.T, eventID string, attempts *int, lastError *string, nextAttempt **time.Time) error {
	t.Helper()

	conn := connect(t)
	defer func() { _ = conn.Close(context.Background()) }()

	var (
		attemptsValue    int
		lastErrorValue   *string
		nextAttemptValue *time.Time
	)
	err := conn.QueryRow(ctx, `
SELECT attempts, last_error, next_attempt_at
  FROM outbox_events
 WHERE event_id = $1::uuid`, eventID).Scan(&attemptsValue, &lastErrorValue, &nextAttemptValue)
	if err != nil {
		return err
	}

	if attempts != nil {
		*attempts = attemptsValue
	}
	if lastError != nil {
		if lastErrorValue == nil {
			*lastError = ""
		} else {
			*lastError = *lastErrorValue
		}
	}
	if nextAttempt != nil {
		*nextAttempt = nextAttemptValue
	}
	return nil
}
