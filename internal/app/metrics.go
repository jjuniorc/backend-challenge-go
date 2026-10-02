package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/metrics"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/worker"
)

// metricsReadTimeout é o prazo das consultas feitas DURANTE a raspagem.
//
// Existe porque a raspagem bloqueia um handler HTTP do Prometheus: uma consulta
// sem prazo prenderia o raspador se o banco estivesse lento, e a indisponibilidade
// das métricas passaria a parecer indisponibilidade da aplicação.
const metricsReadTimeout = 5 * time.Second

// provideMetrics monta as métricas e registra os coletores derivados.
//
// # Por que derivar em vez de contar
//
// Estado das operações e conflitos por código de falha vivem no BANCO. Derivá-los
// de uma consulta significa que a métrica não pode divergir do sistema: um
// contador em memória começaria em zero a cada reinício e não teria como ser
// conferido. O preço é uma consulta agregada por raspagem — documentado, com o
// caminho de materialização caso fique caro.
//
// Já os contadores dos workers (retries, DLQ, tentativas de referência) já existem
// em memória e são apenas lidos: assim o Prometheus não entra dentro dos laços de
// processamento.
func provideMetrics(
	store ports.Store,
	consumer *worker.SQSConsumer,
	reference *worker.ReferenceWorker,
	cfg config.Config,
	logger *slog.Logger,
) (*metrics.Metrics, error) {
	m := metrics.New()

	if err := m.LabeledGaugeFunc("transactions_by_status",
		"Operacoes por estado, como estao no banco.", "status",
		func() (map[string]float64, error) {
			ctx, cancel := context.WithTimeout(context.Background(), metricsReadTimeout)
			defer cancel()

			counts, err := store.Transactions().CountByStatus(ctx)
			if err != nil {
				logger.Error("métricas: contando operações por estado", "err", err)
				return nil, err
			}
			return toFloats(counts), nil
		}); err != nil {
		return nil, fmt.Errorf("registrando wager_transactions_by_status: %w", err)
	}

	if err := m.LabeledGaugeFunc("transactions_by_failure_code",
		"Operacoes com falha registrada, por codigo.", "code",
		func() (map[string]float64, error) {
			ctx, cancel := context.WithTimeout(context.Background(), metricsReadTimeout)
			defer cancel()

			counts, err := store.Transactions().CountByFailureCode(ctx)
			if err != nil {
				logger.Error("métricas: contando operações por código de falha", "err", err)
				return nil, err
			}
			return toFloats(counts), nil
		}); err != nil {
		return nil, fmt.Errorf("registrando wager_transactions_by_failure_code: %w", err)
	}

	// Atraso da outbox: o número que diz se a publicação está acompanhando.
	if err := m.GaugeFunc("outbox_pending_events",
		"Eventos aguardando publicacao na outbox.", func() (float64, error) {
			ctx, cancel := context.WithTimeout(context.Background(), metricsReadTimeout)
			defer cancel()

			pending, err := store.Outbox().CountPending(ctx)
			if err != nil {
				logger.Error("métricas: contando eventos pendentes da outbox", "err", err)
				return 0, err
			}
			return float64(pending), nil
		}); err != nil {
		return nil, fmt.Errorf("registrando wager_outbox_pending_events: %w", err)
	}

	if cfg.Consumer.Enabled {
		if err := m.LabeledCounterFunc("consumer_events_total",
			"Eventos do consumidor por desfecho.", "outcome",
			func() map[string]float64 {
				stats := consumer.Stats()
				return map[string]float64{
					"received":      float64(stats.Received),
					"processed":     float64(stats.Processed),
					"retried":       float64(stats.Retried),
					"dead_lettered": float64(stats.DeadLettered),
					"invalid":       float64(stats.InvalidMessages),
				}
			}); err != nil {
			return nil, fmt.Errorf("registrando wager_consumer_events_total: %w", err)
		}
	}

	if cfg.Reference.Enabled {
		if err := m.LabeledCounterFunc("reference_resolutions_total",
			"Tentativas de retomada de referencia por desfecho.", "outcome",
			func() map[string]float64 {
				stats := reference.Stats()
				return map[string]float64{
					"resolved":    float64(stats.Resolved),
					"rescheduled": float64(stats.Rescheduled),
					"rejected":    float64(stats.Rejected),
					"exhausted":   float64(stats.Exhausted),
					"errors":      float64(stats.Errors),
				}
			}); err != nil {
			return nil, fmt.Errorf("registrando wager_reference_resolutions_total: %w", err)
		}
	}

	logger.Info("métricas registradas",
		"consumerEnabled", cfg.Consumer.Enabled,
		"referenceEnabled", cfg.Reference.Enabled,
	)
	return m, nil
}

func toFloats(counts map[string]int64) map[string]float64 {
	values := make(map[string]float64, len(counts))
	for key, count := range counts {
		values[key] = float64(count)
	}
	return values
}
