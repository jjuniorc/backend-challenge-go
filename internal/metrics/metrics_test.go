package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// gather achatá o registry em um mapa "nome{rótulos}" → valor.
//
// Comparar por GATHER, e não por texto esperado, evita acoplar o teste à
// formatação da exposição (ordem de séries, HELP, TYPE): o que interessa é o
// número por série, e é isso que se afirma.
func gather(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	values := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			key := family.GetName()
			if labels := metric.GetLabel(); len(labels) > 0 {
				parts := make([]string, 0, len(labels))
				for _, label := range labels {
					parts = append(parts, label.GetName()+"="+label.GetValue())
				}
				key += "{" + strings.Join(parts, ",") + "}"
			}

			switch {
			case metric.GetCounter() != nil:
				values[key] = metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				values[key] = metric.GetGauge().GetValue()
			case metric.GetHistogram() != nil:
				values[key] = float64(metric.GetHistogram().GetSampleCount())
			}
		}
	}
	return values
}

func requireValue(t *testing.T, values map[string]float64, key string, want float64) {
	t.Helper()
	got, ok := values[key]
	if !ok {
		t.Fatalf("série %s ausente; presentes: %v", key, keysOf(values))
	}
	if got != want {
		t.Fatalf("%s = %v, quer %v", key, got, want)
	}
}

func keysOf(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

// ── contadores instrumentados ────────────────────────────────────────

func TestRecordHTTPCountsByRouteAndStatus(t *testing.T) {
	m := New()

	m.RecordHTTP("POST", "/wagering/transactions", http.StatusCreated, 12*time.Millisecond)
	m.RecordHTTP("POST", "/wagering/transactions", http.StatusCreated, 8*time.Millisecond)
	m.RecordHTTP("POST", "/wagering/transactions", http.StatusUnprocessableEntity, 3*time.Millisecond)
	m.RecordHTTP("GET", "/wallets/:walletId", http.StatusOK, time.Millisecond)

	values := gather(t, m.Registry())

	requireValue(t, values,
		"wager_http_requests_total{method=POST,route=/wagering/transactions,status=201}", 2)
	requireValue(t, values,
		"wager_http_requests_total{method=POST,route=/wagering/transactions,status=422}", 1)
	requireValue(t, values,
		"wager_http_requests_total{method=GET,route=/wallets/:walletId,status=200}", 1)

	// A latência conta as observações de cada rota, separadas por rótulo.
	requireValue(t, values, "wager_http_request_duration_seconds{method=POST,route=/wagering/transactions}", 3)
	requireValue(t, values, "wager_http_request_duration_seconds{method=GET,route=/wallets/:walletId}", 1)
}

func TestRecordReplayAndDivergence(t *testing.T) {
	m := New()

	m.RecordReplay()
	m.RecordReplay()
	m.RecordDivergence()

	values := gather(t, m.Registry())
	requireValue(t, values, "wager_idempotent_replays_total", 2)
	requireValue(t, values, "wager_reconciliation_divergences_total", 1)
}

// ── coletores derivados ──────────────────────────────────────────────

func TestLabeledCounterFuncReflectsChangesBetweenScrapes(t *testing.T) {
	m := New()

	counters := map[string]float64{"processed": 0, "retried": 0}
	var mu sync.Mutex

	if err := m.LabeledCounterFunc("consumer_events_total",
		"Eventos do consumidor por desfecho.", "outcome",
		func() map[string]float64 {
			mu.Lock()
			defer mu.Unlock()
			// Cópia: o coletor não deve enxergar mutação concorrente do mapa.
			snapshot := make(map[string]float64, len(counters))
			for key, value := range counters {
				snapshot[key] = value
			}
			return snapshot
		}); err != nil {
		t.Fatalf("LabeledCounterFunc: %v", err)
	}

	requireValue(t, gather(t, m.Registry()), "wager_consumer_events_total{outcome=processed}", 0)

	mu.Lock()
	counters["processed"] = 3
	counters["retried"] = 1
	mu.Unlock()

	values := gather(t, m.Registry())
	requireValue(t, values, "wager_consumer_events_total{outcome=processed}", 3)
	requireValue(t, values, "wager_consumer_events_total{outcome=retried}", 1)
}

func TestLabeledGaugeFuncFromDatabaseLikeSource(t *testing.T) {
	m := New()

	byStatus := map[string]float64{"PROCESSED": 7, "REJECTED": 2, "PENDING_REFERENCE": 1}
	if err := m.LabeledGaugeFunc("transactions_by_status",
		"Operacoes por estado, como estao no banco.", "status",
		func() (map[string]float64, error) { return byStatus, nil }); err != nil {
		t.Fatalf("LabeledGaugeFunc: %v", err)
	}

	values := gather(t, m.Registry())
	requireValue(t, values, "wager_transactions_by_status{status=PROCESSED}", 7)
	requireValue(t, values, "wager_transactions_by_status{status=REJECTED}", 2)
	requireValue(t, values, "wager_transactions_by_status{status=PENDING_REFERENCE}", 1)

	// Um estado deixa de existir: a série desaparece, e não fica com valor velho.
	delete(byStatus, "REJECTED")
	if _, ok := gather(t, m.Registry())["wager_transactions_by_status{status=REJECTED}"]; ok {
		t.Fatal("série de estado removido não deveria persistir")
	}
}

func TestGaugeFuncFailureOmitsTheSeriesInsteadOfReportingZero(t *testing.T) {
	m := New()

	failing := true
	if err := m.GaugeFunc("outbox_pending_events",
		"Eventos pendentes de publicacao.", func() (float64, error) {
			if failing {
				return 0, fmt.Errorf("banco indisponível")
			}
			return 4, nil
		}); err != nil {
		t.Fatalf("GaugeFunc: %v", err)
	}

	if _, ok := gather(t, m.Registry())["wager_outbox_pending_events"]; ok {
		t.Fatal("com erro, a série não deveria aparecer: zero seria lido como 'nada pendente'")
	}

	failing = false
	requireValue(t, gather(t, m.Registry()), "wager_outbox_pending_events", 4)
}

// ── exposição HTTP ───────────────────────────────────────────────────

func TestHandlerExposesRegisteredMetrics(t *testing.T) {
	m := New()
	m.RecordReplay()
	// Um CounterVec só passa a ser EXPOSTO depois de existir ao menos uma série
	// observada: sem esta requisição, wager_http_requests_total não aparece na
	// saída — nem a linha TYPE. É semântica do Prometheus, não deste pacote.
	m.RecordHTTP("GET", "/health/live", http.StatusOK, time.Millisecond)

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, quer 200", recorder.Code)
	}

	body, err := io.ReadAll(recorder.Body)
	if err != nil {
		t.Fatalf("lendo o corpo: %v", err)
	}
	text := string(body)

	for _, expected := range []string{
		"wager_idempotent_replays_total 1",
		"# TYPE wager_http_requests_total counter",
		`wager_http_requests_total{method="GET",route="/health/live",status="200"} 1`,
		// Coletores padrão do processo, úteis para diagnosticar vazamento de
		// goroutine nos workers.
		"go_goroutines",
	} {
		if !strings.Contains(text, expected) {
			t.Fatalf("exposição não contém %q", expected)
		}
	}
}

func TestRegistriesAreIsolatedBetweenInstances(t *testing.T) {
	first := New()
	second := New()

	first.RecordReplay()

	// O registry é próprio de cada instância, e o isolamento se demonstra pelo
	// VALOR — não pela ausência da série.
	//
	// Um Counter simples é sempre exposto, mesmo em zero, então a segunda
	// instância TEM a série, valendo 0. Ausência de série só acontece para
	// CounterVec sem nenhuma observação, que é o caso do teste do handler.
	requireValue(t, gather(t, second.Registry()), "wager_idempotent_replays_total", 0)
	requireValue(t, gather(t, first.Registry()), "wager_idempotent_replays_total", 1)
}
