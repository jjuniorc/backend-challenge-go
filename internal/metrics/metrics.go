// Package metrics expõe as métricas Prometheus da aplicação.
//
// # Duas origens, por decisão
//
//   - CONTADORES INSTRUMENTADOS: eventos que não deixam rastro persistente — a
//     repetição idempotente não cria linha, a divergência de reconciliação não é
//     gravada. Só quem atende a requisição sabe que aconteceram.
//
//   - DERIVADOS SOB DEMANDA: o que já existe como estado ou como contador em
//     memória. O estado de uma operação vive no banco, que é a fonte da verdade:
//     um contador em memória começaria em zero a cada reinício e poderia divergir
//     do real, sem forma de reconciliar. Os contadores dos workers já existem
//     (ConsumerStats, ReferenceStats) e são lidos na raspagem, o que evita
//     espalhar dependência de Prometheus dentro dos laços.
//
// O registry é PRÓPRIO, e não o global do Prometheus: o global é estado
// compartilhado do processo, o que torna testes não isolados entre si e permite
// registro duplicado por engano em qualquer ponto do programa.
//
// # Nota de exposição
//
//   - um COUNTER simples é sempre exposto, mesmo valendo zero;
//   - um COUNTERVEC sem nenhuma série observada NÃO é exposto — nem a linha TYPE.
//
// É semântica do Prometheus, não escolha deste pacote. A consequência prática é
// que uma métrica com rótulo só passa a existir depois do primeiro evento, e a
// ausência da série não distingue "sem tráfego" de "métrica quebrada". Quando o
// zero precisa existir ANTES do primeiro evento, o caminho é um coletor derivado
// (LabeledCounterFunc), que devolve as séries conhecidas mesmo em zero.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixa todas as métricas da aplicação.
const Namespace = "wager"

// Metrics reúne o registry e os coletores instrumentados.
type Metrics struct {
	registry *prometheus.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec
	replays      prometheus.Counter
	divergences  prometheus.Counter
}

// New cria o conjunto de métricas com registry próprio.
func New() *Metrics {
	registry := prometheus.NewRegistry()

	m := &Metrics{
		registry: registry,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "http_requests_total",
			Help:      "Requisicoes HTTP por metodo, rota e codigo de resposta.",
		}, []string{"method", "route", "status"}),

		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "http_request_duration_seconds",
			Help:      "Latencia das requisicoes HTTP, incluindo o processamento financeiro.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),

		replays: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "idempotent_replays_total",
			Help:      "Requisicoes respondidas como repeticao idempotente (mesma chave e mesmo conteudo).",
		}),

		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "reconciliation_divergences_total",
			Help:      "Reconciliacoes cujo saldo armazenado divergiu da soma do ledger.",
		}),
	}

	registry.MustRegister(
		m.httpRequests,
		m.httpDuration,
		m.replays,
		m.divergences,
		// Coletores padrão do processo: goroutines, memória, CPU. Baratos e úteis
		// para diagnosticar vazamento de goroutine nos workers.
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Registry devolve o registry, para registro de coletores derivados e testes.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler devolve o handler HTTP de exposição das métricas.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordHTTP registra uma requisição concluída.
//
// A latência inclui o processamento financeiro inteiro (transação SQL, locks e
// outbox), porque é medida no middleware: é o número que o provedor sente.
func (m *Metrics) RecordHTTP(method, route string, status int, duration time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}

// RecordReplay conta uma resposta de repetição idempotente.
func (m *Metrics) RecordReplay() { m.replays.Inc() }

// RecordDivergence conta uma divergência de reconciliação.
func (m *Metrics) RecordDivergence() { m.divergences.Inc() }

// ─────────────────────────────────────────────────────────────
// Coletores derivados (calculados na raspagem)
// ─────────────────────────────────────────────────────────────

// CounterFunc registra um valor CUMULATIVO sem rótulo.
func (m *Metrics) CounterFunc(name, help string, fn func() float64) error {
	desc := prometheus.NewDesc(prometheus.BuildFQName(Namespace, "", name), help, nil, nil)
	return m.registry.Register(&noLabelCollector{desc: desc, valueType: prometheus.CounterValue, fn: fn})
}

// LabeledCounterFunc registra valores CUMULATIVOS por rótulo.
//
// O produtor devolve o mapa rótulo→valor ACUMULADO a cada raspagem. Serve para
// expor contadores que já existem em memória sem instrumentar cada ponto de
// decisão.
func (m *Metrics) LabeledCounterFunc(name, help, label string, fn func() map[string]float64) error {
	desc := prometheus.NewDesc(prometheus.BuildFQName(Namespace, "", name), help, []string{label}, nil)
	return m.registry.Register(&labeledCollector{desc: desc, valueType: prometheus.CounterValue, fn: fn})
}

// LabeledGaugeFunc registra INSTANTÂNEOS por rótulo, com erro possível.
//
// É a forma usada para o que vive no banco: a consulta pode falhar, e nesse caso
// a série simplesmente não aparece na raspagem — melhor do que expor zero, que
// seria lido como "nada aconteceu".
func (m *Metrics) LabeledGaugeFunc(name, help, label string, fn func() (map[string]float64, error)) error {
	desc := prometheus.NewDesc(prometheus.BuildFQName(Namespace, "", name), help, []string{label}, nil)
	return m.registry.Register(&labeledGaugeCollector{desc: desc, fn: fn})
}

// GaugeFunc registra um INSTANTÂNEO sem rótulo, com erro possível.
func (m *Metrics) GaugeFunc(name, help string, fn func() (float64, error)) error {
	desc := prometheus.NewDesc(prometheus.BuildFQName(Namespace, "", name), help, nil, nil)
	return m.registry.Register(&noLabelGaugeCollector{desc: desc, fn: fn})
}

type noLabelCollector struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	fn        func() float64
}

func (c *noLabelCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *noLabelCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(c.desc, c.valueType, c.fn())
}

type noLabelGaugeCollector struct {
	desc *prometheus.Desc
	fn   func() (float64, error)
}

func (c *noLabelGaugeCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *noLabelGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	value, err := c.fn()
	if err != nil {
		// Sem valor: a série não aparece. Ver a nota de exposição no topo.
		return
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, value)
}

type labeledCollector struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	fn        func() map[string]float64
}

func (c *labeledCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *labeledCollector) Collect(ch chan<- prometheus.Metric) {
	for labelValue, value := range c.fn() {
		ch <- prometheus.MustNewConstMetric(c.desc, c.valueType, value, labelValue)
	}
}

type labeledGaugeCollector struct {
	desc *prometheus.Desc
	fn   func() (map[string]float64, error)
}

func (c *labeledGaugeCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *labeledGaugeCollector) Collect(ch chan<- prometheus.Metric) {
	values, err := c.fn()
	if err != nil {
		return
	}
	for labelValue, value := range values {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, value, labelValue)
	}
}
