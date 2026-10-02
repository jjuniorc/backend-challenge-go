package httpapi

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/jjuniorc/backend-challenge-go/internal/metrics"
)

// RouteUnmatched é o rótulo de requisição que não casou com nenhuma rota.
const RouteUnmatched = "unmatched"

// RouteMetrics é o caminho do próprio endpoint de métricas.
const RouteMetrics = "/metrics"

// MetricsMiddleware registra status e latência de cada requisição.
//
// Dois cuidados que definem a utilidade da série:
//
//   - O rótulo é a ROTA DO TEMPLATE (c.FullPath(), como "/wallets/:walletId"), e
//     não o caminho concreto. Com o caminho concreto, cada UUID consultado viraria
//     uma série nova e a cardinalidade cresceria sem limite — além de permitir que
//     qualquer cliente inflasse a cardinalidade mandando caminhos aleatórios.
//
//   - O próprio /metrics fica de fora. Raspagem não é tráfego de negócio, e
//     contá-la misturaria a latência do scrape com a latência do provedor.
//
// A latência medida inclui o processamento financeiro inteiro (transação SQL,
// locks, outbox), porque o middleware envolve o handler: é o tempo que o
// provedor sente, não apenas o tempo de parsing.
func MetricsMiddleware(m *metrics.Metrics) gin.HandlerFunc {
	// Sem métricas, o middleware é transparente em vez de nil: mantém o roteador
	// com a mesma forma quando elas estão desligadas.
	if m == nil {
		return func(c *gin.Context) { c.Next() }
	}

	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := routeLabel(c)
		if route == RouteMetrics {
			return
		}
		m.RecordHTTP(c.Request.Method, route, c.Writer.Status(), time.Since(start))
	}
}

// routeLabel devolve a rota de template da requisição.
//
// FullPath() é vazio quando nenhuma rota casou (404 por caminho inexistente);
// nesse caso o rótulo é RouteUnmatched, e não o caminho recebido — um caminho
// arbitrário vindo do cliente é justamente o que não pode virar rótulo.
func routeLabel(c *gin.Context) string {
	if path := c.FullPath(); path != "" {
		return path
	}
	return RouteUnmatched
}
