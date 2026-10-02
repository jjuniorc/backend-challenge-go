package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/jjuniorc/backend-challenge-go/internal/metrics"
)

// newMetricsTestRouter monta um roteador mínimo com o middleware e rotas que
// reproduzem os padrões reais: parâmetro de caminho, rota inexistente e erro.
func newMetricsTestRouter(m *metrics.Metrics) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(MetricsMiddleware(m))

	router.GET("/wallets/:walletId", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.POST("/wagering/transactions", func(c *gin.Context) { c.Status(http.StatusCreated) })
	router.GET("/boom", func(c *gin.Context) { c.Status(http.StatusInternalServerError) })
	router.GET(RouteMetrics, gin.WrapH(m.Handler()))

	return router
}

func serve(router *gin.Engine, method, path string) int {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	return recorder.Code
}

func exposition(t *testing.T, router *gin.Engine) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, RouteMetrics, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, quer 200", RouteMetrics, recorder.Code)
	}
	return recorder.Body.String()
}

// TestMetricsMiddlewareLabelsByRouteTemplate é o teste que protege a
// CARDINALIDADE: dois UUIDs diferentes têm de cair na MESMA série, senão cada
// carteira consultada criaria uma série nova e o servidor de métricas cresceria
// sem limite.
func TestMetricsMiddlewareLabelsByRouteTemplate(t *testing.T) {
	m := metrics.New()
	router := newMetricsTestRouter(m)

	serve(router, http.MethodGet, "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37")
	serve(router, http.MethodGet, "/wallets/01a0fe52-4dec-7b08-8b4d-5eb2563a22f0")

	text := exposition(t, router)

	const expected = `wager_http_requests_total{method="GET",route="/wallets/:walletId",status="200"} 2`
	if !strings.Contains(text, expected) {
		t.Fatalf("exposição não contém %q:\n%s", expected, onlyWager(text))
	}
	// Nenhum dos identificadores concretos pode ter virado rótulo.
	for _, concrete := range []string{
		"0192f291-27dd-7d3f-8071-5f8685deef37",
		"01a0fe52-4dec-7b08-8b4d-5eb2563a22f0",
	} {
		if strings.Contains(text, concrete) {
			t.Fatalf("o identificador concreto %q virou rótulo (cardinalidade sem limite)", concrete)
		}
	}
}

func TestMetricsMiddlewareLabelsUnmatchedRequests(t *testing.T) {
	m := metrics.New()
	router := newMetricsTestRouter(m)

	if code := serve(router, http.MethodGet, "/nao-existe/abc"); code != http.StatusNotFound {
		t.Fatalf("status = %d, quer 404", code)
	}

	text := exposition(t, router)

	const expected = `wager_http_requests_total{method="GET",route="unmatched",status="404"} 1`
	if !strings.Contains(text, expected) {
		t.Fatalf("exposição não contém %q:\n%s", expected, onlyWager(text))
	}
	// O caminho recebido do cliente é justamente o que NÃO pode virar rótulo.
	if strings.Contains(text, "nao-existe") {
		t.Fatal("o caminho da requisição virou rótulo")
	}
}

func TestMetricsMiddlewareCountsErrorStatuses(t *testing.T) {
	m := metrics.New()
	router := newMetricsTestRouter(m)

	serve(router, http.MethodGet, "/boom")
	serve(router, http.MethodGet, "/boom")

	text := exposition(t, router)
	const expected = `wager_http_requests_total{method="GET",route="/boom",status="500"} 2`
	if !strings.Contains(text, expected) {
		t.Fatalf("exposição não contém %q:\n%s", expected, onlyWager(text))
	}
}

// TestMetricsMiddlewareSkipsItsOwnEndpoint evita que a raspagem se misture com o
// tráfego de negócio: a latência do scrape não é a latência do provedor.
func TestMetricsMiddlewareSkipsItsOwnEndpoint(t *testing.T) {
	m := metrics.New()
	router := newMetricsTestRouter(m)

	exposition(t, router)
	exposition(t, router)

	text := exposition(t, router)
	if strings.Contains(text, `route="/metrics"`) {
		t.Fatal("a própria raspagem não deveria ser contabilizada")
	}
}

func TestMetricsMiddlewareIsTransparentWithoutMetrics(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(MetricsMiddleware(nil))
	router.GET("/wallets/:walletId", func(c *gin.Context) { c.Status(http.StatusOK) })

	if code := serve(router, http.MethodGet, "/wallets/qualquer"); code != http.StatusOK {
		t.Fatalf("status = %d, quer 200 (sem métricas o middleware é transparente)", code)
	}
}

// onlyWager devolve só as linhas wager_* da exposição, para o erro ficar legível
// sem despejar os coletores de processo.
func onlyWager(text string) string {
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "wager_") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}
