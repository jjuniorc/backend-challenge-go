package httpapi

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	"github.com/jjuniorc/backend-challenge-go/internal/metrics"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// RouterDeps reúne as dependências do roteador.
type RouterDeps struct {
	Handler       *Handler
	Authenticator ports.Authenticator
	IDs           ports.IDGenerator
	Metrics       *metrics.Metrics
	Logger        *slog.Logger
}

// NewRouter monta o roteador.
//
// Ficam FORA do grupo autenticado, de propósito:
//
//	/health/live, /health/ready -> o orquestrador faz a probe antes de haver
//	                               credencial de provedor para apresentar;
//	/metrics                    -> o raspador do Prometheus não carrega token de
//	                               provedor nem de serviço interno, e exigir
//	                               credencial para raspar significaria embutir
//	                               um cliente OIDC dentro do monitoramento.
//
// A contrapartida é conhecida e vai no ARCHITECTURE.md: /metrics expõe volume de
// negócio (operações por estado, fila pendente se derivada). Em produção o certo
// é restringir por REDE — o endpoint escutando em porta interna, ou um NetworkPolicy
// liberando apenas o raspador — e não por token da aplicação.
//
// Todo o resto exige credencial válida e o papel correspondente:
//
//	internal -> operações de carteira (abertura, leitura, ledger, reconciliação)
//	provider -> envio e consulta das próprias operações
func NewRouter(deps RouterDeps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	// O middleware de métricas vem primeiro: ele envolve autenticação e handler,
	// então a latência inclui o custo de validar o token — que é parte do tempo
	// que o provedor espera.
	router.Use(MetricsMiddleware(deps.Metrics))
	router.Use(Recovery(deps.Logger))
	router.Use(RequestContext(deps.IDs))
	router.Use(AccessLog(deps.Logger))

	health := router.Group("/health")
	health.GET("/live", deps.Handler.Live)
	health.GET("/ready", deps.Handler.Ready)

	if deps.Metrics != nil {
		router.GET(RouteMetrics, gin.WrapH(deps.Metrics.Handler()))
	}

	authenticated := router.Group("/")
	authenticated.Use(Authenticate(deps.Authenticator))

	internal := authenticated.Group("/")
	internal.Use(RequireRole(ports.RoleInternal))
	internal.POST("/wallets", deps.Handler.OpenWallet)
	internal.GET("/wallets/:walletId", deps.Handler.GetWallet)
	internal.GET("/wallets/:walletId/ledger", deps.Handler.GetLedger)
	internal.POST("/wallets/:walletId/reconciliation", deps.Handler.Reconcile)

	provider := authenticated.Group("/")
	provider.Use(RequireRole(ports.RoleProvider))
	provider.POST("/wagering/transactions", deps.Handler.ProcessTransaction)
	provider.GET("/wagering/transactions/:transactionId", deps.Handler.GetTransaction)
	provider.GET("/providers/:providerId/wagering/transactions/:externalTransactionId", deps.Handler.GetTransactionByExternalID)

	return router
}
