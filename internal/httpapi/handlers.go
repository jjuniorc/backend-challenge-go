package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/metrics"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// HandlerDeps reúne as dependências dos handlers.
type HandlerDeps struct {
	OpenWallet *usecase.OpenWallet
	Process    *usecase.ProcessTransaction
	GetWallet  *usecase.GetWallet
	GetLedger  *usecase.GetLedger
	GetTx      *usecase.GetTransaction
	Reconcile  *usecase.Reconcile
	Checkers   []ports.HealthChecker
	Metrics    *metrics.Metrics
	Logger     *slog.Logger
}

// Handler implementa os endpoints.
type Handler struct {
	deps HandlerDeps
}

// NewHandler cria o conjunto de handlers.
func NewHandler(deps HandlerDeps) *Handler { return &Handler{deps: deps} }

// recordReplay conta uma resposta de repetição idempotente.
//
// Existe porque este é o único desfecho que NÃO deixa rastro no banco: a
// repetição não cria linha em wager_transactions, então uma métrica derivada do
// estado persistido jamais a enxergaria. Só quem atendeu a requisição sabe.
func (h *Handler) recordReplay() {
	if h.deps.Metrics != nil {
		h.deps.Metrics.RecordReplay()
	}
}

// recordDivergence conta uma divergência de reconciliação.
//
// Mesmo motivo: a reconciliação é uma LEITURA e o resultado dela não é
// persistido. Sem contar aqui, uma carteira divergente só apareceria no log.
func (h *Handler) recordDivergence() {
	if h.deps.Metrics != nil {
		h.deps.Metrics.RecordDivergence()
	}
}

// ── saúde ────────────────────────────────────────────────────────────

// Live responde ao liveness probe do processo.
func (h *Handler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Ready responde ao readiness, verificando as dependências (PostgreSQL hoje;
// o broker entra junto com a mensageria).
func (h *Handler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	checks := gin.H{}
	ready := true
	for _, checker := range h.deps.Checkers {
		if err := checker.Check(ctx); err != nil {
			checks[checker.Name()] = "down"
			ready = false
			h.deps.Logger.Warn("readiness: dependência indisponível", "dependency", checker.Name(), "err", err)
			continue
		}
		checks[checker.Name()] = "up"
	}
	if !ready {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "checks": checks})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready", "checks": checks})
}

// ── carteira ─────────────────────────────────────────────────────────

// OpenWallet cria a carteira (operação interna).
func (h *Handler) OpenWallet(c *gin.Context) {
	var request openWalletRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, domainerr.Wrap(domainerr.KindInvalid, "INVALID_REQUEST_BODY", "corpo da requisição inválido", err))
		return
	}

	result, err := h.deps.OpenWallet.Execute(c.Request.Context(), usecase.OpenWalletCommand{
		PlayerID:       request.PlayerID,
		InitialBalance: request.InitialBalance,
		CorrelationID:  CorrelationIDFrom(c.Request.Context()),
	})
	if err != nil {
		writeError(c, err)
		return
	}

	wallet, err := h.deps.GetWallet.Execute(c.Request.Context(), usecase.GetWalletQuery{WalletID: result.WalletID})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, toWalletResponse(wallet))
}

// GetWallet devolve o estado atual da carteira.
func (h *Handler) GetWallet(c *gin.Context) {
	wallet, err := h.deps.GetWallet.Execute(c.Request.Context(), usecase.GetWalletQuery{
		WalletID: c.Param("walletId"),
	})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, toWalletResponse(wallet))
}

// GetLedger devolve uma página do ledger com cursor opaco.
func (h *Handler) GetLedger(c *gin.Context) {
	query := usecase.GetLedgerQuery{WalletID: c.Param("walletId")}

	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		cursor, err := decodeCursor(raw)
		if err != nil {
			writeError(c, err)
			return
		}
		query.After = &cursor
	}

	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			writeError(c, invalid("limit deve ser um inteiro positivo"))
			return
		}
		query.Limit = limit
	}

	view, err := h.deps.GetLedger.Execute(c.Request.Context(), query)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, toLedgerResponse(view))
}

// Reconcile compara o saldo armazenado com o reconstruído do ledger.
func (h *Handler) Reconcile(c *gin.Context) {
	view, err := h.deps.Reconcile.Execute(c.Request.Context(), usecase.ReconcileQuery{
		WalletID: c.Param("walletId"),
	})
	if err != nil {
		writeError(c, err)
		return
	}

	if !view.Consistent {
		h.deps.Logger.Error("reconciliação detectou divergência",
			"walletId", view.WalletID,
			"storedBalance", view.StoredBalance.String(),
			"calculatedBalance", view.CalculatedBalance.String(),
			"difference", view.Difference.String(),
			"correlationId", CorrelationIDFrom(c.Request.Context()),
		)
		h.recordDivergence()
	}

	c.JSON(http.StatusOK, reconciliationResponse{
		WalletID:          view.WalletID,
		StoredBalance:     view.StoredBalance,
		CalculatedBalance: view.CalculatedBalance,
		Difference:        view.Difference,
		Consistent:        view.Consistent,
		CheckedEntries:    view.CheckedEntries,
	})
}

// ── operações ────────────────────────────────────────────────────────

// ProcessTransaction envia uma operação financeira.
//
// O providerId é conferido contra a identidade autenticada: um provedor nunca
// opera em nome de outro, mesmo que envie o campo no corpo.
func (h *Handler) ProcessTransaction(c *gin.Context) {
	identity, ok := IdentityFrom(c.Request.Context())
	if !ok {
		writeError(c, ports.ErrMissingCredentials)
		return
	}

	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		writeError(c, domainerr.New(domainerr.KindInvalid, "IDEMPOTENCY_KEY_REQUIRED",
			"o header Idempotency-Key é obrigatório"))
		return
	}

	var request processTransactionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		writeError(c, domainerr.Wrap(domainerr.KindInvalid, "INVALID_REQUEST_BODY", "corpo da requisição inválido", err))
		return
	}

	if strings.TrimSpace(request.ProviderID) != identity.ProviderID {
		writeError(c, forbidden("o providerId informado não corresponde ao provedor autenticado"))
		return
	}

	result, err := h.deps.Process.Execute(c.Request.Context(), usecase.ProcessTransactionCommand{
		ProviderID:            identity.ProviderID,
		ExternalTransactionID: request.ExternalTransactionID,
		IdempotencyKey:        idempotencyKey,
		PlayerID:              request.PlayerID,
		WalletID:              request.WalletID,
		RoundID:               request.RoundID,
		GameID:                request.GameID,
		Kind:                  request.Kind,
		Money:                 request.Money,
		ReferenceExternalID:   request.ReferenceExternalTransactionID,
		CorrelationID:         CorrelationIDFrom(c.Request.Context()),
	})
	if err != nil {
		writeError(c, err)
		return
	}

	// Contabilizado aqui, e não no switch de status mais abaixo: o replay é uma
	// propriedade do RESULTADO, e existe em mais de um ramo do switch.
	if result.IdempotentReplay {
		h.recordReplay()
	}

	body := processTransactionResponse{
		TransactionID:    result.TransactionID,
		Status:           result.Status,
		Balance:          result.Balance,
		IdempotentReplay: result.IdempotentReplay,
		FailureCode:      result.FailureCode,
		FailureMessage:   result.FailureMessage,
	}

	switch result.Status {
	case wagertransaction.StatusProcessed:
		if result.IdempotentReplay {
			c.JSON(http.StatusOK, body)
			return
		}
		c.JSON(http.StatusCreated, body)
	case wagertransaction.StatusPendingReference:
		c.JSON(http.StatusAccepted, body)
	case wagertransaction.StatusRejected:
		// Rejeição de negócio é 422 e devolve o transactionId auditável, para
		// ser distinguível de 400 (entrada inválida) e 409 (conflito).
		c.JSON(http.StatusUnprocessableEntity, body)
	default:
		c.JSON(http.StatusOK, body)
	}
}

// GetTransaction devolve a operação por identificador interno.
//
// Uma operação de outro provedor responde 404: não vaza existência nem dados.
func (h *Handler) GetTransaction(c *gin.Context) {
	identity, ok := IdentityFrom(c.Request.Context())
	if !ok {
		writeError(c, ports.ErrMissingCredentials)
		return
	}

	transaction, err := h.deps.GetTx.Execute(c.Request.Context(), usecase.GetTransactionQuery{
		TransactionID: c.Param("transactionId"),
	})
	if err != nil {
		writeError(c, err)
		return
	}
	if transaction.ProviderID() != identity.ProviderID {
		writeError(c, ports.ErrNotFound)
		return
	}
	c.JSON(http.StatusOK, toTransactionResponse(transaction))
}

// GetTransactionByExternalID devolve a operação pela identidade externa.
func (h *Handler) GetTransactionByExternalID(c *gin.Context) {
	identity, ok := IdentityFrom(c.Request.Context())
	if !ok {
		writeError(c, ports.ErrMissingCredentials)
		return
	}

	pathProviderID := c.Param("providerId")
	if pathProviderID != identity.ProviderID {
		writeError(c, forbidden("o provedor autenticado não corresponde ao providerId da rota"))
		return
	}

	transaction, err := h.deps.GetTx.ByExternalID(c.Request.Context(), usecase.GetTransactionByExternalIDQuery{
		ProviderID:            pathProviderID,
		ExternalTransactionID: c.Param("externalTransactionId"),
	})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, toTransactionResponse(transaction))
}
