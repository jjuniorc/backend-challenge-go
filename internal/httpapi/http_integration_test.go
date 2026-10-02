//go:build integration

package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/jjuniorc/backend-challenge-go/internal/app"
	"github.com/jjuniorc/backend-challenge-go/internal/httpapi"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
)

const (
	tokenInternal  = "dev:svc-internal:-:internal"
	tokenProviderA = "dev:svc-provider-a:provider-a:provider"
	tokenProviderB = "dev:svc-provider-b:provider-b:provider"
)

// ── infraestrutura do teste ──────────────────────────────────────────

type apiClient struct {
	t    *testing.T
	base string
}

// newTestClient sobe a APLICAÇÃO COMPLETA via Fx (config, pool, repositórios,
// casos de uso, handlers, servidor) contra um banco real e devolve um cliente
// apontando para o endereço efetivamente escutado.
func newTestClient(t *testing.T) *apiClient {
	t.Helper()

	testsupport.PrepareDatabase(t)

	t.Setenv("APP_ENV", "test")
	// Estes testes exercitam o contrato HTTP e o ciclo de vida, sem IdP.
	// O modo oidc é coberto por internal/auth/*_integration_test.go.
	// Estes testes exercitam HTTP/autenticação, não mensageria.
	// A publicação da outbox é coberta em internal/worker/*_integration_test.go.
	t.Setenv("OUTBOX_ENABLED", "false")
	t.Setenv("AUTH_MODE", "dev")
	t.Setenv("INSTANCE_ID", "http-integration")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("POSTGRES_DSN", testsupport.DSN())
	t.Setenv("OIDC_ISSUER", "http://localhost:8080/realms/wager")
	t.Setenv("OIDC_JWKS_URL", "http://keycloak:8080/realms/wager/protocol/openid-connect/certs")
	t.Setenv("OIDC_TOKEN_URL", "http://localhost:8080/realms/wager/protocol/openid-connect/token")

	var server *httpapi.Server
	application := fx.New(app.Module, fx.Populate(&server), fx.NopLogger)

	if err := application.Start(context.Background()); err != nil {
		t.Fatalf("iniciando aplicação: %v", err)
	}
	if err := application.Err(); err != nil {
		t.Fatalf("erro na composição Fx: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := application.Stop(stopCtx); err != nil {
			t.Errorf("encerrando aplicação: %v", err)
		}
	})

	return &apiClient{t: t, base: "http://" + server.Addr()}
}

func (c *apiClient) request(method, path, token string, body any, headers map[string]string) (int, []byte) {
	c.t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			c.t.Fatalf("serializando corpo: %v", err)
		}
		reader = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		c.t.Fatalf("montando requisição: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("lendo corpo da resposta: %v", err)
	}
	return resp.StatusCode, raw
}

func (c *apiClient) get(path, token string) (int, []byte) {
	return c.request(http.MethodGet, path, token, nil, nil)
}

func (c *apiClient) post(path, token string, body any, headers map[string]string) (int, []byte) {
	return c.request(http.MethodPost, path, token, body, headers)
}

func decodeInto(t *testing.T, raw []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decodificando resposta (%s): %v", raw, err)
	}
}

func assertErrorCode(t *testing.T, raw []byte, want string) {
	t.Helper()
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	decodeInto(t, raw, &body)
	if body.Error.Code != want {
		t.Fatalf("código de erro = %q, quer %q (corpo: %s)", body.Error.Code, want, raw)
	}
	if body.Error.Message == "" {
		t.Fatal("mensagem de erro não pode ser vazia")
	}
}

// ── DTOs mínimos do contrato ─────────────────────────────────────────

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type walletJSON struct {
	ID       string    `json:"id"`
	PlayerID string    `json:"playerId"`
	Balance  moneyJSON `json:"balance"`
	Version  int64     `json:"version"`
}

type transactionResultJSON struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          moneyJSON `json:"balance"`
	IdempotentReplay bool      `json:"idempotentReplay"`
	FailureCode      string    `json:"failureCode"`
}

type transactionJSON struct {
	TransactionID         string    `json:"transactionId"`
	ProviderID            string    `json:"providerId"`
	ExternalTransactionID string    `json:"externalTransactionId"`
	Kind                  string    `json:"kind"`
	Status                string    `json:"status"`
	Money                 moneyJSON `json:"money"`
	Balance               moneyJSON `json:"balance"`
	FailureCode           string    `json:"failureCode"`
}

type ledgerEntryJSON struct {
	ID            string    `json:"id"`
	TransactionID string    `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         moneyJSON `json:"money"`
	BalanceBefore moneyJSON `json:"balanceBefore"`
	BalanceAfter  moneyJSON `json:"balanceAfter"`
}

type ledgerJSON struct {
	WalletID   string            `json:"walletId"`
	Entries    []ledgerEntryJSON `json:"entries"`
	NextCursor string            `json:"nextCursor"`
}

type reconciliationJSON struct {
	WalletID          string    `json:"walletId"`
	StoredBalance     moneyJSON `json:"storedBalance"`
	CalculatedBalance moneyJSON `json:"calculatedBalance"`
	Difference        moneyJSON `json:"difference"`
	Consistent        bool      `json:"consistent"`
	CheckedEntries    int       `json:"checkedEntries"`
}

// ── helpers de cenário ───────────────────────────────────────────────

func (c *apiClient) openWallet(playerID, amount string) string {
	c.t.Helper()
	status, raw := c.post("/wallets", tokenInternal, map[string]any{
		"playerId":       playerID,
		"initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	}, nil)
	if status != http.StatusCreated {
		c.t.Fatalf("abertura de carteira: status %d, corpo %s", status, raw)
	}
	var wallet walletJSON
	decodeInto(c.t, raw, &wallet)
	if wallet.ID == "" {
		c.t.Fatal("carteira aberta sem id")
	}
	return wallet.ID
}

func operationBody(walletID, externalID, kind, amount string) map[string]any {
	return map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": externalID,
		"playerId":              "player-1",
		"walletId":              walletID,
		"roundId":               "round-1",
		"gameId":                "game-1",
		"kind":                  kind,
		"money":                 map[string]string{"amount": amount, "currency": "BRL"},
	}
}

func (c *apiClient) sendOperation(walletID, externalID, kind, amount, idempotencyKey string) (int, []byte) {
	c.t.Helper()
	return c.post("/wagering/transactions", tokenProviderA,
		operationBody(walletID, externalID, kind, amount),
		map[string]string{"Idempotency-Key": idempotencyKey})
}

// ── 1. saúde é pública ───────────────────────────────────────────────

func TestHTTPHealthIsPublic(t *testing.T) {
	client := newTestClient(t)

	status, raw := client.get("/health/live", "")
	if status != http.StatusOK {
		t.Fatalf("live: status %d, corpo %s", status, raw)
	}

	status, raw = client.get("/health/ready", "")
	if status != http.StatusOK {
		t.Fatalf("ready: status %d, corpo %s", status, raw)
	}
	var ready struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	decodeInto(t, raw, &ready)
	if ready.Status != "ready" {
		t.Fatalf("status = %q, quer ready", ready.Status)
	}
	if ready.Checks["postgres"] != "up" {
		t.Fatalf("checks = %v, quer postgres=up", ready.Checks)
	}
}

// ── 2. autenticação e autorização ────────────────────────────────────

func TestHTTPAuthenticationAndAuthorization(t *testing.T) {
	client := newTestClient(t)
	walletID := client.openWallet("player-1", "100.00")

	// Operação do provedor A, usada nos testes de isolamento.
	if status, raw := client.sendOperation(walletID, "ext-bet-1", "BET", "25.00", "provider-a:ext-bet-1"); status != http.StatusCreated {
		t.Fatalf("setup da aposta: status %d, corpo %s", status, raw)
	}

	t.Run("sem credencial retorna 401", func(t *testing.T) {
		status, raw := client.get("/wallets/"+walletID, "")
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "AUTH_MISSING_CREDENTIALS")
	})

	t.Run("credencial invalida retorna 401", func(t *testing.T) {
		status, raw := client.get("/wallets/"+walletID, "nao-e-um-token")
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "AUTH_INVALID_CREDENTIALS")
	})

	t.Run("provedor em rota interna retorna 403", func(t *testing.T) {
		status, raw := client.get("/wallets/"+walletID, tokenProviderA)
		if status != http.StatusForbidden {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "AUTH_FORBIDDEN")
	})

	t.Run("provedor nao opera em nome de outro", func(t *testing.T) {
		body := operationBody(walletID, "ext-b-1", "BET", "10.00")
		body["providerId"] = "provider-b"
		status, raw := client.post("/wagering/transactions", tokenProviderA, body,
			map[string]string{"Idempotency-Key": "provider-b:ext-b-1"})
		if status != http.StatusForbidden {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "AUTH_FORBIDDEN")
	})

	t.Run("provedor B nao consulta transacao de A pela rota do provedor", func(t *testing.T) {
		status, raw := client.get("/providers/provider-a/wagering/transactions/ext-bet-1", tokenProviderB)
		if status != http.StatusForbidden {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "AUTH_FORBIDDEN")
	})

	t.Run("provedor B nao ve transacao de A por id interno (404)", func(t *testing.T) {
		status, raw := client.get("/providers/provider-a/wagering/transactions/ext-bet-1", tokenProviderA)
		if status != http.StatusOK {
			t.Fatalf("sanidade: status %d, corpo %s", status, raw)
		}
		var transaction transactionJSON
		decodeInto(t, raw, &transaction)

		status, raw = client.get("/wagering/transactions/"+transaction.TransactionID, tokenProviderB)
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, quer 404 (sem vazar existência), corpo %s", status, raw)
		}
	})
}

// ── 3. contrato principal: abertura, aposta, replay, conflito, rejeição ──

func TestHTTPWalletTransactionAndReplayContract(t *testing.T) {
	client := newTestClient(t)

	// abertura
	status, raw := client.post("/wallets", tokenInternal, map[string]any{
		"playerId":       "player-1",
		"initialBalance": map[string]string{"amount": "1000.00", "currency": "BRL"},
	}, nil)
	if status != http.StatusCreated {
		t.Fatalf("abertura: status %d, corpo %s", status, raw)
	}
	var wallet walletJSON
	decodeInto(t, raw, &wallet)
	if wallet.Version != 1 || wallet.Balance.Amount != "1000.00" {
		t.Fatalf("carteira = %+v", wallet)
	}

	// aposta nova -> 201
	status, raw = client.sendOperation(wallet.ID, "transaction-123", "BET", "25.00", "provider-a:transaction-123")
	if status != http.StatusCreated {
		t.Fatalf("aposta: status %d, corpo %s", status, raw)
	}
	var first transactionResultJSON
	decodeInto(t, raw, &first)
	if first.Status != "PROCESSED" || first.Balance.Amount != "975.00" || first.IdempotentReplay {
		t.Fatalf("resultado = %+v", first)
	}

	// replay idêntico -> 200 com idempotentReplay e o MESMO saldo
	status, raw = client.sendOperation(wallet.ID, "transaction-123", "BET", "25.00", "provider-a:transaction-123")
	if status != http.StatusOK {
		t.Fatalf("replay: status %d, corpo %s", status, raw)
	}
	var replay transactionResultJSON
	decodeInto(t, raw, &replay)
	if !replay.IdempotentReplay {
		t.Fatal("replay deveria ter idempotentReplay=true")
	}
	if replay.TransactionID != first.TransactionID {
		t.Fatalf("transactionId do replay = %s, quer %s", replay.TransactionID, first.TransactionID)
	}
	if replay.Balance.Amount != "975.00" {
		t.Fatalf("saldo do replay = %s, quer 975.00", replay.Balance.Amount)
	}

	// outra chave para a mesma operação -> 409
	status, raw = client.sendOperation(wallet.ID, "transaction-123", "BET", "25.00", "provider-a:outra-chave")
	if status != http.StatusConflict {
		t.Fatalf("conflito: status %d, corpo %s", status, raw)
	}
	assertErrorCode(t, raw, "IDEMPOTENCY_KEY_MISMATCH")

	// mesma chave com conteúdo diferente -> 409
	status, raw = client.sendOperation(wallet.ID, "transaction-123", "BET", "30.00", "provider-a:transaction-123")
	if status != http.StatusConflict {
		t.Fatalf("conflito de conteúdo: status %d, corpo %s", status, raw)
	}
	assertErrorCode(t, raw, "IDEMPOTENCY_KEY_REUSED")

	// saldo insuficiente -> 422 com failureCode e transactionId auditável
	status, raw = client.sendOperation(wallet.ID, "transaction-grande", "BET", "99999.00", "provider-a:transaction-grande")
	if status != http.StatusUnprocessableEntity {
		t.Fatalf("rejeição: status %d, corpo %s", status, raw)
	}
	var rejected transactionResultJSON
	decodeInto(t, raw, &rejected)
	if rejected.Status != "REJECTED" || rejected.FailureCode != "WALLET_INSUFFICIENT_FUNDS" {
		t.Fatalf("rejeição = %+v", rejected)
	}
	if rejected.TransactionID == "" {
		t.Fatal("rejeição precisa devolver o transactionId para auditoria")
	}

	// consulta por id interno
	status, raw = client.get("/wagering/transactions/"+first.TransactionID, tokenProviderA)
	if status != http.StatusOK {
		t.Fatalf("consulta por id: status %d, corpo %s", status, raw)
	}
	var byID transactionJSON
	decodeInto(t, raw, &byID)
	if byID.ExternalTransactionID != "transaction-123" || byID.Kind != "BET" {
		t.Fatalf("transação = %+v", byID)
	}

	// consulta pela identidade externa
	status, raw = client.get("/providers/provider-a/wagering/transactions/transaction-123", tokenProviderA)
	if status != http.StatusOK {
		t.Fatalf("consulta por external id: status %d, corpo %s", status, raw)
	}

	// carteira não encontrada -> 404
	status, raw = client.get("/wallets/00000000-0000-7000-8000-0000000000ff", tokenInternal)
	if status != http.StatusNotFound {
		t.Fatalf("carteira inexistente: status %d, corpo %s", status, raw)
	}
}

// ── 4. ledger paginado e reconciliação ───────────────────────────────

func TestHTTPLedgerPaginationAndReconciliation(t *testing.T) {
	client := newTestClient(t)
	walletID := client.openWallet("player-1", "1000.00")

	// abertura + BET + WIN = 3 lançamentos
	if status, raw := client.sendOperation(walletID, "ext-bet-1", "BET", "25.00", "provider-a:ext-bet-1"); status != http.StatusCreated {
		t.Fatalf("bet: status %d, corpo %s", status, raw)
	}
	if status, raw := client.sendOperation(walletID, "ext-win-1", "WIN", "100.00", "provider-a:ext-win-1"); status != http.StatusCreated {
		t.Fatalf("win: status %d, corpo %s", status, raw)
	}

	// página 1: 2 lançamentos + cursor
	status, raw := client.get("/wallets/"+walletID+"/ledger?limit=2", tokenInternal)
	if status != http.StatusOK {
		t.Fatalf("ledger página 1: status %d, corpo %s", status, raw)
	}
	var page1 ledgerJSON
	decodeInto(t, raw, &page1)
	if len(page1.Entries) != 2 {
		t.Fatalf("página 1 com %d lançamentos, quer 2", len(page1.Entries))
	}
	if page1.NextCursor == "" {
		t.Fatal("página 1 deveria devolver nextCursor")
	}
	if page1.Entries[0].Direction != "CREDIT" || page1.Entries[0].Money.Amount != "100.00" {
		t.Fatalf("ordenação do ledger incorreta: %+v", page1.Entries[0])
	}

	// página 2 pelo cursor: 1 lançamento, sem cursor
	status, raw = client.get("/wallets/"+walletID+"/ledger?limit=2&cursor="+page1.NextCursor, tokenInternal)
	if status != http.StatusOK {
		t.Fatalf("ledger página 2: status %d, corpo %s", status, raw)
	}
	var page2 ledgerJSON
	decodeInto(t, raw, &page2)
	if len(page2.Entries) != 1 {
		t.Fatalf("página 2 com %d lançamentos, quer 1", len(page2.Entries))
	}
	if page2.NextCursor != "" {
		t.Fatalf("página 2 não deveria ter cursor: %q", page2.NextCursor)
	}
	// sem sobreposição entre as páginas
	if page1.Entries[1].ID == page2.Entries[0].ID {
		t.Fatal("as páginas se sobrepõem")
	}

	// reconciliação: saldo armazenado == reconstruído do ledger
	status, raw = client.post("/wallets/"+walletID+"/reconciliation", tokenInternal, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("reconciliação: status %d, corpo %s", status, raw)
	}
	var reconciliation reconciliationJSON
	decodeInto(t, raw, &reconciliation)
	if !reconciliation.Consistent {
		t.Fatalf("reconciliação inconsistente: %+v", reconciliation)
	}
	if reconciliation.CheckedEntries != 3 {
		t.Fatalf("lançamentos verificados = %d, quer 3", reconciliation.CheckedEntries)
	}
	if reconciliation.StoredBalance.Amount != "1075.00" {
		t.Fatalf("saldo armazenado = %s, quer 1075.00", reconciliation.StoredBalance.Amount)
	}
	if reconciliation.Difference.Amount != "0.00" {
		t.Fatalf("diferença = %s, quer 0.00", reconciliation.Difference.Amount)
	}
}

// ── 5. entradas inválidas e referência pendente ──────────────────────

func TestHTTPInvalidInputsAndPendingReference(t *testing.T) {
	client := newTestClient(t)
	walletID := client.openWallet("player-1", "100.00")

	t.Run("json malformado retorna 400", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, client.base+"/wagering/transactions",
			bytes.NewReader([]byte("{")))
		if err != nil {
			t.Fatalf("montando requisição: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokenProviderA)
		req.Header.Set("Idempotency-Key", "provider-a:malformado")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("requisição: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("status = %d, quer 400", resp.StatusCode)
		}
	})

	t.Run("Idempotency-Key ausente retorna 400", func(t *testing.T) {
		status, raw := client.post("/wagering/transactions", tokenProviderA,
			operationBody(walletID, "ext-sem-chave", "BET", "10.00"), nil)
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "IDEMPOTENCY_KEY_REQUIRED")
	})

	t.Run("valor invalido retorna 400", func(t *testing.T) {
		body := operationBody(walletID, "ext-valor-invalido", "BET", "10.00")
		body["money"] = map[string]string{"amount": "1e3", "currency": "BRL"}
		status, raw := client.post("/wagering/transactions", tokenProviderA, body,
			map[string]string{"Idempotency-Key": "provider-a:ext-valor-invalido"})
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
	})

	t.Run("tipo desconhecido retorna 400", func(t *testing.T) {
		status, raw := client.sendOperation(walletID, "ext-pix", "PIX", "10.00", "provider-a:ext-pix")
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
	})

	t.Run("OPENING enviado por HTTP retorna 422", func(t *testing.T) {
		status, raw := client.sendOperation(walletID, "ext-opening", "OPENING", "10.00", "provider-a:ext-opening")
		if status != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "WAGER_OPENING_NOT_EXTERNAL")
	})

	t.Run("cursor invalido retorna 400", func(t *testing.T) {
		status, raw := client.get("/wallets/"+walletID+"/ledger?cursor=nao-e-cursor", tokenInternal)
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "INVALID_CURSOR")
	})

	t.Run("limit acima do maximo retorna 400", func(t *testing.T) {
		status, raw := client.get("/wallets/"+walletID+"/ledger?limit=999", tokenInternal)
		if status != http.StatusBadRequest {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		assertErrorCode(t, raw, "LEDGER_LIMIT_TOO_LARGE")
	})

	t.Run("REFUND antes da referencia retorna 202 PENDING_REFERENCE", func(t *testing.T) {
		body := operationBody(walletID, "ext-refund-1", "REFUND", "25.00")
		body["referenceExternalTransactionId"] = "ext-inexistente"
		status, raw := client.post("/wagering/transactions", tokenProviderA, body,
			map[string]string{"Idempotency-Key": "provider-a:ext-refund-1"})
		if status != http.StatusAccepted {
			t.Fatalf("status = %d, corpo %s", status, raw)
		}
		var result transactionResultJSON
		decodeInto(t, raw, &result)
		if result.Status != "PENDING_REFERENCE" {
			t.Fatalf("status = %q, quer PENDING_REFERENCE", result.Status)
		}
		if result.Balance.Amount != "100.00" {
			t.Fatalf("saldo = %s, quer 100.00 inalterado", result.Balance.Amount)
		}
	})
}
