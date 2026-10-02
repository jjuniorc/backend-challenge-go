//go:build integration

package app_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/jjuniorc/backend-challenge-go/internal/app"
	"github.com/jjuniorc/backend-challenge-go/internal/httpapi"
	"github.com/jjuniorc/backend-challenge-go/internal/repository/pg"
	"github.com/jjuniorc/backend-challenge-go/internal/testsupport"
)

// TestApplicationLifecycleReleasesResources prova início e encerramento da
// composição Fx com RECURSOS REAIS: servidor HTTP escutando e pool do
// PostgreSQL aberto.
//
// A ordem de encerramento é garantida pelo fx.Lifecycle: os hooks de OnStop
// rodam na ordem inversa do OnStart. Como o hook do servidor é registrado por
// último (num fx.Invoke, depois de todos os fx.Provide), o servidor para de
// aceitar requisições ANTES de o pool ser fechado — evitando que uma requisição
// em andamento encontre o banco já fechado.
func TestApplicationLifecycleReleasesResources(t *testing.T) {
	testsupport.PrepareDatabase(t)

	t.Setenv("APP_ENV", "test")
	// Estes testes exercitam o contrato HTTP e o ciclo de vida, sem IdP.
	// O modo oidc é coberto por internal/auth/*_integration_test.go.
	// Estes testes exercitam HTTP/autenticação, não mensageria.
	// A publicação da outbox é coberta em internal/worker/*_integration_test.go.
	t.Setenv("OUTBOX_ENABLED", "false")
	t.Setenv("AUTH_MODE", "dev")
	t.Setenv("INSTANCE_ID", "lifecycle-test")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("HTTP_ADDR", "127.0.0.1:0")
	t.Setenv("POSTGRES_DSN", testsupport.DSN())
	t.Setenv("OIDC_ISSUER", "http://localhost:8080/realms/wager")
	t.Setenv("OIDC_JWKS_URL", "http://keycloak:8080/realms/wager/protocol/openid-connect/certs")

	var (
		server *httpapi.Server
		db     *pg.DB
	)
	application := fx.New(app.Module, fx.Populate(&server, &db), fx.NopLogger)

	if err := application.Start(context.Background()); err != nil {
		t.Fatalf("iniciando aplicação: %v", err)
	}

	// 1) o servidor está no ar
	baseURL := "http://" + server.Addr()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(baseURL + "/health/live")
	if err != nil {
		t.Fatalf("requisição ao health/live: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health/live = %d, quer 200", resp.StatusCode)
	}

	// 2) o pool do banco responde
	if err := db.Ping(context.Background()); err != nil {
		t.Fatalf("ping no banco: %v", err)
	}

	// 3) encerramento
	stopCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := application.Stop(stopCtx); err != nil {
		t.Fatalf("encerrando aplicação: %v", err)
	}

	// 4) o pool foi LIBERADO
	if err := db.Ping(context.Background()); err == nil {
		t.Fatal("o pool do PostgreSQL deveria estar fechado após o Stop")
	}

	// 5) o servidor parou de aceitar conexões (cliente sem keep-alive, para não
	//    reaproveitar uma conexão ociosa já existente)
	freshClient := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	if resp, err := freshClient.Get(baseURL + "/health/live"); err == nil {
		_ = resp.Body.Close()
		t.Fatal("o servidor HTTP deveria estar encerrado após o Stop")
	}
}
