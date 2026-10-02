//go:build integration

// Package testsupport concentra a infraestrutura compartilhada dos testes de
// integração: banco PostgreSQL REAL (sem mocks), criado, migrado e limpo pelo
// próprio teste.
//
// # Isolamento entre pacotes
//
// O runner do Go executa os binários de teste de pacotes DIFERENTES em paralelo.
// Se todos compartilhassem o mesmo banco, o TRUNCATE de um pacote apagaria dados
// que o outro acabou de inserir. Por isso cada pacote de teste usa o SEU banco,
// derivado do nome do binário de teste:
//
//	internal/usecase        -> wager_test_usecase
//	internal/repository/pg  -> wager_test_pg
//	internal/httpapi        -> wager_test_httpapi
//
// TEST_POSTGRES_DSN aponta para o SERVIDOR: o nome do banco no DSN é sempre
// substituído pelo banco do pacote. Para forçar um único banco (execução de um
// único pacote), defina TEST_POSTGRES_DATABASE.
//
// Só é compilado com a build tag `integration`.
package testsupport

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/jjuniorc/backend-challenge-go/internal/repository/pg"
	"github.com/jjuniorc/backend-challenge-go/migrations"
)

const defaultDSN = "postgres://wager:wager@localhost:5432/wager_test?sslmode=disable"

// DSN devolve o DSN do banco de teste DESTE pacote.
func DSN() string {
	base := baseDSN()
	parsed, err := url.Parse(base)
	if err != nil {
		return base
	}
	parsed.Path = "/" + DatabaseName()
	return parsed.String()
}

// DatabaseName devolve o nome do banco usado por este pacote de teste.
func DatabaseName() string { return databaseName() }

func baseDSN() string {
	if value := strings.TrimSpace(os.Getenv("TEST_POSTGRES_DSN")); value != "" {
		return value
	}
	return defaultDSN
}

func databaseName() string {
	if value := strings.TrimSpace(os.Getenv("TEST_POSTGRES_DATABASE")); value != "" {
		return value
	}
	// os.Args[0] é o binário de teste, por exemplo ".../usecase.test".
	base := strings.TrimSuffix(filepath.Base(os.Args[0]), ".test")
	base = sanitizeIdentifier(base)
	if base == "" {
		base = "package"
	}
	return "wager_test_" + base
}

func sanitizeIdentifier(value string) string {
	var builder strings.Builder
	for _, r := range strings.ToLower(value) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteRune('_')
		}
	}
	return builder.String()
}

// NewStore cria o banco do pacote se necessário, aplica as migrations, limpa as
// tabelas e devolve um store pronto. O pool é fechado automaticamente no fim do
// teste.
func NewStore(t *testing.T) *pg.DB {
	t.Helper()

	dsn := DSN()
	ensureDatabase(t, dsn)
	applyMigrations(t, dsn)

	store, err := pg.Open(context.Background(), dsn, 20)
	if err != nil {
		t.Fatalf("abrindo store de testes: %v", err)
	}
	t.Cleanup(store.Close)

	TruncateAll(t)
	return store
}

// PrepareDatabase garante que o banco do pacote existe, está migrado e limpo,
// SEM abrir pool próprio. Usado por testes que constroem a aplicação via Fx
// (a aplicação abre o próprio pool a partir de POSTGRES_DSN).
func PrepareDatabase(t *testing.T) {
	t.Helper()
	ensureDatabase(t, DSN())
	applyMigrations(t, DSN())
	TruncateAll(t)
}

// TruncateAll limpa todas as tabelas do domínio. TRUNCATE não dispara os
// triggers de imutabilidade do ledger (que são FOR EACH ROW em UPDATE/DELETE).
func TruncateAll(t *testing.T) {
	t.Helper()
	Exec(t, `TRUNCATE wallet_ledger_entries, outbox_events, inbox_messages,
	                  wager_transactions, wallets RESTART IDENTITY CASCADE`)
}

// Exec executa um comando SQL numa conexão própria do suporte de teste.
func Exec(t *testing.T, sqlText string, args ...any) {
	t.Helper()
	conn := connect(t)
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(context.Background(), sqlText, args...); err != nil {
		t.Fatalf("executando SQL de suporte: %v\nSQL: %s", err, sqlText)
	}
}

// ExecErr executa um comando SQL e devolve o erro traduzido, para que o teste
// possa verificar a CLASSIFICAÇÃO da rejeição (ErrImmutable, ErrDuplicate...)
// em vez de apenas "deu erro".
func ExecErr(t *testing.T, sqlText string, args ...any) error {
	t.Helper()
	conn := connect(t)
	defer func() { _ = conn.Close(context.Background()) }()
	_, err := conn.Exec(context.Background(), sqlText, args...)
	return pg.Translate(err)
}

// QueryInt devolve o primeiro inteiro de uma consulta.
func QueryInt(t *testing.T, sqlText string, args ...any) int64 {
	t.Helper()
	conn := connect(t)
	defer func() { _ = conn.Close(context.Background()) }()
	var value int64
	if err := conn.QueryRow(context.Background(), sqlText, args...).Scan(&value); err != nil {
		t.Fatalf("consultando SQL de suporte: %v\nSQL: %s", err, sqlText)
	}
	return value
}

func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), DSN())
	if err != nil {
		t.Fatalf("conectando ao banco de testes (%s): %v", DSN(), err)
	}
	return conn
}

func ensureDatabase(t *testing.T, dsn string) {
	t.Helper()

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("DSN de teste inválido: %v", err)
	}

	ctx := context.Background()
	adminCfg := cfg.Copy()
	adminCfg.Database = "postgres"

	conn, err := pgx.ConnectConfig(ctx, adminCfg)
	if err != nil {
		t.Fatalf("conectando ao banco administrativo: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, cfg.Database).Scan(&exists); err != nil {
		t.Fatalf("consultando pg_database: %v", err)
	}
	if exists {
		return
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", quoteIdent(cfg.Database))); err != nil {
		t.Fatalf("criando banco de testes %q: %v", cfg.Database, err)
	}
}

func applyMigrations(t *testing.T, dsn string) {
	t.Helper()

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		t.Fatalf("abrindo fonte de migrations: %v", err)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		_ = src.Close()
		t.Fatalf("abrindo banco para migrations: %v", err)
	}
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		_ = db.Close()
		_ = src.Close()
		t.Fatalf("inicializando driver de migrations: %v", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		_ = db.Close()
		_ = src.Close()
		t.Fatalf("criando migrator: %v", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatalf("aplicando migrations: %v", err)
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
