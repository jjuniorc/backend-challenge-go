// Package pg implementa os contratos de internal/ports sobre PostgreSQL usando
// pgx com SQL explícito.
//
// Decisões que aparecem em todo o pacote:
//
//   - Colunas UUID são lidas com cast `::text` e escritas com cast `$n::uuid`.
//     Isso elimina qualquer dependência de codec de UUID do driver e mantém o
//     SQL legível e verificável.
//   - Colunas anuláveis são lidas com COALESCE para string/zero e escritas com
//     NULLIF/ponteiro nulo, de modo que o domínio nunca vê ponteiros.
//   - Todo erro do driver passa por translate(), que o converte em sentinelas
//     classificáveis de ports.
package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// Garantias de contrato em tempo de compilação.
var (
	_ ports.Store         = (*DB)(nil)
	_ ports.Tx            = (*Tx)(nil)
	_ ports.HealthChecker = (*DB)(nil)
)

// querier é o subconjunto comum de *pgxpool.Pool e pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DB é o acesso ao banco em autocommit.
type DB struct {
	pool *pgxpool.Pool
}

// Open abre o pool, valida a conectividade e devolve o store.
func Open(ctx context.Context, dsn string, maxConns int32) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &DB{pool: pool}, nil
}

// Ping verifica a disponibilidade do banco.
func (db *DB) Ping(ctx context.Context) error { return db.pool.Ping(ctx) }

// Name identifica o componente nos health checks.
func (db *DB) Name() string { return "postgres" }

// Check satisfaz ports.HealthChecker: o readiness falha se o banco não responde.
func (db *DB) Check(ctx context.Context) error { return db.Ping(ctx) }

// Close encerra o pool.
func (db *DB) Close() { db.pool.Close() }

func (db *DB) Wallets() ports.WalletRepository           { return &walletRepo{q: db.pool} }
func (db *DB) Transactions() ports.TransactionRepository { return &transactionRepo{q: db.pool} }
func (db *DB) Ledger() ports.LedgerRepository            { return &ledgerRepo{q: db.pool} }
func (db *DB) Outbox() ports.OutboxRepository            { return &outboxRepo{q: db.pool} }
func (db *DB) Inbox() ports.InboxRepository              { return &inboxRepo{q: db.pool} }

// RunInTx executa fn numa transação SQL.
//
// Um erro em fn provoca rollback de TODO o trabalho — inclusive os registros de
// outbox — o que é exatamente a garantia exigida: nenhum evento pode ser
// publicado antes do commit da transação que o originou.
func (db *DB) RunInTx(ctx context.Context, fn func(ctx context.Context, tx ports.Tx) error) error {
	sqlTx, err := db.pool.Begin(ctx)
	if err != nil {
		return translate(err)
	}
	defer func() {
		// context.WithoutCancel garante que o rollback aconteça mesmo se o
		// contexto da requisição tiver sido cancelado no meio do caminho.
		_ = sqlTx.Rollback(context.WithoutCancel(ctx))
	}()

	if err := fn(ctx, &Tx{q: sqlTx}); err != nil {
		return err
	}
	if err := sqlTx.Commit(ctx); err != nil {
		return translate(err)
	}
	return nil
}

// Tx é o conjunto de repositórios ligados a uma transação SQL.
type Tx struct {
	q querier
}

func (t *Tx) Wallets() ports.WalletRepository           { return &walletRepo{q: t.q} }
func (t *Tx) Transactions() ports.TransactionRepository { return &transactionRepo{q: t.q} }
func (t *Tx) Ledger() ports.LedgerRepository            { return &ledgerRepo{q: t.q} }
func (t *Tx) Outbox() ports.OutboxRepository            { return &outboxRepo{q: t.q} }
func (t *Tx) Inbox() ports.InboxRepository              { return &inboxRepo{q: t.q} }

// ── helpers de valor ─────────────────────────────────────────────────

// nullableString devolve NULL para string vazia, evitando gravar ” em colunas
// opcionais (o que quebraria as constraints de consistência do schema).
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt64(v int64, present bool) any {
	if !present {
		return nil
	}
	return v
}

func nullableTime(t time.Time, present bool) any {
	if !present {
		return nil
	}
	return t
}
