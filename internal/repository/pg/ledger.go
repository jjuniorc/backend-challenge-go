package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

type ledgerRepo struct{ q querier }

func (r *ledgerRepo) Insert(ctx context.Context, e ledger.Entry) error {
	_, err := r.q.Exec(ctx, `
INSERT INTO wallet_ledger_entries (
    id, wallet_id, transaction_id, direction, currency,
    amount_minor, balance_before_minor, balance_after_minor, created_at
) VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()),
		string(e.Amount().Currency()), e.Amount().Amount(),
		e.BalanceBefore().Amount(), e.BalanceAfter().Amount(), e.CreatedAt())
	return translate(err)
}

// SumByWallet soma créditos como positivos e débitos como negativos, em
// unidades mínimas. É o lado "reconstruído" da reconciliação.
func (r *ledgerRepo) SumByWallet(ctx context.Context, walletID string) (int64, int, error) {
	var (
		sum   int64
		count int
	)
	err := r.q.QueryRow(ctx, `
SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0),
       COUNT(*)
  FROM wallet_ledger_entries
 WHERE wallet_id = $1::uuid`, walletID).Scan(&sum, &count)
	if err != nil {
		return 0, 0, translate(err)
	}
	return sum, count, nil
}

const ledgerColumns = `id::text, wallet_id::text, transaction_id::text, direction, currency,
	amount_minor, balance_before_minor, balance_after_minor, created_at`

// ListByWallet pagina o ledger do mais recente para o mais antigo.
//
// A ordenação é (created_at DESC, id DESC) e o cursor usa a comparação de
// linha (created_at, id) < (cursor), que é estável mesmo quando vários
// lançamentos compartilham o mesmo instante. Busca limit+1 linhas para saber se
// existe próxima página sem uma segunda consulta.
func (r *ledgerRepo) ListByWallet(ctx context.Context, walletID string, after *ports.LedgerCursor, limit int) (ports.LedgerPage, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if after == nil {
		rows, err = r.q.Query(ctx, `
SELECT `+ledgerColumns+`
  FROM wallet_ledger_entries
 WHERE wallet_id = $1::uuid
 ORDER BY created_at DESC, id DESC
 LIMIT $2`, walletID, limit+1)
	} else {
		rows, err = r.q.Query(ctx, `
SELECT `+ledgerColumns+`
  FROM wallet_ledger_entries
 WHERE wallet_id = $1::uuid
   AND (created_at, id) < ($2::timestamptz, $3::uuid)
 ORDER BY created_at DESC, id DESC
 LIMIT $4`, walletID, after.CreatedAt, after.ID, limit+1)
	}
	if err != nil {
		return ports.LedgerPage{}, translate(err)
	}
	defer rows.Close()

	entries := make([]ledger.Entry, 0, limit+1)
	for rows.Next() {
		var (
			id, walletIDCol, transactionID, direction, currency string
			amountMinor, beforeMinor, afterMinor                int64
			createdAt                                           time.Time
		)
		if err := rows.Scan(&id, &walletIDCol, &transactionID, &direction, &currency,
			&amountMinor, &beforeMinor, &afterMinor, &createdAt); err != nil {
			return ports.LedgerPage{}, translate(err)
		}
		cur, err := money.ParseCurrency(currency)
		if err != nil {
			return ports.LedgerPage{}, err
		}
		amount, err := money.NewMoney(amountMinor, cur)
		if err != nil {
			return ports.LedgerPage{}, err
		}
		before, err := money.NewMoney(beforeMinor, cur)
		if err != nil {
			return ports.LedgerPage{}, err
		}
		afterBalance, err := money.NewMoney(afterMinor, cur)
		if err != nil {
			return ports.LedgerPage{}, err
		}
		entry, err := ledger.Rehydrate(ledger.Params{
			ID:            id,
			WalletID:      walletIDCol,
			TransactionID: transactionID,
			Direction:     ledger.Direction(direction),
			Amount:        amount,
			BalanceBefore: before,
			BalanceAfter:  afterBalance,
			CreatedAt:     createdAt,
		})
		if err != nil {
			return ports.LedgerPage{}, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return ports.LedgerPage{}, translate(err)
	}

	page := ports.LedgerPage{}
	if len(entries) > limit {
		last := entries[limit-1]
		page.NextCursor = &ports.LedgerCursor{CreatedAt: last.CreatedAt(), ID: last.ID()}
		entries = entries[:limit]
	}
	page.Entries = entries
	return page, nil
}

var _ ports.LedgerRepository = (*ledgerRepo)(nil)
