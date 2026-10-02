package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wallet"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

type walletRepo struct{ q querier }

const walletColumns = `id::text, player_id, currency, balance_minor, version, created_at, updated_at`

func (r *walletRepo) Insert(ctx context.Context, w wallet.Wallet) error {
	_, err := r.q.Exec(ctx, `
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()),
		w.Balance().Amount(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return translate(err)
}

func (r *walletRepo) Get(ctx context.Context, id string) (wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE id = $1::uuid`, id))
}

// GetForUpdate é o ponto de serialização por carteira: duas operações na mesma
// carteira nunca avançam ao mesmo tempo, e carteiras distintas não se bloqueiam.
//
// Usa FOR NO KEY UPDATE em vez de FOR UPDATE de propósito. O INSERT em
// wager_transactions valida a FK (wallet_id, currency) e o PostgreSQL adquire
// FOR KEY SHARE na linha da carteira, mantendo-o até o commit. FOR UPDATE
// conflita com FOR KEY SHARE, o que faria cada concorrente travar no upgrade
// KEY SHARE -> FOR UPDATE e gerar deadlock. FOR NO KEY UPDATE é compatível com
// FOR KEY SHARE e, ainda assim, exclusivo entre escritores.
//
// A garantia de leitura atualizada se mantém: após aguardar o lock, o
// PostgreSQL re-avalia a linha (EvalPlanQual) e devolve a versão mais recente,
// de modo que o saldo lido já reflete o commit anterior. A cláusula de versão
// em UpdateBalance é a segunda linha de defesa contra lost update.
func (r *walletRepo) GetForUpdate(ctx context.Context, id string) (wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE id = $1::uuid FOR NO KEY UPDATE`, id))
}

func (r *walletRepo) FindByPlayerAndCurrency(ctx context.Context, playerID string, currency money.Currency) (wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallets WHERE player_id = $1 AND currency = $2`,
		playerID, string(currency)))
}

// UpdateBalance grava saldo/versão/updatedAt condicionado à versão esperada.
//
// A cláusula AND version = $5 é a segunda linha de defesa contra lost update,
// além do lock em GetForUpdate: se qualquer outra transação alterou o saldo
// entre a leitura e a escrita, nenhuma linha é afetada e o conflito é exposto.
func (r *walletRepo) UpdateBalance(ctx context.Context, w wallet.Wallet, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx, `
UPDATE wallets
   SET balance_minor = $2, version = $3, updated_at = $4
 WHERE id = $1::uuid AND version = $5`,
		w.ID(), w.Balance().Amount(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return translate(err)
	}
	if tag.RowsAffected() == 0 {
		return ports.ErrVersionConflict
	}
	return nil
}

func scanWallet(row pgx.Row) (wallet.Wallet, error) {
	var (
		id, playerID, currency string
		balanceMinor, version  int64
		createdAt, updatedAt   time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balanceMinor, &version, &createdAt, &updatedAt); err != nil {
		return wallet.Wallet{}, translate(err)
	}
	balance, err := money.NewMoney(balanceMinor, money.Currency(currency))
	if err != nil {
		return wallet.Wallet{}, err
	}
	return wallet.Rehydrate(wallet.RehydrateParams{
		ID:        id,
		PlayerID:  playerID,
		Currency:  money.Currency(currency),
		Balance:   balance,
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	})
}
