package usecase

import (
	"context"
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wallet"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// Limites de paginação do ledger.
const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// ── carteira ─────────────────────────────────────────────────────────

// GetWallet lê o estado atual de uma carteira.
type GetWallet struct {
	store ports.Store
}

// NewGetWallet injeta as dependências.
func NewGetWallet(store ports.Store) *GetWallet { return &GetWallet{store: store} }

// GetWalletQuery é a entrada.
type GetWalletQuery struct {
	WalletID string
}

// WalletView é a saída.
type WalletView struct {
	ID        string
	PlayerID  string
	Currency  money.Currency
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Execute devolve a carteira.
func (uc *GetWallet) Execute(ctx context.Context, q GetWalletQuery) (WalletView, error) {
	w, err := uc.store.Wallets().Get(ctx, q.WalletID)
	if err != nil {
		return WalletView{}, err
	}
	return toWalletView(w), nil
}

func toWalletView(w wallet.Wallet) WalletView {
	return WalletView{
		ID:        w.ID(),
		PlayerID:  w.PlayerID(),
		Currency:  w.Currency(),
		Balance:   w.Balance(),
		Version:   w.Version(),
		CreatedAt: w.CreatedAt(),
		UpdatedAt: w.UpdatedAt(),
	}
}

// ── ledger ───────────────────────────────────────────────────────────

// GetLedger lê uma página do ledger da carteira.
type GetLedger struct {
	store ports.Store
}

// NewGetLedger injeta as dependências.
func NewGetLedger(store ports.Store) *GetLedger { return &GetLedger{store: store} }

// GetLedgerQuery é a entrada.
type GetLedgerQuery struct {
	WalletID string
	After    *ports.LedgerCursor
	Limit    int
}

// LedgerView é a saída.
type LedgerView struct {
	WalletID   string
	Entries    []ledger.Entry
	NextCursor *ports.LedgerCursor
}

// Execute devolve a página do ledger, validando o limite.
func (uc *GetLedger) Execute(ctx context.Context, q GetLedgerQuery) (LedgerView, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		return LedgerView{}, domainerr.New(domainerr.KindInvalid, "LEDGER_LIMIT_TOO_LARGE",
			"limit excede o máximo permitido")
	}

	page, err := uc.store.Ledger().ListByWallet(ctx, q.WalletID, q.After, limit)
	if err != nil {
		return LedgerView{}, err
	}
	return LedgerView{WalletID: q.WalletID, Entries: page.Entries, NextCursor: page.NextCursor}, nil
}

// ── transações ───────────────────────────────────────────────────────

// GetTransaction lê uma operação por identificador interno ou pela identidade
// externa (providerId + externalTransactionId).
type GetTransaction struct {
	store ports.Store
}

// NewGetTransaction injeta as dependências.
func NewGetTransaction(store ports.Store) *GetTransaction { return &GetTransaction{store: store} }

// GetTransactionQuery é a entrada por identificador interno.
type GetTransactionQuery struct {
	TransactionID string
}

// GetTransactionByExternalIDQuery é a entrada pela identidade externa.
type GetTransactionByExternalIDQuery struct {
	ProviderID            string
	ExternalTransactionID string
}

// Execute busca pelo identificador interno.
func (uc *GetTransaction) Execute(ctx context.Context, q GetTransactionQuery) (wagertransaction.Transaction, error) {
	if strings.TrimSpace(q.TransactionID) == "" {
		return wagertransaction.Transaction{}, domainerr.New(domainerr.KindInvalid, "TRANSACTION_ID_REQUIRED",
			"transactionId é obrigatório")
	}
	return uc.store.Transactions().GetByID(ctx, q.TransactionID)
}

// ByExternalID busca pela identidade externa da operação.
func (uc *GetTransaction) ByExternalID(ctx context.Context, q GetTransactionByExternalIDQuery) (wagertransaction.Transaction, error) {
	if strings.TrimSpace(q.ProviderID) == "" || strings.TrimSpace(q.ExternalTransactionID) == "" {
		return wagertransaction.Transaction{}, domainerr.New(domainerr.KindInvalid, "TRANSACTION_ID_REQUIRED",
			"providerId e externalTransactionId são obrigatórios")
	}
	return uc.store.Transactions().FindByProviderAndExternalID(ctx, q.ProviderID, q.ExternalTransactionID)
}

// ── reconciliação ────────────────────────────────────────────────────

// Reconcile reconstrói o saldo a partir do ledger e compara com o armazenado.
type Reconcile struct {
	store ports.Store
}

// NewReconcile injeta as dependências.
func NewReconcile(store ports.Store) *Reconcile { return &Reconcile{store: store} }

// ReconcileQuery é a entrada.
type ReconcileQuery struct {
	WalletID string
}

// ReconciliationView é a saída. Difference é o saldo armazenado menos o
// reconstruído.
type ReconciliationView struct {
	WalletID          string
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int
}

// Execute calcula a reconciliação numa VISÃO CONSISTENTE.
//
// Carteira e ledger são lidos dentro da MESMA transação, e a carteira é travada
// antes da leitura. Isso é o que garante consistência: toda mudança de saldo
// acontece sob esse mesmo lock e no mesmo commit do seu lançamento, então,
// enquanto o lock é mantido, nenhum escritor está no meio do caminho e o par
// (saldo, lançamentos) é sempre coerente.
//
// A reconciliação NÃO altera o saldo.
func (uc *Reconcile) Execute(ctx context.Context, q ReconcileQuery) (ReconciliationView, error) {
	var view ReconciliationView
	err := uc.store.RunInTx(ctx, func(ctx context.Context, tx ports.Tx) error {
		w, err := tx.Wallets().GetForUpdate(ctx, q.WalletID)
		if err != nil {
			return err
		}
		sum, entries, err := tx.Ledger().SumByWallet(ctx, q.WalletID)
		if err != nil {
			return err
		}
		calculated, err := money.NewMoney(sum, w.Currency())
		if err != nil {
			return err
		}
		difference, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		view = ReconciliationView{
			WalletID:          q.WalletID,
			StoredBalance:     w.Balance(),
			CalculatedBalance: calculated,
			Difference:        difference,
			Consistent:        difference.IsZero(),
			CheckedEntries:    entries,
		}
		return nil
	})
	if err != nil {
		return ReconciliationView{}, err
	}
	return view, nil
}
