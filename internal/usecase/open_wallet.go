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
	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// OpenWallet abre uma carteira e, quando o saldo inicial é positivo, cria a
// abertura (OPENING), o lançamento de crédito e os eventos financeiros no MESMO
// commit da carteira.
//
// Saldo inicial zero não cria OPENING, ledger nem eventos financeiros: a
// carteira simplesmente nasce zerada, na versão 1.
type OpenWallet struct {
	store ports.Store
	ids   ports.IDGenerator
	clock ports.Clock
}

// NewOpenWallet injeta as dependências do caso de uso.
func NewOpenWallet(store ports.Store, ids ports.IDGenerator, clock ports.Clock) *OpenWallet {
	return &OpenWallet{store: store, ids: ids, clock: clock}
}

// OpenWalletCommand é a entrada do caso de uso.
type OpenWalletCommand struct {
	PlayerID       string
	InitialBalance money.Money
	CorrelationID  string
}

// OpenWalletResult é a saída do caso de uso.
type OpenWalletResult struct {
	WalletID string
	PlayerID string
	Balance  money.Money
	Version  int64
}

// Execute cria a carteira de forma atômica.
func (uc *OpenWallet) Execute(ctx context.Context, cmd OpenWalletCommand) (OpenWalletResult, error) {
	if !cmd.InitialBalance.IsValid() {
		return OpenWalletResult{}, domainerr.New(domainerr.KindInvalid, "OPEN_WALLET_INVALID_BALANCE",
			"saldo inicial inválido")
	}

	now := uc.clock.Now()
	walletID, err := uc.ids.NewID()
	if err != nil {
		return OpenWalletResult{}, err
	}

	w, err := wallet.New(wallet.NewParams{
		ID:             walletID,
		PlayerID:       cmd.PlayerID,
		InitialBalance: cmd.InitialBalance,
		Now:            now,
	})
	if err != nil {
		return OpenWalletResult{}, err
	}

	correlationID := strings.TrimSpace(cmd.CorrelationID)
	if correlationID == "" {
		correlationID = walletID
	}

	var (
		opening         wagertransaction.Transaction
		entry           ledger.Entry
		eventsToPublish []events.Envelope
	)

	if cmd.InitialBalance.IsPositive() {
		zero, err := money.Zero(cmd.InitialBalance.Currency())
		if err != nil {
			return OpenWalletResult{}, err
		}

		openingID, err := uc.ids.NewID()
		if err != nil {
			return OpenWalletResult{}, err
		}
		opening, err = wagertransaction.NewOpening(wagertransaction.OpeningParams{
			ID:       openingID,
			WalletID: walletID,
			PlayerID: cmd.PlayerID,
			Money:    cmd.InitialBalance,
			Now:      now,
		})
		if err != nil {
			return OpenWalletResult{}, err
		}

		entryID, err := uc.ids.NewID()
		if err != nil {
			return OpenWalletResult{}, err
		}
		entry, err = ledger.New(ledger.Params{
			ID:            entryID,
			WalletID:      walletID,
			TransactionID: opening.ID(),
			Direction:     ledger.DirectionCredit,
			Amount:        cmd.InitialBalance,
			BalanceBefore: zero,
			BalanceAfter:  cmd.InitialBalance,
			CreatedAt:     now,
		})
		if err != nil {
			return OpenWalletResult{}, err
		}

		eventsToPublish, err = uc.buildOpeningEvents(walletID, opening, correlationID, zero, now)
		if err != nil {
			return OpenWalletResult{}, err
		}
	}

	err = uc.store.RunInTx(ctx, func(ctx context.Context, tx ports.Tx) error {
		if err := tx.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		if !cmd.InitialBalance.IsPositive() {
			return nil
		}
		if err := tx.Transactions().Insert(ctx, opening); err != nil {
			return err
		}
		if err := tx.Ledger().Insert(ctx, entry); err != nil {
			return err
		}
		for _, event := range eventsToPublish {
			if err := tx.Outbox().Insert(ctx, event); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return OpenWalletResult{}, err
	}

	return OpenWalletResult{
		WalletID: walletID,
		PlayerID: cmd.PlayerID,
		Balance:  w.Balance(),
		Version:  w.Version(),
	}, nil
}

// buildOpeningEvents cria os dois eventos da abertura.
//
// OPENING é origem interna: os metadados externos (providerId) não se aplicam e
// são omitidos do payload. A versão da carteira na abertura é 1, porque a
// abertura não incrementa a versão.
func (uc *OpenWallet) buildOpeningEvents(walletID string, opening wagertransaction.Transaction, correlationID string, zero money.Money, now time.Time) ([]events.Envelope, error) {
	processedID, err := uc.ids.NewID()
	if err != nil {
		return nil, err
	}
	processed, err := events.NewWagerTransactionProcessed(events.Meta{
		EventID:       processedID,
		CorrelationID: correlationID,
		OccurredAt:    now,
	}, events.ProcessedParams{
		TransactionID: opening.ID(),
		WalletID:      walletID,
		Kind:          opening.Kind(),
		Status:        opening.Status(),
		Money:         opening.Amount(),
		Balance:       opening.ResultBalance(),
	})
	if err != nil {
		return nil, err
	}

	balanceChangedID, err := uc.ids.NewID()
	if err != nil {
		return nil, err
	}
	balanceChanged, err := events.NewWalletBalanceChanged(events.Meta{
		EventID:       balanceChangedID,
		CorrelationID: correlationID,
		CausationID:   processedID,
		OccurredAt:    now,
	}, events.BalanceChangedParams{
		WalletID:      walletID,
		TransactionID: opening.ID(),
		Direction:     ledger.DirectionCredit,
		Money:         opening.Amount(),
		BalanceBefore: zero,
		BalanceAfter:  opening.Amount(),
		WalletVersion: 1,
	})
	if err != nil {
		return nil, err
	}

	return []events.Envelope{processed, balanceChanged}, nil
}
