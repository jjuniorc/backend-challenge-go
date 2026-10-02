package httpapi

import (
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// ── carteira ─────────────────────────────────────────────────────────

type openWalletRequest struct {
	PlayerID       string      `json:"playerId"`
	InitialBalance money.Money `json:"initialBalance"`
}

type walletResponse struct {
	ID        string      `json:"id"`
	PlayerID  string      `json:"playerId"`
	Currency  string      `json:"currency"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt time.Time   `json:"createdAt"`
	UpdatedAt time.Time   `json:"updatedAt"`
}

func toWalletResponse(v usecase.WalletView) walletResponse {
	return walletResponse{
		ID:        v.ID,
		PlayerID:  v.PlayerID,
		Currency:  string(v.Currency),
		Balance:   v.Balance,
		Version:   v.Version,
		CreatedAt: v.CreatedAt,
		UpdatedAt: v.UpdatedAt,
	}
}

// ── ledger ───────────────────────────────────────────────────────────

type ledgerEntryResponse struct {
	ID            string      `json:"id"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	CreatedAt     time.Time   `json:"createdAt"`
}

type ledgerResponse struct {
	WalletID   string                `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor string                `json:"nextCursor,omitempty"`
}

func toLedgerResponse(v usecase.LedgerView) ledgerResponse {
	entries := make([]ledgerEntryResponse, 0, len(v.Entries))
	for _, e := range v.Entries {
		entries = append(entries, toLedgerEntryResponse(e))
	}
	response := ledgerResponse{WalletID: v.WalletID, Entries: entries}
	if v.NextCursor != nil {
		response.NextCursor = encodeCursor(*v.NextCursor)
	}
	return response
}

func toLedgerEntryResponse(e ledger.Entry) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID:            e.ID(),
		TransactionID: e.TransactionID(),
		Direction:     string(e.Direction()),
		Money:         e.Amount(),
		BalanceBefore: e.BalanceBefore(),
		BalanceAfter:  e.BalanceAfter(),
		CreatedAt:     e.CreatedAt(),
	}
}

// ── operação ─────────────────────────────────────────────────────────

type processTransactionRequest struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
}

type processTransactionResponse struct {
	TransactionID    string                  `json:"transactionId"`
	Status           wagertransaction.Status `json:"status"`
	Balance          money.Money             `json:"balance"`
	IdempotentReplay bool                    `json:"idempotentReplay"`
	FailureCode      string                  `json:"failureCode,omitempty"`
	FailureMessage   string                  `json:"failureMessage,omitempty"`
}

type transactionResponse struct {
	TransactionID                  string                  `json:"transactionId"`
	ProviderID                     string                  `json:"providerId,omitempty"`
	ExternalTransactionID          string                  `json:"externalTransactionId,omitempty"`
	PlayerID                       string                  `json:"playerId"`
	WalletID                       string                  `json:"walletId"`
	RoundID                        string                  `json:"roundId,omitempty"`
	GameID                         string                  `json:"gameId,omitempty"`
	Kind                           wagertransaction.Kind   `json:"kind"`
	Status                         wagertransaction.Status `json:"status"`
	Money                          money.Money             `json:"money"`
	Balance                        *money.Money            `json:"balance,omitempty"`
	ReferenceExternalTransactionID string                  `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string                  `json:"referenceTransactionId,omitempty"`
	FailureCode                    string                  `json:"failureCode,omitempty"`
	FailureMessage                 string                  `json:"failureMessage,omitempty"`
	OccurredAt                     time.Time               `json:"occurredAt"`
	CreatedAt                      time.Time               `json:"createdAt"`
	UpdatedAt                      time.Time               `json:"updatedAt"`
}

func toTransactionResponse(t wagertransaction.Transaction) transactionResponse {
	response := transactionResponse{
		TransactionID:                  t.ID(),
		ProviderID:                     t.ProviderID(),
		ExternalTransactionID:          t.ExternalTransactionID(),
		PlayerID:                       t.PlayerID(),
		WalletID:                       t.WalletID(),
		RoundID:                        t.RoundID(),
		GameID:                         t.GameID(),
		Kind:                           t.Kind(),
		Status:                         t.Status(),
		Money:                          t.Amount(),
		ReferenceExternalTransactionID: t.ExternalReferenceID(),
		ReferenceTransactionID:         t.ReferenceTransactionID(),
		FailureCode:                    t.FailureCode(),
		FailureMessage:                 t.FailureMessage(),
		OccurredAt:                     t.OccurredAt(),
		CreatedAt:                      t.CreatedAt(),
		UpdatedAt:                      t.UpdatedAt(),
	}
	if t.ResultBalance().IsValid() {
		balance := t.ResultBalance()
		response.Balance = &balance
	}
	return response
}

// ── reconciliação ────────────────────────────────────────────────────

type reconciliationResponse struct {
	WalletID          string      `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int         `json:"checkedEntries"`
}
