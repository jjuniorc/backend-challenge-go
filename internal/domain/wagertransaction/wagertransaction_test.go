package wagertransaction

import (
	"errors"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
)

func testNow() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("setup money(%q): %v", s, err)
	}
	return m
}

func externalParams(t *testing.T, kind Kind, amount string) ExternalParams {
	t.Helper()
	p := ExternalParams{
		ID:                    "tx-1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		IdempotencyKey:        "provider-a:ext-1",
		PayloadHash:           "hash-1",
		WalletID:              "w-1",
		PlayerID:              "p-1",
		RoundID:               "r-1",
		GameID:                "g-1",
		Kind:                  kind,
		Money:                 brl(t, amount),
		Now:                   testNow(),
	}
	if kind.RequiresReference() {
		p.ReferenceExternalID = "ext-ref-1"
	}
	return p
}

func mustExternal(t *testing.T, kind Kind, amount string) Transaction {
	t.Helper()
	tx, err := NewExternal(externalParams(t, kind, amount))
	if err != nil {
		t.Fatalf("NewExternal(%s, %s) falhou: %v", kind, amount, err)
	}
	return tx
}

func TestNewExternalAcceptsAllExternalKinds(t *testing.T) {
	tests := []struct {
		kind   Kind
		amount string
	}{
		{KindBet, "25.00"},
		{KindWin, "10.00"},
		{KindLoss, "0.00"},
		{KindRefund, "25.00"},
		{KindRollback, "25.00"},
	}
	for _, tc := range tests {
		t.Run(string(tc.kind), func(t *testing.T) {
			tx := mustExternal(t, tc.kind, tc.amount)
			if tx.Status() != StatusPending {
				t.Fatalf("status = %s, quer PENDING", tx.Status())
			}
			if tx.Origin() != OriginExternal {
				t.Fatalf("origem = %s, quer EXTERNAL", tx.Origin())
			}
			if tx.IsTerminal() {
				t.Fatal("PENDING não deveria ser terminal")
			}
		})
	}
}

func TestNewExternalRejectsOpening(t *testing.T) {
	if _, err := NewExternal(externalParams(t, KindOpening, "10.00")); !errors.Is(err, ErrOpeningNotExternal) {
		t.Fatalf("erro = %v, quer ErrOpeningNotExternal", err)
	}
}

func TestNewExternalAmountRules(t *testing.T) {
	tests := []struct {
		name   string
		kind   Kind
		amount string
		target error
	}{
		{"BET zero", KindBet, "0.00", ErrAmountMustBePositive},
		{"WIN zero", KindWin, "0.00", ErrAmountMustBePositive},
		{"REFUND zero", KindRefund, "0.00", ErrAmountMustBePositive},
		{"ROLLBACK zero", KindRollback, "0.00", ErrAmountMustBePositive},
		{"LOSS positivo", KindLoss, "25.00", ErrLossRequiresZeroAmount},
		{"LOSS negativo", KindLoss, "-1.00", ErrLossRequiresZeroAmount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewExternal(externalParams(t, tc.kind, tc.amount)); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestNewExternalReferenceRules(t *testing.T) {
	t.Run("REFUND sem referencia", func(t *testing.T) {
		p := externalParams(t, KindRefund, "25.00")
		p.ReferenceExternalID = ""
		if _, err := NewExternal(p); !errors.Is(err, ErrReferenceRequired) {
			t.Fatalf("erro = %v, quer ErrReferenceRequired", err)
		}
	})
	t.Run("ROLLBACK sem referencia", func(t *testing.T) {
		p := externalParams(t, KindRollback, "25.00")
		p.ReferenceExternalID = ""
		if _, err := NewExternal(p); !errors.Is(err, ErrReferenceRequired) {
			t.Fatalf("erro = %v, quer ErrReferenceRequired", err)
		}
	})
	t.Run("BET com referencia", func(t *testing.T) {
		p := externalParams(t, KindBet, "25.00")
		p.ReferenceExternalID = "ext-ref-1"
		if _, err := NewExternal(p); !errors.Is(err, ErrReferenceNotAllowed) {
			t.Fatalf("erro = %v, quer ErrReferenceNotAllowed", err)
		}
	})
	t.Run("LOSS com referencia", func(t *testing.T) {
		p := externalParams(t, KindLoss, "0.00")
		p.ReferenceExternalID = "ext-ref-1"
		if _, err := NewExternal(p); !errors.Is(err, ErrReferenceNotAllowed) {
			t.Fatalf("erro = %v, quer ErrReferenceNotAllowed", err)
		}
	})
	t.Run("WIN com referencia e permitido", func(t *testing.T) {
		p := externalParams(t, KindWin, "10.00")
		p.ReferenceExternalID = "ext-bet-1"
		if _, err := NewExternal(p); err != nil {
			t.Fatalf("WIN com referência deveria ser aceito: %v", err)
		}
	})
	t.Run("auto referencia rejeitada", func(t *testing.T) {
		p := externalParams(t, KindRefund, "25.00")
		p.ReferenceExternalID = p.ExternalTransactionID
		if _, err := NewExternal(p); !errors.Is(err, ErrReferenceSelf) {
			t.Fatalf("erro = %v, quer ErrReferenceSelf", err)
		}
	})
}

func TestNewExternalRequiresMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ExternalParams)
		target error
	}{
		{"sem provider", func(p *ExternalParams) { p.ProviderID = "" }, ErrInvalidProviderID},
		{"sem external id", func(p *ExternalParams) { p.ExternalTransactionID = "" }, ErrInvalidExternalID},
		{"sem idempotency key", func(p *ExternalParams) { p.IdempotencyKey = "" }, ErrInvalidIdempotencyKey},
		{"sem payload hash", func(p *ExternalParams) { p.PayloadHash = "" }, ErrInvalidPayloadHash},
		{"sem wallet", func(p *ExternalParams) { p.WalletID = "" }, ErrInvalidWalletID},
		{"sem player", func(p *ExternalParams) { p.PlayerID = "" }, ErrInvalidPlayerID},
		{"sem round", func(p *ExternalParams) { p.RoundID = "" }, ErrInvalidRoundID},
		{"sem game", func(p *ExternalParams) { p.GameID = "" }, ErrInvalidGameID},
		{"sem id", func(p *ExternalParams) { p.ID = "" }, ErrInvalidID},
		{"sem timestamp", func(p *ExternalParams) { p.Now = time.Time{} }, ErrInvalidTimestamp},
		{"money nao inicializado", func(p *ExternalParams) { p.Money = money.Money{} }, money.ErrUninitialized},
		{"tipo desconhecido", func(p *ExternalParams) { p.Kind = "PIX" }, ErrInvalidKind},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := externalParams(t, KindBet, "25.00")
			tc.mutate(&p)
			if _, err := NewExternal(p); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestNewOpening(t *testing.T) {
	tx, err := NewOpening(OpeningParams{ID: "op-1", WalletID: "w-1", PlayerID: "p-1", Money: brl(t, "1000.00"), Now: testNow()})
	if err != nil {
		t.Fatalf("NewOpening falhou: %v", err)
	}
	if tx.Status() != StatusProcessed {
		t.Fatalf("status = %s, quer PROCESSED", tx.Status())
	}
	if tx.Origin() != OriginInternal {
		t.Fatalf("origem = %s, quer INTERNAL", tx.Origin())
	}
	if tx.Kind() != KindOpening {
		t.Fatalf("tipo = %s, quer OPENING", tx.Kind())
	}
	if tx.ProviderID() != "" || tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" {
		t.Fatal("OPENING não deve carregar metadados externos")
	}
	if !tx.IsTerminal() {
		t.Fatal("OPENING criado em PROCESSED deveria ser terminal")
	}
}

func TestNewOpeningRejectsNonPositiveAmount(t *testing.T) {
	if _, err := NewOpening(OpeningParams{ID: "op-1", WalletID: "w-1", PlayerID: "p-1", Money: brl(t, "0.00"), Now: testNow()}); !errors.Is(err, ErrOpeningNonPositiveAmount) {
		t.Fatalf("erro = %v, quer ErrOpeningNonPositiveAmount", err)
	}
}

func TestStateMachineHappyPaths(t *testing.T) {
	t.Run("PENDING para PROCESSED", func(t *testing.T) {
		tx := mustExternal(t, KindBet, "25.00")
		if err := tx.MarkProcessed(brl(t, "975.00"), testNow()); err != nil {
			t.Fatalf("MarkProcessed falhou: %v", err)
		}
		if tx.Status() != StatusProcessed {
			t.Fatalf("status = %s, quer PROCESSED", tx.Status())
		}
		if got := tx.ResultBalance().String(); got != "975.00" {
			t.Fatalf("saldo resultante = %s, quer 975.00", got)
		}
	})
	t.Run("PENDING para PENDING_REFERENCE e resolucao", func(t *testing.T) {
		tx := mustExternal(t, KindRefund, "25.00")
		if err := tx.MarkPendingReference(testNow()); err != nil {
			t.Fatalf("MarkPendingReference falhou: %v", err)
		}
		if tx.Status() != StatusPendingReference {
			t.Fatalf("status = %s, quer PENDING_REFERENCE", tx.Status())
		}
		if err := tx.ResolveReference("tx-ref-1", testNow()); err != nil {
			t.Fatalf("ResolveReference falhou: %v", err)
		}
		if tx.ReferenceTransactionID() != "tx-ref-1" {
			t.Fatalf("referência = %q, quer tx-ref-1", tx.ReferenceTransactionID())
		}
		if err := tx.MarkProcessed(brl(t, "100.00"), testNow()); err != nil {
			t.Fatalf("MarkProcessed após resolução falhou: %v", err)
		}
	})
	t.Run("PENDING para REJECTED", func(t *testing.T) {
		tx := mustExternal(t, KindBet, "80.00")
		if err := tx.MarkRejected("WALLET_INSUFFICIENT_FUNDS", "saldo insuficiente", testNow()); err != nil {
			t.Fatalf("MarkRejected falhou: %v", err)
		}
		if tx.FailureCode() != "WALLET_INSUFFICIENT_FUNDS" {
			t.Fatalf("failureCode = %q", tx.FailureCode())
		}
	})
	t.Run("PENDING para FAILED", func(t *testing.T) {
		tx := mustExternal(t, KindBet, "25.00")
		if err := tx.MarkFailed("PERSISTENCE_UNAVAILABLE", "banco indisponível", testNow()); err != nil {
			t.Fatalf("MarkFailed falhou: %v", err)
		}
	})
}

func TestTerminalStateRejectsTransitions(t *testing.T) {
	tx := mustExternal(t, KindBet, "25.00")
	if err := tx.MarkProcessed(brl(t, "975.00"), testNow()); err != nil {
		t.Fatalf("setup falhou: %v", err)
	}
	if err := tx.MarkRejected("ANY", "msg", testNow()); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("erro = %v, quer ErrTerminalState", err)
	}
	if err := tx.MarkPendingReference(testNow()); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("erro = %v, quer ErrTerminalState", err)
	}
	if err := tx.MarkProcessed(brl(t, "1.00"), testNow()); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("erro = %v, quer ErrTerminalState", err)
	}
}

func TestInvalidTransitions(t *testing.T) {
	tx := mustExternal(t, KindRefund, "25.00")
	if err := tx.MarkPendingReference(testNow()); err != nil {
		t.Fatalf("setup falhou: %v", err)
	}
	if err := tx.MarkPendingReference(testNow()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("PENDING_REFERENCE -> PENDING_REFERENCE deveria falhar, got %v", err)
	}
}

func TestMarkProcessedValidations(t *testing.T) {
	tx := mustExternal(t, KindBet, "25.00")
	if err := tx.MarkProcessed(money.Money{}, testNow()); !errors.Is(err, money.ErrUninitialized) {
		t.Fatalf("saldo não inicializado: erro = %v", err)
	}
	usd, err := money.ParseDecimal("10.00", "USD")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if err := tx.MarkProcessed(usd, testNow()); !errors.Is(err, ErrResultCurrencyMismatch) {
		t.Fatalf("moeda divergente: erro = %v", err)
	}
}

func TestMarkRejectedRequiresFailureCode(t *testing.T) {
	tx := mustExternal(t, KindBet, "25.00")
	if err := tx.MarkRejected("", "msg", testNow()); !errors.Is(err, ErrInvalidFailureCode) {
		t.Fatalf("erro = %v, quer ErrInvalidFailureCode", err)
	}
}

func TestMarkPendingReferenceNotApplicable(t *testing.T) {
	tx := mustExternal(t, KindBet, "25.00")
	if err := tx.MarkPendingReference(testNow()); !errors.Is(err, ErrReferenceNotApplicable) {
		t.Fatalf("erro = %v, quer ErrReferenceNotApplicable", err)
	}
}

func TestMovementPerKind(t *testing.T) {
	tests := []struct {
		kind Kind
		want Movement
	}{
		{KindOpening, MovementCredit},
		{KindBet, MovementDebit},
		{KindWin, MovementCredit},
		{KindLoss, MovementNone},
		{KindRefund, MovementCredit},
		{KindRollback, MovementOpposite},
	}
	for _, tc := range tests {
		if got := tc.kind.Movement(); got != tc.want {
			t.Fatalf("%s.Movement() = %s, quer %s", tc.kind, got, tc.want)
		}
	}
}

func TestOppositeOf(t *testing.T) {
	tests := []struct {
		referenced Kind
		want       Movement
		wantErr    bool
	}{
		{KindBet, MovementCredit, false},
		{KindWin, MovementDebit, false},
		{KindRefund, MovementDebit, false},
		{KindLoss, "", true},
		{KindRollback, "", true},
		{KindOpening, "", true},
	}
	for _, tc := range tests {
		got, err := OppositeOf(tc.referenced)
		if tc.wantErr {
			if !errors.Is(err, ErrRollbackUnsupportedRef) {
				t.Fatalf("OppositeOf(%s) erro = %v, quer ErrRollbackUnsupportedRef", tc.referenced, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("OppositeOf(%s) = %s/%v, quer %s", tc.referenced, got, err, tc.want)
		}
	}
}

func TestKindHelpers(t *testing.T) {
	if KindOpening.IsExternal() {
		t.Fatal("OPENING não deveria ser externo")
	}
	if !KindBet.IsExternal() {
		t.Fatal("BET deveria ser externo")
	}
	if !KindRefund.IsReversal() || !KindRollback.IsReversal() {
		t.Fatal("REFUND e ROLLBACK deveriam ser reversões")
	}
	if KindBet.IsReversal() {
		t.Fatal("BET não é reversão")
	}
}

func TestParseHelpers(t *testing.T) {
	if k, err := ParseKind(" bet "); err != nil || k != KindBet {
		t.Fatalf("ParseKind = %v/%v", k, err)
	}
	if _, err := ParseKind("pix"); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("ParseKind inválido = %v", err)
	}
	if s, err := ParseStatus("processed"); err != nil || s != StatusProcessed {
		t.Fatalf("ParseStatus = %v/%v", s, err)
	}
	if _, err := ParseStatus("DONE"); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("ParseStatus inválido = %v", err)
	}
	if o, err := ParseOrigin("internal"); err != nil || o != OriginInternal {
		t.Fatalf("ParseOrigin = %v/%v", o, err)
	}
	if _, err := ParseOrigin("OUTRO"); !errors.Is(err, ErrInvalidOrigin) {
		t.Fatalf("ParseOrigin inválido = %v", err)
	}
}

func TestRehydrateExternalRoundTrip(t *testing.T) {
	original := mustExternal(t, KindBet, "25.00")
	if err := original.MarkProcessed(brl(t, "975.00"), testNow()); err != nil {
		t.Fatalf("setup falhou: %v", err)
	}
	restored, err := Rehydrate(RehydrateParams{
		ID:                    original.ID(),
		Origin:                original.Origin(),
		Kind:                  original.Kind(),
		Status:                original.Status(),
		ProviderID:            original.ProviderID(),
		ExternalTransactionID: original.ExternalTransactionID(),
		IdempotencyKey:        original.IdempotencyKey(),
		PayloadHash:           original.PayloadHash(),
		WalletID:              original.WalletID(),
		PlayerID:              original.PlayerID(),
		RoundID:               original.RoundID(),
		GameID:                original.GameID(),
		Amount:                original.Amount(),
		ResultBalance:         original.ResultBalance(),
		OccurredAt:            original.OccurredAt(),
		CreatedAt:             original.CreatedAt(),
		UpdatedAt:             original.UpdatedAt(),
	})
	if err != nil {
		t.Fatalf("Rehydrate falhou: %v", err)
	}
	if restored.Status() != StatusProcessed {
		t.Fatalf("status = %s, quer PROCESSED", restored.Status())
	}
	if !restored.ResultBalance().Equal(original.ResultBalance()) {
		t.Fatal("saldo resultante não preservado")
	}
	if !restored.IsTerminal() {
		t.Fatal("reidratação deveria preservar estado terminal")
	}
}

func TestRehydrateValidations(t *testing.T) {
	base := func() RehydrateParams {
		return RehydrateParams{
			ID: "tx-1", Origin: OriginExternal, Kind: KindBet, Status: StatusPending,
			ProviderID: "provider-a", ExternalTransactionID: "ext-1",
			IdempotencyKey: "provider-a:ext-1", PayloadHash: "hash-1",
			WalletID: "w-1", PlayerID: "p-1",
			Amount:     brl(t, "25.00"),
			OccurredAt: testNow(), CreatedAt: testNow(), UpdatedAt: testNow(),
		}
	}
	tests := []struct {
		name   string
		mutate func(*RehydrateParams)
		target error
	}{
		{"externo sem provider", func(p *RehydrateParams) { p.ProviderID = "" }, ErrInvalidProviderID},
		{"externo sem idempotency", func(p *RehydrateParams) { p.IdempotencyKey = "" }, ErrInvalidIdempotencyKey},
		{"externo com tipo OPENING", func(p *RehydrateParams) { p.Kind = KindOpening }, ErrOpeningNotExternal},
		{"origem invalida", func(p *RehydrateParams) { p.Origin = "OUTRA" }, ErrInvalidOrigin},
		{"status invalido", func(p *RehydrateParams) { p.Status = "DONE" }, ErrInvalidStatus},
		{"processed sem saldo resultante", func(p *RehydrateParams) { p.Status = StatusProcessed }, money.ErrUninitialized},
		{"rejected sem failure code", func(p *RehydrateParams) { p.Status = StatusRejected }, ErrInvalidFailureCode},
		{"sem timestamps", func(p *RehydrateParams) { p.UpdatedAt = time.Time{} }, ErrInvalidTimestamp},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := base()
			tc.mutate(&p)
			if _, err := Rehydrate(p); !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestNewOpeningSetsResultBalance(t *testing.T) {
	tx, err := NewOpening(OpeningParams{
		ID: "op-1", WalletID: "w-1", PlayerID: "p-1", Money: brl(t, "1000.00"), Now: testNow(),
	})
	if err != nil {
		t.Fatalf("NewOpening falhou: %v", err)
	}
	if !tx.ResultBalance().IsValid() {
		t.Fatal("OPENING em PROCESSED precisa ter resultBalance válido")
	}
	if !tx.ResultBalance().Equal(brl(t, "1000.00")) {
		t.Fatalf("resultBalance = %s, quer 1000.00", tx.ResultBalance().String())
	}
}

func TestResolveReferenceAllowedFromPending(t *testing.T) {
	tx := mustExternal(t, KindRefund, "25.00")
	if err := tx.ResolveReference("tx-ref-1", testNow()); err != nil {
		t.Fatalf("resolução a partir de PENDING deveria ser aceita: %v", err)
	}
	if tx.ReferenceTransactionID() != "tx-ref-1" {
		t.Fatalf("referência = %q, quer tx-ref-1", tx.ReferenceTransactionID())
	}
	if tx.Status() != StatusPending {
		t.Fatalf("status não deveria mudar na resolução: %s", tx.Status())
	}
}
