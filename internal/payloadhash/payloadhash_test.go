package payloadhash

import (
	"errors"
	"strings"
	"testing"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.ParseDecimal(s, "BRL")
	if err != nil {
		t.Fatalf("setup money(%q): %v", s, err)
	}
	return m
}

func baseInput(t *testing.T) Input {
	t.Helper()
	return Input{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Money:                 brl(t, "25.00"),
	}
}

// Trava o contrato: bytes exatos do JSON canônico (chaves ordenadas).
func TestCanonicalJSONExactBytes(t *testing.T) {
	got, err := CanonicalJSON(baseInput(t))
	if err != nil {
		t.Fatalf("CanonicalJSON falhou: %v", err)
	}
	want := `{"amount":"25.00","currency":"BRL","externalReferenceTransactionId":"","externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	if string(got) != want {
		t.Fatalf("JSON canônico divergente.\n got: %s\nwant: %s", got, want)
	}
}

func TestComputeIsDeterministic(t *testing.T) {
	in := baseInput(t)
	first, err := Compute(in)
	if err != nil {
		t.Fatalf("Compute falhou: %v", err)
	}
	second, err := Compute(in)
	if err != nil {
		t.Fatalf("Compute falhou: %v", err)
	}
	if first != second {
		t.Fatalf("hash não determinístico: %s != %s", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("hash deveria ter 64 caracteres hex, tem %d", len(first))
	}
	if first != strings.ToLower(first) {
		t.Fatalf("hash deveria ser hexadecimal minúsculo: %s", first)
	}
}

// Normalizações: formas equivalentes produzem o MESMO hash.
func TestNormalizationEquivalences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{"amount sem casas decimais", func(in *Input) { in.Money = brl(t, "25") }},
		{"amount com uma casa", func(in *Input) { in.Money = brl(t, "25.0") }},
		{"amount com zeros à esquerda", func(in *Input) { in.Money = brl(t, "025.00") }},
		{"kind em minúsculas", func(in *Input) { in.Kind = "bet" }},
		{"kind com espaços", func(in *Input) { in.Kind = "  BET  " }},
		{"providerId com espaços", func(in *Input) { in.ProviderID = "  provider-a " }},
		{"referência ausente vs vazia", func(in *Input) { in.ExternalReferenceID = "" }},
	}

	want, err := Compute(baseInput(t))
	if err != nil {
		t.Fatalf("Compute base falhou: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := baseInput(t)
			tc.mutate(&in)
			got, err := Compute(in)
			if err != nil {
				t.Fatalf("Compute falhou: %v", err)
			}
			if got != want {
				t.Fatalf("hash deveria ser igual após normalização: %s != %s", got, want)
			}
		})
	}
}

// Qualquer mudança em campo de negócio muda o hash.
func TestFieldChangesAlterHash(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{"provider", func(in *Input) { in.ProviderID = "provider-b" }},
		{"externalTransactionId", func(in *Input) { in.ExternalTransactionID = "transaction-124" }},
		{"player", func(in *Input) { in.PlayerID = "outro-jogador" }},
		{"wallet", func(in *Input) { in.WalletID = "outra-carteira" }},
		{"round", func(in *Input) { in.RoundID = "round-988" }},
		{"game", func(in *Input) { in.GameID = "outro-jogo" }},
		{"kind", func(in *Input) { in.Kind = "WIN" }},
		{"amount", func(in *Input) { in.Money = brl(t, "25.01") }},
		{"referência presente", func(in *Input) { in.ExternalReferenceID = "ext-bet-1" }},
	}

	base, err := Compute(baseInput(t))
	if err != nil {
		t.Fatalf("Compute base falhou: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := baseInput(t)
			tc.mutate(&in)
			got, err := Compute(in)
			if err != nil {
				t.Fatalf("Compute falhou: %v", err)
			}
			if got == base {
				t.Fatalf("mudança em %s deveria alterar o hash", tc.name)
			}
		})
	}
}

// Equivalência HTTP <-> SQS: duas origens diferentes que carregam os mesmos
// campos de negócio (e metadados de transporte distintos) produzem o mesmo hash.
func TestHTTPAndSQSProduceSameHash(t *testing.T) {
	// Corpo HTTP (sem idempotencyKey, que vem no header).
	httpInput := Input{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Money:                 brl(t, "25.00"),
	}

	// Envelope SQS: mesmos campos de negócio + idempotencyKey + messageId,
	// que NÃO entram no hash.
	sqsInput := httpInput
	sqsInput.Money = brl(t, "25")

	httpHash, err := Compute(httpInput)
	if err != nil {
		t.Fatalf("Compute HTTP falhou: %v", err)
	}
	sqsHash, err := Compute(sqsInput)
	if err != nil {
		t.Fatalf("Compute SQS falhou: %v", err)
	}
	if httpHash != sqsHash {
		t.Fatalf("HTTP e SQS deveriam produzir o mesmo hash: %s != %s", httpHash, sqsHash)
	}
}

func TestComputeRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
		target error
	}{
		{"provider vazio", func(in *Input) { in.ProviderID = "" }, ErrMissingField},
		{"provider só espaços", func(in *Input) { in.ProviderID = "   " }, ErrMissingField},
		{"external id vazio", func(in *Input) { in.ExternalTransactionID = "" }, ErrMissingField},
		{"player vazio", func(in *Input) { in.PlayerID = "" }, ErrMissingField},
		{"wallet vazio", func(in *Input) { in.WalletID = "" }, ErrMissingField},
		{"round vazio", func(in *Input) { in.RoundID = "" }, ErrMissingField},
		{"game vazio", func(in *Input) { in.GameID = "" }, ErrMissingField},
		{"kind desconhecido", func(in *Input) { in.Kind = "PIX" }, ErrInvalidKind},
		{"kind OPENING", func(in *Input) { in.Kind = "OPENING" }, ErrInternalKind},
		{"money não inicializado", func(in *Input) { in.Money = money.Money{} }, ErrInvalidMoney},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := baseInput(t)
			tc.mutate(&in)
			_, err := Compute(in)
			if err == nil {
				t.Fatal("Compute deveria falhar")
			}
			if !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}
