package worker

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/payloadhash"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// documentedBody é o exemplo de mensagem da seção 10 do README, usado como
// contrato de referência: se o parser deixar de aceitar este corpo, o contrato
// pedido pelo desafio foi quebrado.
const documentedBody = `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

func TestParseInboundTransactionReadsTheDocumentedContract(t *testing.T) {
	parsed, err := ParseInboundTransaction([]byte(documentedBody))
	if err != nil {
		t.Fatalf("ParseInboundTransaction: %v", err)
	}

	if parsed.MessageID != "msg-123" {
		t.Fatalf("messageId = %q, quer %q", parsed.MessageID, "msg-123")
	}
	wantTime := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	if !parsed.OccurredAt.Equal(wantTime) {
		t.Fatalf("occurredAt = %s, quer %s", parsed.OccurredAt, wantTime)
	}

	cmd := parsed.Command
	fields := []struct{ name, got, want string }{
		{"providerId", cmd.ProviderID, "provider-a"},
		{"externalTransactionId", cmd.ExternalTransactionID, "transaction-123"},
		{"idempotencyKey", cmd.IdempotencyKey, "provider-a:transaction-123"},
		{"playerId", cmd.PlayerID, "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"},
		{"walletId", cmd.WalletID, "0192f291-27dd-7d3f-8071-5f8685deef37"},
		{"roundId", cmd.RoundID, "round-987"},
		{"gameId", cmd.GameID, "fortune-chimp"},
		{"kind", cmd.Kind, "BET"},
		{"referenceExternalId", cmd.ReferenceExternalID, ""},
	}
	for _, f := range fields {
		if f.got != f.want {
			t.Errorf("%s = %q, quer %q", f.name, f.got, f.want)
		}
	}

	if cmd.Money.String() != "25.00" || cmd.Money.Currency() != "BRL" {
		t.Fatalf("money = %s %s, quer 25.00 BRL", cmd.Money.String(), cmd.Money.Currency())
	}
	if cmd.Money.Amount() != 2500 {
		t.Fatalf("amount em unidades mínimas = %d, quer 2500", cmd.Money.Amount())
	}

	// O correlationId liga a mensagem do broker aos eventos gerados por ela.
	if cmd.CorrelationID != "msg-123" {
		t.Fatalf("correlationId = %q, quer o messageId %q", cmd.CorrelationID, "msg-123")
	}

	// O parser já devolve o hash do corpo bruto: é o que a inbox grava e
	// verifica em reentregas, sem depender de quem chama.
	if parsed.PayloadHash != PayloadHashOf([]byte(documentedBody)) {
		t.Fatalf("payloadHash = %q, quer o hash do corpo recebido", parsed.PayloadHash)
	}
}

func TestParseInboundTransactionRejectsInvalidMessages(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(env map[string]any)
		target error
	}{
		{"json inválido", nil, ErrInvalidEnvelope},
		{"sem messageId", func(env map[string]any) { delete(env, "messageId") }, ErrMissingField},
		{"messageId vazio", func(env map[string]any) { env["messageId"] = "   " }, ErrMissingField},
		{"tipo desconhecido", func(env map[string]any) { env["type"] = "OutroEvento" }, ErrUnknownType},
		{"sem type", func(env map[string]any) { delete(env, "type") }, ErrUnknownType},
		{"sem occurredAt", func(env map[string]any) { delete(env, "occurredAt") }, ErrInvalidOccurredAt},
		{"occurredAt inválido", func(env map[string]any) { env["occurredAt"] = "08/09/2026" }, ErrInvalidOccurredAt},
		{"sem data", func(env map[string]any) { delete(env, "data") }, ErrMissingField},
		{"sem idempotencyKey", func(env map[string]any) { deleteInData(env, "idempotencyKey") }, ErrMissingField},
		{"sem walletId", func(env map[string]any) { deleteInData(env, "walletId") }, ErrMissingField},
		{"sem roundId", func(env map[string]any) { deleteInData(env, "roundId") }, ErrMissingField},
		{"sem gameId", func(env map[string]any) { deleteInData(env, "gameId") }, ErrMissingField},
		{"sem money", func(env map[string]any) { deleteInData(env, "money") }, ErrMissingField},
		// Ponto crítico do desafio: dinheiro nunca entra como ponto flutuante.
		{"money como número decimal", func(env map[string]any) { setMoneyAmount(env, json.Number("25.0")) }, ErrInvalidMoney},
		{"money como número inteiro", func(env map[string]any) { setMoneyAmount(env, json.Number("25")) }, ErrInvalidMoney},
		{"money notação científica", func(env map[string]any) { setMoneyAmount(env, json.Number("1e3")) }, ErrInvalidMoney},
		{"money string inválida", func(env map[string]any) { setMoneyAmount(env, "25,00") }, ErrInvalidMoney},
		{"money com três casas", func(env map[string]any) { setMoneyAmount(env, "25.001") }, ErrInvalidMoney},
		// A moeda e validada por FORMA (3 letras A-Z), nao por existencia na
		// tabela ISO 4217 - a mesma regra do schema (currency ~ '^[A-Z]{3}$'):
		{"moeda minuscula", func(env map[string]any) { moneyOf(env)["currency"] = "brl" }, ErrInvalidMoney},
		{"moeda com dois caracteres", func(env map[string]any) { moneyOf(env)["currency"] = "BR" }, ErrInvalidMoney},
		{"moeda com quatro caracteres", func(env map[string]any) { moneyOf(env)["currency"] = "BRLL" }, ErrInvalidMoney},
		{"moeda com digito", func(env map[string]any) { moneyOf(env)["currency"] = "B1L" }, ErrInvalidMoney},
		{"kind desconhecido", func(env map[string]any) { env["data"].(map[string]any)["kind"] = "SUPER_BET" }, ErrInvalidKind},
		{"data não é objeto", func(env map[string]any) { env["data"] = "texto" }, ErrInvalidData},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := documentedBody
			if tt.mutate != nil {
				body = mutateBody(t, tt.mutate)
			} else {
				body = "{ isso não é json"
			}
			_, err := ParseInboundTransaction([]byte(body))
			if !errors.Is(err, tt.target) {
				t.Fatalf("erro = %v, quer %v", err, tt.target)
			}
		})
	}
}

// TestInboundAndHTTPProduceTheSameIdempotencyHash prova que as duas entradas
// compartilham a impressão digital da operação — é isso que faz a movimentação
// ser única mesmo que o mesmo pedido chegue por HTTP e por SQS
// concorrentemente.
func TestInboundAndHTTPProduceTheSameIdempotencyHash(t *testing.T) {
	parsed, err := ParseInboundTransaction([]byte(documentedBody))
	if err != nil {
		t.Fatalf("ParseInboundTransaction: %v", err)
	}

	// Corpo HTTP equivalente: mesmos campos de negócio, chave no header
	// Idempotency-Key e amount "25" (a normalização documentada faz "25" e
	// "25.00" produzirem a mesma forma canônica).
	httpCommand := usecase.ProcessTransactionCommand{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Money:                 mustMoney(t, "25"),
	}

	sqsHash, err := payloadhash.Compute(hashInput(parsed.Command))
	if err != nil {
		t.Fatalf("hash da entrada SQS: %v", err)
	}
	httpHash, err := payloadhash.Compute(hashInput(httpCommand))
	if err != nil {
		t.Fatalf("hash da entrada HTTP: %v", err)
	}
	if sqsHash != httpHash {
		t.Fatalf("hash divergente entre HTTP e SQS: %s != %s", httpHash, sqsHash)
	}

	// A chave de idempotência não participa do hash: mudá-la não muda a
	// impressão digital da operação.
	otherKey := parsed.Command
	otherKey.IdempotencyKey = "provider-a:outra-chave"
	otherHash, err := payloadhash.Compute(hashInput(otherKey))
	if err != nil {
		t.Fatalf("hash com outra chave: %v", err)
	}
	if otherHash != sqsHash {
		t.Fatal("a chave de idempotência não deveria entrar no hash de negócio")
	}
}

// TestWellFormedUnknownCurrencyPassesTheParser documenta uma escolha de
// CONTRATO, e nao um descuido. A moeda e validada por FORMA (3 letras A-Z),
// exatamente como o schema (currency ~ '^[A-Z]{3}$'): a aplicacao nao
// replica a tabela ISO 4217. Um codigo bem formado porem inexistente (XYZ)
// passa o parser e e recusado depois, pela carteira, como WALLET_NOT_FOUND
// - falha igualmente PERMANENTE, que vai para a DLQ sem consumir tentativas.
// Validar a existencia do codigo exigiria embutir a tabela ISO; preferiu-se
// manter a MESMA regra do banco, para que entrada e schema concordem.
func TestWellFormedUnknownCurrencyPassesTheParser(t *testing.T) {
	body := mutateBody(t, func(env map[string]any) { moneyOf(env)["currency"] = "XYZ" })
	parsed, err := ParseInboundTransaction([]byte(body))
	if err != nil {
		t.Fatalf("moeda bem formada deveria passar o parser: %v", err)
	}
	if got := parsed.Command.Money.Currency(); got != "XYZ" {
		t.Fatalf("currency = %q, quer XYZ", got)
	}
}

func TestPayloadHashOfTracksRawBytes(t *testing.T) {
	// Reentrega legítima: o broker preserva o corpo, então o hash é o mesmo.
	if PayloadHashOf([]byte(documentedBody)) != PayloadHashOf([]byte(documentedBody)) {
		t.Fatal("o mesmo corpo deveria produzir o mesmo hash")
	}
	// Corpo reencodado: hash diferente => tratado como mensagem diferente.
	reencoded := strings.Replace(documentedBody, `"msg-123"`, `"msg-123" `, 1)
	if PayloadHashOf([]byte(documentedBody)) == PayloadHashOf([]byte(reencoded)) {
		t.Fatal("corpos diferentes deveriam produzir hashes diferentes")
	}
	if len(PayloadHashOf([]byte(documentedBody))) != 64 {
		t.Fatal("o hash deveria ter 64 caracteres hexadecimais (sha256)")
	}
}

// ── helpers ──────────────────────────────────────────────────────────

// mutateBody aplica uma mutação sobre o corpo documentado e devolve o JSON
// resultante, com UseNumber para preservar a distinção entre string e número.
func mutateBody(t *testing.T, mutate func(env map[string]any)) string {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(documentedBody))
	decoder.UseNumber()
	var env map[string]any
	if err := decoder.Decode(&env); err != nil {
		t.Fatalf("decodificando o corpo documentado: %v", err)
	}
	mutate(env)
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("serializando o corpo mutado: %v", err)
	}
	return string(raw)
}

func dataOf(env map[string]any) map[string]any {
	return env["data"].(map[string]any)
}

func deleteInData(env map[string]any, field string) { delete(dataOf(env), field) }

func moneyOf(env map[string]any) map[string]any {
	return dataOf(env)["money"].(map[string]any)
}

func setMoneyAmount(env map[string]any, amount any) { moneyOf(env)["amount"] = amount }

// hashInput espelha o que usecase.prepare monta para o hash canônico. Mantido
// no teste de propósito: se o caso de uso mudar os campos do hash, este teste
// falha e força a atualização consciente do contrato.
func hashInput(cmd usecase.ProcessTransactionCommand) payloadhash.Input {
	return payloadhash.Input{
		ProviderID:            cmd.ProviderID,
		ExternalTransactionID: cmd.ExternalTransactionID,
		PlayerID:              cmd.PlayerID,
		WalletID:              cmd.WalletID,
		RoundID:               cmd.RoundID,
		GameID:                cmd.GameID,
		Kind:                  cmd.Kind,
		Money:                 cmd.Money,
		ExternalReferenceID:   cmd.ReferenceExternalID,
	}
}

func mustMoney(t *testing.T, value string) money.Money {
	t.Helper()
	parsed, err := money.ParseDecimal(value, "BRL")
	if err != nil {
		t.Fatalf("money(%q): %v", value, err)
	}
	return parsed
}
