#!/usr/bin/env bash
# Prova automatizada das garantias do schema (seção 5 do enunciado).
#
# Roda num banco DESCARTÁVEL: cria, aplica as migrations, tenta violar cada
# invariante e verifica não só que a operação falha, mas que ela falha pelo
# motivo CERTO (o nome da constraint/índice/trigger aparece no erro).
# Ao final, aplica `migrate down` e confirma que o schema foi removido.
set -uo pipefail

ADMIN_DSN="${ADMIN_DSN:-postgres://wager:wager@localhost:5432/postgres?sslmode=disable}"
CHECKS_DB="${CHECKS_DB:-wager_schema_checks}"
DSN="${DSN:-postgres://wager:wager@localhost:5432/${CHECKS_DB}?sslmode=disable}"

FAILED=0
pass()    { printf '  \033[32mOK  \033[0m %s\n' "$1"; }
fail()    { printf '  \033[31mFALHA\033[0m %s\n' "$1"; FAILED=1; }
section() { printf '\n\033[1m%s\033[0m\n' "$1"; }

psql_admin() { psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -q -A -t "$@"; }

expect_ok() {
  local desc="$1" sql="$2"
  local out
  if out="$(psql "$DSN" -v ON_ERROR_STOP=1 -q -A -t -c "$sql" 2>&1)"; then
    pass "$desc"
  else
    fail "$desc — comando deveria ser aceito: $(printf '%s' "$out" | head -1)"
  fi
}

expect_fail() {
  local desc="$1" sql="$2" want="${3:-}"
  local out
  if out="$(psql "$DSN" -v ON_ERROR_STOP=1 -q -A -t -c "$sql" 2>&1)"; then
    fail "$desc — comando foi ACEITO e deveria ser rejeitado"
    return
  fi
  if [[ -n "$want" && "$out" != *"$want"* ]]; then
    fail "$desc — rejeitado pelo motivo ERRADO (esperado '$want'): $(printf '%s' "$out" | head -1)"
    return
  fi
  pass "$desc"
}

# ── Banco descartável ────────────────────────────────────────────────
section "Preparação"
psql_admin -c "DROP DATABASE IF EXISTS ${CHECKS_DB}" >/dev/null 2>&1
if psql_admin -c "CREATE DATABASE ${CHECKS_DB}" >/dev/null 2>&1; then
  pass "banco descartável ${CHECKS_DB} criado"
else
  fail "não foi possível criar ${CHECKS_DB} — o Postgres do Compose está no ar?"
  exit 1
fi

if go run ./cmd/migrate up -dsn "$DSN" >/dev/null 2>&1; then
  pass "migrate up aplicado"
else
  fail "migrate up falhou"
  exit 1
fi

# ── Fixtures válidas ─────────────────────────────────────────────────
W1='00000000-0000-7000-8000-000000000001'
W2='00000000-0000-7000-8000-000000000002'
BET1='00000000-0000-7000-8000-0000000000b1'
LED1='00000000-0000-7000-8000-0000000000e1'

section "Caminhos válidos (controle positivo)"
expect_ok "carteira válida aceita" "
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ('$W1', 'player-1', 'BRL', 10000, 1, now(), now())"

expect_ok "segunda carteira (mesmo jogador, outra moeda) aceita" "
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ('$W2', 'player-1', 'USD', 5000, 1, now(), now())"

expect_ok "BET PROCESSED aceita" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   round_id, game_id, resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('$BET1', 'EXTERNAL', 'BET', 'PROCESSED', '$W1', 'player-1', 'BRL', 2500,
   'provider-a', 'ext-bet-1', 'provider-a:ext-bet-1', 'hash-bet-1',
   'round-1', 'game-1', 7500, now(), now(), now())"

expect_ok "lançamento de débito coerente aceito" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('$LED1', '$W1', '$BET1', 'DEBIT', 'BRL', 2500, 10000, 7500, now())"

expect_ok "REFUND PROCESSED sobre a BET aceito" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   round_id, game_id, external_reference_id, reference_transaction_id,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000c1', 'EXTERNAL', 'REFUND', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 2500, 'provider-a', 'ext-refund-1',
   'provider-a:ext-refund-1', 'hash-refund-1', 'round-1', 'game-1',
   'ext-bet-1', '$BET1', 10000, now(), now(), now())"

section "Carteira"
expect_fail "saldo negativo rejeitado pelo schema" "
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f1', 'player-neg', 'BRL', -1, 1, now(), now())" \
"wallets_balance_non_negative"

expect_fail "moeda fora do ISO 4217 rejeitada" "
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f2', 'player-cur', 'brl', 1, 1, now(), now())" \
"wallets_currency_iso4217"

expect_fail "carteira duplicada para (playerId, moeda) rejeitada" "
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f3', 'player-1', 'BRL', 1, 1, now(), now())" \
"wallets_player_currency_uniq"

expect_fail "versão zero rejeitada" "
INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f4', 'player-v0', 'BRL', 1, 0, now(), now())" \
"wallets_version_positive"

section "Transações — política de valor por tipo"
expect_fail "BET com valor zero rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f5', 'EXTERNAL', 'BET', 'PENDING',
   '$W1', 'player-1', 'BRL', 0, 'provider-a', 'ext-zero',
   'provider-a:ext-zero', 'h', now(), now(), now())" \
"wt_positive_amount_kinds"

expect_fail "LOSS com valor positivo rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f6', 'EXTERNAL', 'LOSS', 'PENDING',
   '$W1', 'player-1', 'BRL', 100, 'provider-a', 'ext-loss',
   'provider-a:ext-loss', 'h', now(), now(), now())" \
"wt_loss_zero_amount"

expect_fail "abertura com valor zero rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f7', 'INTERNAL', 'OPENING', 'PROCESSED',
   '$W2', 'player-1', 'USD', 0, now(), now(), now())" \
"wt_opening_positive_amount"

section "Transações — origem, metadados e estados"
expect_fail "OPENING enviada como externa rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f8', 'EXTERNAL', 'OPENING', 'PENDING',
   '$W2', 'player-1', 'USD', 100, 'provider-a', 'ext-op',
   'provider-a:ext-op', 'h', now(), now(), now())" \
"wt_opening_is_internal"

expect_fail "externa sem providerId rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000f9', 'EXTERNAL', 'BET', 'PENDING',
   '$W1', 'player-1', 'BRL', 100, 'ext-x', 'k-x', 'h', now(), now(), now())" \
"wt_external_metadata_required"

expect_fail "interna com metadados externos rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000fa', 'INTERNAL', 'OPENING', 'PROCESSED',
   '$W2', 'player-1', 'USD', 100, 'provider-a', now(), now(), now())" \
"wt_internal_metadata_absent"

expect_fail "REFUND sem referenceExternalTransactionId rejeitado" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000fb', 'EXTERNAL', 'REFUND', 'PENDING',
   '$W1', 'player-1', 'BRL', 2500, 'provider-a', 'ext-ref-noref',
   'provider-a:ext-ref-noref', 'h', now(), now(), now())" \
"wt_reversal_requires_external_reference"

expect_fail "PROCESSED sem resultBalance rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000fc', 'EXTERNAL', 'BET', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 100, 'provider-a', 'ext-noresult',
   'provider-a:ext-noresult', 'h', now(), now(), now())" \
"wt_processed_requires_result_balance"

expect_fail "REJECTED sem failureCode rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000fd', 'EXTERNAL', 'BET', 'REJECTED',
   '$W1', 'player-1', 'BRL', 100, 'provider-a', 'ext-nofail',
   'provider-a:ext-nofail', 'h', now(), now(), now())" \
"wt_failure_code_when_terminal_negative"

expect_fail "estado desconhecido rejeitado" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-0000000000fe', 'EXTERNAL', 'BET', 'DONE',
   '$W1', 'player-1', 'BRL', 100, 'provider-a', 'ext-badstatus',
   'provider-a:ext-badstatus', 'h', now(), now(), now())" \
"wt_status_valid"

section "Idempotência (o banco decide a corrida)"
expect_fail "chave de idempotência duplicada no mesmo provedor rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000101', 'EXTERNAL', 'BET', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 2500, 'provider-a', 'ext-outro-id',
   'provider-a:ext-bet-1', 'hash-outro', 7500, now(), now(), now())" \
"wt_provider_idempotency_key_uniq"

expect_fail "mesma operação externa com outra chave rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000102', 'EXTERNAL', 'BET', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 2500, 'provider-a', 'ext-bet-1',
   'provider-a:chave-diferente-102', 'hash-dif', 7500, now(), now(), now())" \
"wt_provider_external_id_uniq"

expect_ok "mesma string de chave em provedor DIFERENTE é aceita (índice é por provedor)" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000110', 'EXTERNAL', 'BET', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 500, 'provider-b', 'ext-bet-b1',
   'provider-a:ext-bet-1', 'hash-b1', 7000, now(), now(), now())"

section "Abertura e reversões"
expect_ok "primeira OPENING da carteira aceita" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000103', 'INTERNAL', 'OPENING', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 100, 10100, now(), now(), now())"

expect_fail "segunda OPENING para a mesma carteira rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000111', 'INTERNAL', 'OPENING', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 200, 10300, now(), now(), now())" \
"wt_opening_per_wallet_uniq"

expect_fail "segunda reversão bem-sucedida da mesma referência rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   round_id, game_id, external_reference_id, reference_transaction_id,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000104', 'EXTERNAL', 'ROLLBACK', 'PROCESSED',
   '$W1', 'player-1', 'BRL', 2500, 'provider-a', 'ext-rollback-1',
   'provider-a:ext-rollback-1', 'hash-rollback-1', 'round-1', 'game-1',
   'ext-bet-1', '$BET1', 12500, now(), now(), now())" \
"wt_single_successful_reversal_per_reference"

section "Ledger append-only"
expect_fail "segundo lançamento para a mesma (carteira, transação) rejeitado" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('00000000-0000-7000-8000-000000000105', '$W1', '$BET1', 'DEBIT', 'BRL',
   2500, 10000, 7500, now())" \
"wle_wallet_transaction_uniq"

expect_fail "lançamento com direção incoerente rejeitado" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('00000000-0000-7000-8000-000000000106', '$W1', '$BET1', 'DEBIT', 'BRL',
   2500, 10000, 8000, now())" \
"wle_direction_consistent"

expect_fail "lançamento com valor zero rejeitado" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('00000000-0000-7000-8000-000000000107', '$W1', '$BET1', 'DEBIT', 'BRL',
   0, 10000, 10000, now())" \
"wle_amount_positive"

expect_fail "lançamento resultando em saldo negativo rejeitado" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('00000000-0000-7000-8000-000000000108', '$W1', '$BET1', 'DEBIT', 'BRL',
   100, 50, -50, now())" \
"wle_balance_after_non_negative"

expect_fail "UPDATE no ledger bloqueado pelo trigger" "
UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE id = '$LED1'" \
"append-only"

expect_fail "DELETE no ledger bloqueado pelo trigger" "
DELETE FROM wallet_ledger_entries WHERE id = '$LED1'" \
"append-only"

expect_fail "transação com moeda diferente da carteira rejeitada" "
INSERT INTO wager_transactions
  (id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
   provider_id, external_transaction_id, idempotency_key, payload_hash,
   resulting_balance_minor, occurred_at, created_at, updated_at)
VALUES ('00000000-0000-7000-8000-000000000112', 'EXTERNAL', 'BET', 'PROCESSED',
   '$W1', 'player-1', 'USD', 500, 'provider-c', 'ext-c1',
   'provider-c:ext-c1', 'hash-c1', 7000, now(), now(), now())" \
"wt_wallet_currency_fk"

expect_fail "lançamento com moeda diferente da carteira rejeitado" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('00000000-0000-7000-8000-000000000113', '$W1',
   '00000000-0000-7000-8000-000000000103', 'DEBIT', 'USD',
   100, 10000, 9900, now())" \
"wle_wallet_currency_fk"

expect_fail "lançamento de carteira diferente da transação rejeitado" "
INSERT INTO wallet_ledger_entries
  (id, wallet_id, transaction_id, direction, currency, amount_minor,
   balance_before_minor, balance_after_minor, created_at)
VALUES ('00000000-0000-7000-8000-000000000114', '$W2',
   '00000000-0000-7000-8000-000000000103', 'DEBIT', 'USD',
   2500, 5000, 2500, now())" \
"wle_transaction_wallet_fk"

section "Inbox e outbox"
expect_ok "registro de inbox aceito" "
INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
VALUES ('wager-transactions-consumer', 'msg-1', 'hash-msg-1', now())"

expect_fail "inbox duplicada por (consumer, messageId) rejeitada" "
INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
VALUES ('wager-transactions-consumer', 'msg-1', 'hash-msg-1', now())" \
"inbox_consumer_message_uniq"

expect_ok "evento de outbox aceito" "
INSERT INTO outbox_events
  (id, event_id, aggregate_id, event_type, event_version, payload, occurred_at,
   next_attempt_at, created_at)
VALUES (DEFAULT, '00000000-0000-7000-8000-0000000001a1', '$W1',
   'WalletBalanceChanged', 1, '{\"walletId\":\"x\"}'::jsonb, now(), now(), now())"

expect_fail "eventId duplicado na outbox rejeitado" "
INSERT INTO outbox_events
  (id, event_id, aggregate_id, event_type, event_version, payload, occurred_at,
   next_attempt_at, created_at)
VALUES (DEFAULT, '00000000-0000-7000-8000-0000000001a1', '$W1',
   'WalletBalanceChanged', 1, '{\"walletId\":\"x\"}'::jsonb, now(), now(), now())" \
"outbox_event_id_uniq"

# ── Reversão das migrations ──────────────────────────────────────────
section "Reversão (migrate down)"
if go run ./cmd/migrate down -dsn "$DSN" >/dev/null 2>&1; then
  pass "migrate down aplicado"
else
  fail "migrate down falhou"
fi

left="$(psql "$DSN" -v ON_ERROR_STOP=1 -q -A -t -c \
  "SELECT count(*) FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name IN ('wallets','wager_transactions','wallet_ledger_entries','inbox_messages','outbox_events')" 2>/dev/null)"
if [[ "$left" == "0" ]]; then
  pass "todas as tabelas do domínio removidas"
else
  fail "restaram ${left} tabelas após o down"
fi

# ── Limpeza ──────────────────────────────────────────────────────────
section "Limpeza"
psql_admin -c "DROP DATABASE IF EXISTS ${CHECKS_DB}" >/dev/null 2>&1 && pass "banco descartável removido"

printf '\n'
if [[ "$FAILED" == "0" ]]; then
  printf '\033[32m%s\033[0m\n' "TODAS AS VERIFICAÇÕES PASSARAM"
  exit 0
fi
printf '\033[31m%s\033[0m\n' "HOUVE FALHAS"
exit 1
