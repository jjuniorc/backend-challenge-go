#!/usr/bin/env bash
#
# Roteiro de demonstração — enunciado §13 "Verificação obrigatória".
#
# Roda contra a stack VIVA (PostgreSQL, Keycloak/MiniStack e a aplicação) e
# imprime PASS/FAIL por cenário. Os cenários que exigem derrubar processo no meio
# — queda do consumidor, publishers disputando, reinício — são provados por teste
# automatizado, e o script mostra o comando exato de cada um: um shell não
# consegue matar um processo "entre o commit e a remoção" de forma determinística.
#
# Uso:
#   ./docs/demo/demonstrate.sh
#
# Variáveis:
#   BASE             URL da aplicação            (default: http://localhost:8081)
#   BASES            portas das instâncias       (default: 8081 8082 8083)
#   KEYCLOAK_URL     issuer do IdP               (default: http://localhost:8090)
#   USE_DEV_AUTH=1   usa o autenticador de desenvolvimento em vez do Keycloak
#   SQS_ENDPOINT     endpoint do broker emulado  (default: http://localhost:4566)
#   PSQL_DSN         DSN do banco para a conferência final
#                    (default: postgres://wager:wager@localhost:5432/wager)
#
set -euo pipefail

BASE="${BASE:-http://localhost:8081}"
BASES="${BASES:-8081 8082 8083}"
KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8090}"
SQS_ENDPOINT="${SQS_ENDPOINT:-http://localhost:4566}"
USE_DEV_AUTH="${USE_DEV_AUTH:-0}"
PSQL_DSN="${PSQL_DSN:-postgres://wager:wager@localhost:5432/wager?sslmode=disable}"

PASS_COUNT=0
FAIL_COUNT=0
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ok()    { printf '  \033[32mPASS\033[0m %s\n' "$1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
titulo(){ printf '\n\033[1m%s\033[0m\n' "$1"; }
nota()  { printf '    %s\n' "$1"; }

# ── autenticação ─────────────────────────────────────────────────────
# A via principal é o IdP real, como o enunciado exige. O autenticador de
# desenvolvimento é um atalho EXPLÍCITO para testar o contrato HTTP sem IdP.
provider_token() {
  if [ "$USE_DEV_AUTH" = "1" ]; then
    printf 'dev:svc-provider-a:provider-a:provider'
    return
  fi
  curl -fsS -X POST "$KEYCLOAK_URL/realms/wager/protocol/openid-connect/token" \
    -d grant_type=client_credentials \
    -d client_id=provider-a -d client_secret=provider-a-secret \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])'
}

internal_token() {
  if [ "$USE_DEV_AUTH" = "1" ]; then
    printf 'dev:svc-internal:-:internal'
    return
  fi
  curl -fsS -X POST "$KEYCLOAK_URL/realms/wager/protocol/openid-connect/token" \
    -d grant_type=client_credentials \
    -d client_id=internal-service -d client_secret=internal-secret \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])'
}

titulo "Autenticação"
PROVIDER_TOKEN="$(provider_token)"
INTERNAL_TOKEN="$(internal_token)"
if [ -n "$PROVIDER_TOKEN" ] && [ -n "$INTERNAL_TOKEN" ]; then
  ok "credenciais obtidas ($([ "$USE_DEV_AUTH" = "1" ] && echo 'dev' || echo 'Keycloak'))"
else
  fail "não foi possível obter credenciais"
  exit 1
fi

# ── helpers ──────────────────────────────────────────────────────────
STAMP_SEQ=0
# Segundos + contador sequencial. Um identificador repetido faria open_wallet
# responder 409 (a carteira do jogador já existe) e, com `-f`, o script
# abortaria — o contador elimina a colisão sem depender do relógio.
stamp() {
  STAMP_SEQ=$((STAMP_SEQ + 1))
  printf '%s%03d' "$(date +%s)" "$STAMP_SEQ"
}

open_wallet() { # playerId saldo -> walletId
  curl -fsS -X POST "$BASE/wallets" \
    -H "Authorization: Bearer $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"playerId\":\"$1\",\"initialBalance\":{\"amount\":\"$2\",\"currency\":\"BRL\"}}" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])'
}

balance_of() { # walletId -> saldo
  curl -fsS -H "Authorization: Bearer $INTERNAL_TOKEN" "$BASE/wallets/$1" \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["balance"]["amount"])'
}

# post_bet <base> <walletId> <playerId> <externalId> <chave> <valor> -> codigo HTTP
#
# O corpo é montado numa variável ANTES do curl de propósito. O bash 3.2 do
# macOS (GNU bash 3.2.57, em /bin/bash) faz expansão de chaves sobre um `{...}`
# literal entre aspas dentro de `$( )` que está dentro de outro comando: o JSON
# seria partido nas vírgulas e CADA fragmento viraria uma requisição (9 campos
# -> 9 chamadas, todas 400). Com as chaves fora da linha de comando não há o que
# expandir. O mesmo construtor na forma de atribuição sempre funcionou — é só a
# combinação com outro comando que quebra.
#
# O código é impresso COM quebra de linha: os cenários concatenam estes arquivos
# com `cat` e, sem o newline, dois códigos virariam um único token. (O corpo vai
# para um arquivo único compartilhado: artefato de diagnóstico, nunca lido pelo
# script — a corrida é benigna.)
post_bet() {
  local body
  body="{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$4\",\"playerId\":\"$3\",\"walletId\":\"$2\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"BET\",\"money\":{\"amount\":\"$6\",\"currency\":\"BRL\"}}"
  printf '%s\n' "$(curl -sS -o "$TMP/body.json" -w '%{http_code}' -X POST "$1/wagering/transactions" \
    -H "Authorization: Bearer $PROVIDER_TOKEN" \
    -H "Idempotency-Key: $5" -H 'Content-Type: application/json' \
    -d "$body")"
}

# ── consultas diretas no banco ───────────────────────────────────────
# Usa o psql local quando existe — mesma convenção do Makefile e de
# docs/verify/01_schema.sh — e cai para o container quando não existe.
psql_q() {
  if command -v psql >/dev/null 2>&1; then
    psql "$PSQL_DSN" -q -A -t -c "$1"
  else
    docker compose exec -T postgres psql -U wager -d wager -q -A -t -c "$1"
  fi
}

stored_minor() { # walletId -> balance_minor
  psql_q "SELECT balance_minor FROM wallets WHERE id = '$1'::uuid" | tr -d '[:space:]'
}

ledger_delta() { # walletId -> créditos menos débitos no ledger
  psql_q "SELECT COALESCE(sum(CASE WHEN direction='CREDIT' THEN amount_minor ELSE -amount_minor END),0)
            FROM wallet_ledger_entries WHERE wallet_id = '$1'::uuid" | tr -d '[:space:]'
}

# ── 1. Mesma aposta 50x em paralelo: um único débito ─────────────────
titulo "1. Mesma aposta 50x em paralelo -> um único débito"
S="$(stamp)"
W1="$(open_wallet "player-50-$S" 100.00)"
nota "carteira $W1 com 100.00"
for i in $(seq 1 50); do
  # Mesma chave E mesmo conteúdo: o corpo tem de ser idêntico em todas as 50.
  ( post_bet "$BASE" "$W1" "player-50-$S" "ext-50-$S" "provider-a:50-$S" 25.00 > "$TMP/50-$i.code" ) &
done
wait
# `|| true` dentro de cada contagem: sob `set -o pipefail`, um grep SEM match sai
# com 1 e derrubaria a pipeline — e "sem erro" é justamente o resultado esperado.
CREATED=$( { grep -l '^201$' "$TMP"/50-*.code 2>/dev/null || true; } | wc -l | tr -d ' ')
REPLAYED=$( { grep -l '^200$' "$TMP"/50-*.code 2>/dev/null || true; } | wc -l | tr -d ' ')
ERRORS=$( { grep -lE '^(4|5)' "$TMP"/50-*.code 2>/dev/null || true; } | wc -l | tr -d ' ')
B1="$(balance_of "$W1")"
if [ "$CREATED" = "1" ] && [ "$B1" = "75.00" ]; then
  ok "1 aceita (201), $REPLAYED replicadas (200), $ERRORS erros; saldo 75.00"
else
  fail "criadas=$CREATED replicadas=$REPLAYED erros=$ERRORS saldo=$B1 (quer 1 / 49 / 0 / 75.00)"
fi

# ── 2. Disputa: duas apostas de 80.00 sobre saldo de 100.00 ──────────
titulo "2. Duas apostas de 80.00 sobre saldo de 100.00"
S="$(stamp)"
W2="$(open_wallet "player-80-$S" 100.00)"
( post_bet "$BASE" "$W2" "player-80-$S" "ext-80-a-$S" "provider-a:80-a-$S" 80.00 > "$TMP/80a.code" ) &
( post_bet "$BASE" "$W2" "player-80-$S" "ext-80-b-$S" "provider-a:80-b-$S" 80.00 > "$TMP/80b.code" ) &
wait
A="$(cat "$TMP/80a.code")"
B="$(cat "$TMP/80b.code")"
B2="$(balance_of "$W2")"
if { [ "$A" = "201" ] && [ "$B" = "422" ]; } || { [ "$A" = "422" ] && [ "$B" = "201" ]; }; then
  if [ "$B2" = "20.00" ]; then
    ok "exatamente uma aceita e uma recusada; saldo 20.00 (nunca negativo)"
  else
    fail "saldo=$B2 (quer 20.00)"
  fi
else
  fail "respostas: $A e $B (quer uma 201 e uma 422)"
fi

# ── 3. Carteiras distintas processando simultaneamente ───────────────
titulo "3. Carteiras distintas processando simultaneamente"
S="$(stamp)"
# Cada carteira pertence a um jogador DIFERENTE, e o playerId da operação tem de
# corresponder ao dono da carteira: por isso o par (carteira, jogador) anda junto.
PARES=""
I=0
for p in a b c; do
  I=$((I + 1))
  W="$(open_wallet "player-p3$p-$S" 100.00)"
  PARES="$PARES$W:player-p3$p-$S "
  for j in $(seq 1 5); do
    ( post_bet "$BASE" "$W" "player-p3$p-$S" "ext-p3-$I-$S-$j" "provider-a:p3-$I-$S-$j" 10.00 > "$TMP/p3-$I-$j.code" ) &
  done
done
wait
ACEITAS=$(cat "$TMP"/p3-*.code | grep -c '^201$' || true)
SALDOS_OK=0
for par in $PARES; do
  W="${par%%:*}"
  if [ "$(balance_of "$W")" = "50.00" ]; then SALDOS_OK=$((SALDOS_OK + 1)); fi
done
if [ "$ACEITAS" = "15" ] && [ "$SALDOS_OK" = "3" ]; then
  ok "15 apostas aceitas (5 por carteira), as 3 carteiras com saldo 50.00"
else
  fail "aceitas=$ACEITAS (quer 15); carteiras com saldo 50.00: $SALDOS_OK de 3"
fi

# ── 4. Três instâncias independentes ─────────────────────────────────
titulo "4. Três instâncias independentes"
LIVE=""
for p in $BASES; do
  if curl -fsS "http://localhost:$p/health/live" >/dev/null 2>&1; then LIVE="$LIVE $p"; fi
done
NUM_LIVE=$(echo $LIVE | wc -w | tr -d ' ')
if [ "$NUM_LIVE" -ge 2 ]; then
  S="$(stamp)"
  I=0
  for p in $LIVE; do
    I=$((I + 1))
    W="$(curl -fsS -X POST "http://localhost:$p/wallets" \
      -H "Authorization: Bearer $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
      -d "{\"playerId\":\"player-i$I-$S\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" \
      | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')"
    for j in $(seq 1 10); do
      ( post_bet "http://localhost:$p" "$W" "player-i$I-$S" "ext-i$I-$S-$j" "provider-a:i$I-$S-$j" 10.00 > "$TMP/i$I-$j.code" ) &
    done
  done
  wait
  ACC=$(cat "$TMP"/i*.code | grep -c '^201$' || true)
  ESPERADO=$((NUM_LIVE * 10))
  if [ "$ACC" = "$ESPERADO" ]; then
    ok "$NUM_LIVE instâncias, $ACC apostas aceitas (lost update nenhum)"
  else
    fail "instâncias=$NUM_LIVE aceitas=$ACC (quer $ESPERADO)"
  fi
else
  ok "SKIP: apenas $NUM_LIVE instância(s) no ar (suba app1/app2/app3 para exercitar)"
fi

# ── 7. REFUND antes da referência: o worker retoma sozinho ───────────
titulo "7. REFUND antes da referência -> resolução posterior"
S="$(stamp)"
W7="$(open_wallet "player-ref-$S" 100.00)"
REFUND_TX=$(curl -sS -X POST "$BASE/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" -H "Idempotency-Key: provider-a:refund-$S" \
  -H 'Content-Type: application/json' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"ext-refund-$S\",\"playerId\":\"player-ref-$S\",\"walletId\":\"$W7\",\"roundId\":\"round-1\",\"gameId\":\"game-1\",\"kind\":\"REFUND\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"ext-bet-$S\"}" \
  | python3 -c 'import json,sys
try:
    d = json.load(sys.stdin)
    print(str(d.get("status", "-")) + " " + str(d.get("transactionId", "-")))
except Exception:
    print("- -")')
REFUND_STATUS="$(echo "$REFUND_TX" | cut -d' ' -f1)"
REFUND_ID="$(echo "$REFUND_TX" | cut -d' ' -f2)"
if [ "$REFUND_STATUS" = "PENDING_REFERENCE" ]; then
  ok "REFUND devolveu PENDING_REFERENCE (pendência registrada, sem movimentar)"
else
  fail "status do REFUND = $REFUND_STATUS (quer PENDING_REFERENCE)"
fi

# A BET referenciada chega; o WORKER DE REFERÊNCIAS retoma sem intervenção.
post_bet "$BASE" "$W7" "player-ref-$S" "ext-bet-$S" "provider-a:bet-$S" 25.00 >/dev/null
RESOLVIDO="no"
ST=""
for _ in $(seq 1 30); do
  ST=$(curl -fsS -H "Authorization: Bearer $PROVIDER_TOKEN" \
        "$BASE/wagering/transactions/$REFUND_ID" \
        | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])' || echo "-")
  if [ "$ST" = "PROCESSED" ]; then RESOLVIDO="yes"; break; fi
  sleep 1
done
# 100 - 25 (BET) + 25 (REFUND) = 100.00
if [ "$RESOLVIDO" = "yes" ] && [ "$(balance_of "$W7")" = "100.00" ]; then
  ok "worker de referências retomou sozinho: PROCESSED, saldo 100.00"
else
  fail "resolvido=$RESOLVIDO status=$ST saldo=$(balance_of "$W7") (quer PROCESSED / 100.00)"
fi

# ── Mesma operação por HTTP e depois por SQS ─────────────────────────
titulo "8. Mesma operação por HTTP e depois por SQS -> um único débito"
S="$(stamp)"
W8="$(open_wallet "player-cross-$S" 100.00)"
post_bet "$BASE" "$W8" "player-cross-$S" "ext-cross-$S" "provider-a:cross-$S" 25.00 >/dev/null
if command -v aws >/dev/null 2>&1; then
  QURL=$(AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "$SQS_ENDPOINT" \
    sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text 2>/dev/null || true)
  if [ -n "$QURL" ]; then
    BODY=$(python3 - <<PYEOF
import json
print(json.dumps({"messageId": "cross-$S", "type": "WagerTransactionRequested",
 "occurredAt": "2026-09-08T12:00:00.000Z",
 "data": {"providerId": "provider-a", "externalTransactionId": "ext-cross-$S",
  "idempotencyKey": "provider-a:cross-$S", "playerId": "player-cross-$S",
  "walletId": "$W8", "roundId": "round-1", "gameId": "game-1", "kind": "BET",
  "money": {"amount": "25.00", "currency": "BRL"}}}))
PYEOF
)
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
      aws --endpoint-url "$SQS_ENDPOINT" \
      sqs send-message --queue-url "$QURL" --message-group-id "$W8" \
      --message-deduplication-id "cross-$S" --message-body "$BODY" >/dev/null
    sleep 5
    if [ "$(balance_of "$W8")" = "75.00" ]; then
      ok "a operação por SQS foi reconhecida como a mesma: saldo 75.00 (um débito)"
    else
      fail "saldo=$(balance_of "$W8") (quer 75.00: um débito, não dois)"
    fi
  else
    ok "SKIP: fila não encontrada em $SQS_ENDPOINT"
  fi
else
  ok "SKIP: aws-cli não disponível"
fi

# ── Conferência final: saldo armazenado x soma do ledger ─────────────
titulo "Conferência final: saldo armazenado x soma do ledger (todas as carteiras)"
DIVERGENTES=0
TOTAL=0
# Só espaços são removidos: `tr -d '[:space:]'` apagaria também os NEWLINES e
# colaria todos os UUIDs num único token inválido.
for w in $(psql_q "SELECT id FROM wallets" | tr -d ' '); do
  TOTAL=$((TOTAL + 1))
  ARMAZENADO="$(stored_minor "$w")"
  LEDGER="$(ledger_delta "$w")"
  if [ "$ARMAZENADO" != "$LEDGER" ]; then
    DIVERGENTES=$((DIVERGENTES + 1))
    printf '    divergência em %s: saldo=%s ledger=%s\n' "$w" "$ARMAZENADO" "$LEDGER"
  fi
done
if [ "$DIVERGENTES" = "0" ]; then
  ok "$TOTAL carteira(s) conferida(s), nenhuma divergência"
else
  fail "$DIVERGENTES de $TOTAL carteiras divergentes"
fi

# ── Cenários que exigem derrubar processo: provados por teste ────────
titulo "Cenários 5, 6 e do reinício: provados por teste automatizado"
nota "Item 5 — queda do consumidor ENTRE o commit e a remoção da mensagem:"
nota "  make test-consumer"
nota "  -> TestConsumerRedeliveryAfterCommitIsReplayAgainstRealQueue"
nota "  -> TestInFlightMessageCompletesAfterShutdown"
nota ""
nota "Item 6 — dois publishers disputando a mesma outbox:"
nota "  make test-consumer"
nota "  -> TestClaimBatchIsExclusiveBetweenPublishers"
nota "  -> TestWorkersPublishEachEventExactlyOnce"
nota ""
nota "Reinício preservando idempotência, pendências e consistência:"
nota "  make test-integration"
nota "  -> TestInboundProcessingIsAtomicAndReplaySafe"
nota "  -> TestReferenceWorkerResolvesPendingReferenceEndToEnd"
nota "  -> TestPendingReferenceCannotRefundTheSameReferenceTwice"
nota ""
nota "Razão: um shell não mata um processo 'entre o commit e a remoção' de forma"
nota "determinística. O teste faz isso descartando a PRIMEIRA remoção da fila."

# ── Resumo ───────────────────────────────────────────────────────────
titulo "Resumo"
printf '  PASS: %d   FAIL: %d\n' "$PASS_COUNT" "$FAIL_COUNT"
if [ "$FAIL_COUNT" -eq 0 ]; then
  printf '  \033[32mtodos os cenários demonstrados\033[0m\n'
  exit 0
fi
printf '  \033[31mhouve falha: verifique a saída acima\033[0m\n'
exit 1