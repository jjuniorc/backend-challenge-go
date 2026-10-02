#!/usr/bin/env bash
# Provisiona as filas SQS do desafio de forma idempotente.
#
# Executado pelo MiniStack a partir de /etc/localstack/init/ready.d e também
# utilizável direto no host (AWS_ENDPOINT_URL default = http://localhost:4566).
#
# ATENÇÃO (bug corrigido): FifoQueue e ContentBasedDeduplication são IMUTÁVEIS
# após a criação. Enviá-los em SetQueueAttributes faz a API rejeitar o request
# INTEIRO — inclusive o RedrivePolicy que ia no mesmo payload. Por isso:
#   - CreateQueue recebe o conjunto completo (a fila ainda não existe);
#   - SetQueueAttributes recebe SOMENTE atributos mutáveis.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="$REGION"

DLQ_NAME="${SQS_DLQ_NAME:-wager-transactions-dlq.fifo}"
QUEUE_NAME="${SQS_QUEUE_NAME:-wager-transactions.fifo}"
VISIBILITY_TIMEOUT="${SQS_VISIBILITY_TIMEOUT:-30}"
WAIT_TIME_SECONDS="${SQS_WAIT_TIME_SECONDS:-20}"
MAX_RECEIVE_COUNT="${SQS_MAX_RECEIVE_COUNT:-5}"

log() { echo "[queues] $*"; }

if ! command -v aws >/dev/null 2>&1; then
  echo "[queues] aws CLI não encontrado; abortando provisionamento" >&2
  exit 1
fi

sqs() { command aws --endpoint-url "$ENDPOINT" --region "$REGION" sqs "$@"; }

exists() { sqs get-queue-url --queue-name "$1" >/dev/null 2>&1; }

# ── 1) DLQ (precisa existir antes: é o alvo do redrive) ──────────────
if exists "$DLQ_NAME"; then
  log "já existe: $DLQ_NAME"
else
  cat > /tmp/wager-dlq.json <<JSON
{"QueueName":"${DLQ_NAME}","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false"}}
JSON
  sqs create-queue --cli-input-json file:///tmp/wager-dlq.json >/dev/null
  log "criada: $DLQ_NAME"
fi

DLQ_URL="$(sqs get-queue-url --queue-name "$DLQ_NAME" --query QueueUrl --output text)"
DLQ_ARN="$(sqs get-queue-attributes --queue-url "$DLQ_URL" \
             --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"
log "DLQ arn: $DLQ_ARN"

# ── 2) Fila principal, criada JÁ com redrive ─────────────────────────
if exists "$QUEUE_NAME"; then
  log "já existe: $QUEUE_NAME"
else
  cat > /tmp/wager-queue.json <<JSON
{
  "QueueName": "${QUEUE_NAME}",
  "Attributes": {
    "FifoQueue": "true",
    "ContentBasedDeduplication": "false",
    "VisibilityTimeout": "${VISIBILITY_TIMEOUT}",
    "ReceiveMessageWaitTimeSeconds": "${WAIT_TIME_SECONDS}",
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"${MAX_RECEIVE_COUNT}\"}"
  }
}
JSON
  sqs create-queue --cli-input-json file:///tmp/wager-queue.json >/dev/null
  log "criada: $QUEUE_NAME"
fi

QUEUE_URL="$(sqs get-queue-url --queue-name "$QUEUE_NAME" --query QueueUrl --output text)"

# ── 3) Convergência idempotente: SOMENTE atributos mutáveis ──────────
cat > /tmp/wager-setattrs.json <<JSON
{
  "QueueUrl": "${QUEUE_URL}",
  "Attributes": {
    "VisibilityTimeout": "${VISIBILITY_TIMEOUT}",
    "ReceiveMessageWaitTimeSeconds": "${WAIT_TIME_SECONDS}",
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"${MAX_RECEIVE_COUNT}\"}"
  }
}
JSON
sqs set-queue-attributes --cli-input-json file:///tmp/wager-setattrs.json
log "redrive aplicado"

log "estado final:"
sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names FifoQueue VisibilityTimeout ReceiveMessageWaitTimeSeconds RedrivePolicy \
  --output json

log "concluído"
