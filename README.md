# Wager Wallet — desafio de backend em Go

Backend de carteira e apostas com dinheiro em **centavos inteiros (`int64`)**, transações
atômicas no PostgreSQL, **idempotência** em HTTP e SQS, **inbox/outbox**, consumidor de fila
com DLQ, autenticação via **Keycloak** e métricas **Prometheus**.

> Este `README.md` documenta a **solução**: como subir, configurar, executar e testar.
> O enunciado original do desafio, preservado palavra por palavra, está em
> [`docs/CHALLENGE.md`](docs/CHALLENGE.md). As decisões de arquitetura e as limitações
> conhecidas estão em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Sumário

1. [O que a solução faz](#1-o-que-a-solução-faz)
2. [Pré-requisitos](#2-pré-requisitos)
3. [Subir a stack](#3-subir-a-stack)
4. [Inicialização das filas SQS](#4-inicialização-das-filas-sqs)
5. [Migrations](#5-migrations)
6. [Variáveis de ambiente](#6-variáveis-de-ambiente)
7. [Executar a aplicação](#7-executar-a-aplicação)
8. [Autenticação e autorização](#8-autenticação-e-autorização)
9. [Exemplos de chamadas](#9-exemplos-de-chamadas)
10. [Comandos de teste](#10-comandos-de-teste)
11. [Roteiro de demonstração](#11-roteiro-de-demonstração)
12. [Observabilidade](#12-observabilidade)
13. [Solução de problemas](#13-solução-de-problemas)
14. [Estrutura do repositório](#14-estrutura-do-repositório)

---

## 1. O que a solução faz

| Capacidade | Como está implementada |
| --- | --- |
| Dinheiro | `int64` de centavos; `amount` trafega como **string** (`"25.00"`), nunca float |
| Atomicidade | Débito, lançamento de ledger, transação, inbox e outbox na **mesma transação** SQL |
| Concorrência | `SELECT ... FOR NO KEY UPDATE` na carteira; saldo não negativo imposto pelo schema |
| Idempotência | Hash canônico do conteúdo; replay devolve o resultado original (`idempotentReplay`) |
| Referências | `REFUND`/`ROLLBACK` antes da referência viram `PENDING_REFERENCE` com retomada durável |
| Mensageria | FIFO com grupo por carteira, dedup por hash, `maxReceiveCount` + DLQ |
| Consistência | Worker de outbox com lease e worker de referências com `SKIP LOCKED` |
| Observabilidade | `/metrics` Prometheus, `/health/live`, `/health/ready`, logs `slog` com correlação |
| Ciclo de vida | `fx` com ordem de encerramento explícita e *grace* para o trabalho em andamento |

A composição sobe **três instâncias** idênticas da API (`app1`, `app2`, `app3`) compartilhando
PostgreSQL e a mesma fila — é o que permite demonstrar concorrência real entre processos.

---

## 2. Pré-requisitos

Versões em que o projeto foi executado e validado:

| Ferramenta | Versão | Necessária para |
| --- | --- | --- |
| **Docker** + **Docker Compose v2** | 20+ | subir PostgreSQL, MiniStack, Keycloak e as instâncias |
| **Go** | 1.27 (`go.mod`) | compilar, rodar localmente e executar os testes |
| **make** | qualquer | atalhos de build, migration, teste e demonstração |
| `aws` CLI | v2 | **opcional** — provisionar/inspecionar filas fora do container |
| `psql` | 16+ | **opcional** — conferência direta no banco e `make psql` |
| `python3` | 3.x | **opcional** — apenas o roteiro `docs/demo/demonstrate.sh` |
| `jq` | 1.6+ | **opcional** — legibilidade dos exemplos e `make token` |

Portas usadas por padrão (todas configuráveis):

| Porta | Serviço | Variável |
| --- | --- | --- |
| `5432` | PostgreSQL | `POSTGRES_HOST_PORT` |
| `4566` | MiniStack (SQS emulado) | fixa no `docker-compose.yml` |
| `8080` | Keycloak (no host) | `KEYCLOAK_HOST_PORT` |
| `8081`, `8082`, `8083` | `app1`, `app2`, `app3` | fixas no `docker-compose.yml` |

> **Se a porta 8080 estiver ocupada**, mude `KEYCLOAK_HOST_PORT` (por exemplo para `8090`)
> **e também** `OIDC_ISSUER`/`OIDC_TOKEN_URL`, que precisam acompanhar a mesma porta — o `iss`
> do token é gerado a partir dela. A aplicação valida o issuer, então divergir aqui resulta
> em `401` em todas as chamadas de provedor.

---

## 3. Subir a stack

```bash
# 1) Configuração local (o .env é ignorado pelo git; o exemplo não tem segredos reais)
cp .env.example .env

# 2) Stack completa: infraestrutura + migrations + 3 instâncias da API
docker compose up -d --build

# 3) Conferir
docker compose ps
curl -s http://localhost:8081/health/ready
```

A ordem de subida é garantida por `depends_on` com condições:

1. `postgres` sobe e fica **`service_healthy`** (`pg_isready`);
2. `ministack` fica **`service_healthy`** — o publisher valida a fila no *startup*, então o
   broker precisa estar pronto, não apenas iniciado;
3. `migrate` é um serviço **de vida curta**: aplica as migrations e termina com sucesso
   (`service_completed_successfully`); as aplicações só sobem depois disso;
4. `keycloak` sobe com `start-dev --import-realm` importando o realm **`wager`** de
   `docs/init/keycloak/realm-export.json`;
5. `app1`, `app2`, `app3` sobem com a mesma imagem, variando apenas `INSTANCE_ID`.

Alternativa em duas etapas, útil durante o desenvolvimento:

```bash
make up                              # apenas a infraestrutura
docker compose up -d                 # depois, migrations + instâncias
```

Para derrubar:

```bash
make down      # mantém os volumes (dados preservados)
make reset     # remove os volumes: estado limpo, migrations reaplicadas na próxima subida
```

## 4. Inicialização das filas SQS

**Automática.** O MiniStack executa todo script montado em `/etc/localstack/init/ready.d`;
o `docker-compose.yml` monta `./docs/init/ministack` ali. Portanto
[`docs/init/ministack/01-queues.sh`](docs/init/ministack/01-queues.sh) roda sozinho quando o
container fica pronto e provisiona de forma **idempotente**:

| Fila | Papel |
| --- | --- |
| `wager-transactions.fifo` | fila principal de operações |
| `wager-transactions-dlq.fifo` | dead-letter do redrive |

Configuração aplicada: `FifoQueue=true`, `ContentBasedDeduplication=false` (a deduplicação é
explícita, com `MessageDeduplicationId` derivado do hash de conteúdo), `VisibilityTimeout=30`,
`ReceiveMessageWaitTimeSeconds=20` (long polling) e
`RedrivePolicy` com `maxReceiveCount=5`.

> **Detalhe que importa:** `FifoQueue` e `ContentBasedDeduplication` são **imutáveis** após a
> criação. Enviá-los em `SetQueueAttributes` faz a API rejeitar o request **inteiro** —
> inclusive o `RedrivePolicy` que ia no mesmo payload. Por isso o script cria a fila já com
> o conjunto completo e usa `SetQueueAttributes` apenas para atributos mutáveis.

**Verificar:**

```bash
make queues
# {"QueueUrls": ["http://localhost:4566/000000000000/wager-transactions-dlq.fifo",
#                "http://localhost:4566/000000000000/wager-transactions.fifo"]}
```

**Provisionar manualmente** (se rodar a API contra um broker sem o script de init):

```bash
./docs/init/ministack/01-queues.sh          # idempotente; respeita AWS_ENDPOINT_URL
```

> `CONSUMER_MAX_RECEIVE_COUNT` (na aplicação) precisa **casar** com o `maxReceiveCount` da
> fila. Se divergirem, o consumidor encaminha para a DLQ antes do redrive do broker
> (desperdiçando tentativas) ou depende de uma rodada que já não teria chance de sucesso.

---

## 5. Migrations

As migrations vivem em `migrations/` e são embutidas no binário (`iofs`), então não dependem
de arquivos externos. As mesmas migration são aplicadas por `make migrate-up`, pelo serviço
`migrate` do Compose e pelos próprios testes de integração.

**Aplicar e reverter:**

```bash
make migrate-up        # aplica todas as pendentes
make migrate-version   # migrate: versão 1 (dirty=false)
make migrate-down      # reverte TODAS as aplicadas

# equivalente direto, com DSN explícito:
go run ./cmd/migrate up   -dsn 'postgres://wager:wager@localhost:5432/wager?sslmode=disable'
go run ./cmd/migrate down -dsn 'postgres://wager:wager@localhost:5432/wager?sslmode=disable'
```

Sem `-dsn`, o comando usa a variável `POSTGRES_DSN`:

```bash
POSTGRES_DSN='postgres://wager:wager@localhost:5432/wager?sslmode=disable' go run ./cmd/migrate up
```

Comandos disponíveis além de `up`/`down`/`version`:

| Comando | Efeito |
| --- | --- |
| `steps -n 2` | aplica 2 migrations (`-n` negativo reverte) |
| `goto -n 1` | vai para a versão informada |
| `force -n 1` | força a versão atual — resolve estado `dirty` após falha |
| `drop` | remove todas as tabelas do schema atual (**destrutivo**) |

Via container (sem Go instalado no host):

```bash
docker compose run --rm migrate up
docker compose run --rm migrate down
docker compose run --rm migrate version
```

> O driver roda com *multi-statement desabilitado* de propósito: o corpo inteiro da migration
> é enviado como uma consulta simples e o PostgreSQL o executa como transação implícita
> (tudo ou nada). Habilitar multi-statement faria *split* ingênuo por `;` e quebraria o corpo
> das funções PL/pgSQL delimitado por `$$`.

**Provar o schema** (constraints, triggers de imutabilidade, índices) em um banco
**descartável**, criado e removido pelo próprio script:

```bash
make schema-check     # ./docs/verify/01_schema.sh
```

---

## 6. Variáveis de ambiente

O arquivo [`.env.example`](.env.example) contém **valores locais de exemplo, sem segredos
reais** (usuário/senha `wager`, credenciais `test` do emulador e segredos ilustrativos do
realm). Copie-o para `.env` e ajuste o que precisar.

| Grupo | Variáveis |
| --- | --- |
| Aplicação | `APP_ENV`, `INSTANCE_ID`, `LOG_LEVEL`, `HTTP_ADDR` |
| PostgreSQL | `POSTGRES_DSN`, `POSTGRES_MAX_CONNS`, `POSTGRES_HOST_PORT` |
| SQS | `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `SQS_ENDPOINT`, `SQS_QUEUE_NAME`, `SQS_DLQ_NAME`, `SQS_QUEUE_URL`¹, `SQS_DLQ_URL`¹ |
| Consumidor | `CONSUMER_ENABLED`, `CONSUMER_NAME`, `CONSUMER_MAX_MESSAGES`, `CONSUMER_WAIT_TIME`, `CONSUMER_VISIBILITY_TIMEOUT`, `CONSUMER_MAX_RECEIVE_COUNT`, `CONSUMER_BASE_BACKOFF`, `CONSUMER_SHUTDOWN_GRACE` |
| Worker de referências | `REFERENCE_ENABLED`, `REFERENCE_INTERVAL`, `REFERENCE_ERROR_BACKOFF`, `REFERENCE_MAX_ATTEMPTS`, `REFERENCE_TTL`, `REFERENCE_BASE_BACKOFF`, `REFERENCE_MAX_BACKOFF` |
| Autenticação | `AUTH_MODE` (`oidc` \| `dev`), `OIDC_ISSUER`, `OIDC_JWKS_URL`, `OIDC_TOKEN_URL`, `OIDC_AUDIENCE`, `KEYCLOAK_HOST_PORT` |
| Credenciais de exemplo | `PROVIDER_A_CLIENT_ID/SECRET`, `PROVIDER_B_...`, `INTERNAL_CLIENT_ID/SECRET` |

¹ **Opcionais**: a URL da fila é resolvida em runtime por `GetQueueUrl`, para não depender do
*accountId* do emulador. Preencha apenas se precisar forçar um endpoint.

Pontos que costumam gerar erro:

- `AUTH_MODE=oidc` **exige** `OIDC_ISSUER` e `OIDC_JWKS_URL`; com valores ausentes a aplicação
  recusa o *startup* em vez de subir sem validar tokens.
- `AUTH_MODE=dev` só é aceito com `APP_ENV` em `local`, `dev` ou `test` — a construção falha
  em qualquer outro ambiente, de propósito.
- `OIDC_JWKS_URL` é a URL de **backchannel** (`http://keycloak:8080/...`), buscada por dentro
  da rede Docker; `OIDC_ISSUER`/`OIDC_TOKEN_URL` são URLs de **frontend** e precisam refletir
  a porta publicada no host.
- `CONSUMER_MAX_RECEIVE_COUNT` deve casar com o `maxReceiveCount` do redrive (ver §4).

---

## 7. Executar a aplicação

### Via Docker Compose (recomendado)

```bash
docker compose up -d --build           # infraestrutura + migrations + app1/app2/app3
docker compose logs -f app1            # acompanhar o log de uma instância
```

### Localmente, com Go (iteração rápida)

```bash
make up                                # infraestrutura no Docker
cp .env.example .env                   # a aplicação lê o ambiente do shell
set -a && source .env && set +a        # exporta as variáveis
make run                               # go run ./cmd/api
```

Ao rodar fora do Docker, `POSTGRES_DSN` e `SQS_ENDPOINT` apontam para `localhost`
(o `.env.example` já traz os valores de host), enquanto dentro do Compose os mesmos valores
apontam para os nomes dos serviços (`postgres`, `ministack`).

### Build dos binários

```bash
make build                             # go build ./...
go build -o /tmp/api ./cmd/api         # binário único
```

O `Dockerfile` gera dois binários (`/api` e `/migrate`) e usa a imagem
`distroless/static-debian12:nonroot` como runtime.

### Encerramento

O `fx` desliga na ordem inversa da construção e cada worker tem seu *grace*: o consumidor usa
`CONSUMER_SHUTDOWN_GRACE` para **concluir ou liberar** a mensagem em andamento, e a outbox
encerra o publicador antes de fechar o pool. Em `SIGTERM`/`SIGINT` o servidor HTTP para de
aceitar novas requisições antes dos workers.

---

## 8. Autenticação e autorização

O realm **`wager`** é importado automaticamente de `docs/init/keycloak/realm-export.json`:

| Cliente | Papel | Usado para |
| --- | --- | --- |
| `provider-a` | `provider` | operações financeiras do provedor A |
| `provider-b` | `provider` | operações do provedor B (isolamento entre provedores) |
| `internal-service` | `internal` | abrir/consultar carteiras, ledger, reconciliação |
| `provider-short-token` | `provider` | caso de token com validade curta |

O token carrega o papel e a claim `providerId`; o `aud` é `wager-api` (`OIDC_AUDIENCE`).
O `providerId` do corpo é conferido contra a identidade autenticada — um provedor nunca opera
em nome de outro.

**Obter token:**

```bash
make token              # provider-a (CLIENT/CLIENT_SECRET sobrescrevem)
make token-internal     # internal-service
make token-payload      # decodifica o payload do token para inspeção
```

**Modo de desenvolvimento** (atalho explícito para testar o contrato HTTP sem IdP) — exige
`AUTH_MODE=dev` e `APP_ENV=local`:

```bash
# Authorization: Bearer dev:<subject>:<providerId|->:<papeis>
curl -H 'Authorization: Bearer dev:svc-provider-a:provider-a:provider'   ...
curl -H 'Authorization: Bearer dev:svc-internal:-:internal'             ...
```

Rotas e papéis exigidos:

| Rota | Papel |
| --- | --- |
| `POST /wallets` | `internal` |
| `GET /wallets/:walletId` | `internal` |
| `GET /wallets/:walletId/ledger` | `internal` |
| `POST /wallets/:walletId/reconciliation` | `internal` |
| `POST /wagering/transactions` | `provider` |
| `GET /wagering/transactions/:transactionId` | `provider` |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | `provider` |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | públicas (probes e *scraper*) |

Respostas de autorização verificadas: **`401`** sem credencial, **`403`** com papel errado,
**`404`** ao consultar transação de outro provedor (isolamento sem vazar existência).

---

## 9. Exemplos de chamadas

Todos os exemplos abaixo foram executados contra a stack e as respostas são as reais.
Para reproduzir, exporte os tokens:

```bash
PROVIDER_TOKEN=$(make -s token)
INTERNAL_TOKEN=$(make -s token-internal)
BASE=http://localhost:8081
```

`amount` é sempre **string decimal** (`"25.00"`); número (inclusive `25.0`) é rejeitado, para
que nenhum float entre no domínio.

### 9.1 Abrir carteira (papel `internal`)

```bash
curl -s -X POST "$BASE/wallets" \
  -H "Authorization: Bearer $INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d '{"playerId":"player-42","initialBalance":{"amount":"100.00","currency":"BRL"}}'
```

```json
{"id":"01a0fe9c-8295-73f0-b378-40c885dce5df","playerId":"player-42","currency":"BRL",
 "balance":{"amount":"100.00","currency":"BRL"},"version":1,
 "createdAt":"2026-10-02T21:54:25.045256Z","updatedAt":"2026-10-02T21:54:25.045256Z"}
```

`201 Created`. A abertura registra a transação `OPENING` e gera lançamento de `CREDIT` no
ledger, além dos eventos de outbox correspondentes.

### 9.2 Consultar carteira, ledger e reconciliação

```bash
curl -s -H "Authorization: Bearer $INTERNAL_TOKEN" "$BASE/wallets/$WALLET_ID"
```

```json
{"walletId":"01a0fe9c-...","entries":[
  {"id":"01a0fe9c-df6b-...","transactionId":"01a0fe9c-df68-...","direction":"DEBIT",
   "money":{"amount":"25.00","currency":"BRL"},
   "balanceBefore":{"amount":"100.00","currency":"BRL"},
   "balanceAfter":{"amount":"75.00","currency":"BRL"},
   "createdAt":"2026-10-02T21:54:48.811239Z"}],
 "nextCursor":"..."}
```

```bash
curl -s -X POST -H "Authorization: Bearer $INTERNAL_TOKEN" \
  "$BASE/wallets/$WALLET_ID/reconciliation"
```

```json
{"walletId":"01a0fe9c-...","storedBalance":{"amount":"75.00","currency":"BRL"},
 "calculatedBalance":{"amount":"75.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}
```

`GET /wallets/:walletId/ledger` pagina com cursor opaco (`?limit=50&cursor=...`).
A reconciliação compara o saldo **armazenado** com a soma do ledger e, quando divergem,
registra log de erro e incrementa `wager_reconciliation_divergences_total`.

### 9.3 Apostar (papel `provider`)

```bash
curl -s -X POST "$BASE/wagering/transactions" \
  -H "Authorization: Bearer $PROVIDER_TOKEN" \
  -H 'Idempotency-Key: provider-a:bet-1' -H 'Content-Type: application/json' \
  -d '{"providerId":"provider-a","externalTransactionId":"ext-bet-1","playerId":"player-42",
       "walletId":"'$WALLET_ID'","roundId":"round-1","gameId":"game-1","kind":"BET",
       "money":{"amount":"25.00","currency":"BRL"}}'
```

```json
{"transactionId":"01a0fe9c-82d4-...","status":"PROCESSED",
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

`201 Created`. O header `Idempotency-Key` é **obrigatório** (`400` sem ele).

### 9.4 Repetir a mesma chamada → replay

Mesma chave **e** mesmo conteúdo:

```json
{"transactionId":"01a0fe9c-82d4-...","status":"PROCESSED",
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":true}
```

`200 OK`, mesmo `transactionId` e o **saldo observado no processamento original** — não o
saldo atual, que pode ter mudado por outras operações.

### 9.5 Mesma chave com conteúdo diferente → conflito

```json
{"error":{"code":"IDEMPOTENCY_KEY_REUSED",
 "message":"IDEMPOTENCY_KEY_REUSED: chave de idempotência reutilizada com conteúdo diferente",
 "correlationId":"01a0fe9c-831a-..."}}
```

`409 Conflict`.

### 9.6 Saldo insuficiente

```json
{"transactionId":"01a0fe9c-8336-...","status":"REJECTED",
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false,
 "failureCode":"WALLET_INSUFFICIENT_FUNDS","failureMessage":"saldo insuficiente para o débito"}
```

`422 Unprocessable Entity`. A rejeição é **registrada e auditável** (tem `transactionId`) e
não movimenta o ledger. Para reversões, o código é distinto:
`REVERSAL_INSUFFICIENT_FUNDS`.

### 9.7 `REFUND` antes da referência

```json
{"transactionId":"01a0fe9c-dff4-...","status":"PENDING_REFERENCE",
 "balance":{"amount":"75.00","currency":"BRL"},"idempotentReplay":false}
```

`202 Accepted`. A operação fica agendada com `reference_attempts` e `next_attempt_at`
persistidos; o **worker de referências** retoma sozinho quando a `BET` referenciada chega —
inclusive após reinício da aplicação. Esgotadas as tentativas (ou o TTL), a operação é
finalizada como `REJECTED` com código de referência não encontrada.

### 9.8 Consultar transações

```bash
curl -s -H "Authorization: Bearer $PROVIDER_TOKEN" "$BASE/wagering/transactions/$TX_ID"
curl -s -H "Authorization: Bearer $PROVIDER_TOKEN" \
  "$BASE/providers/provider-a/wagering/transactions/ext-bet-1"
```

```json
{"transactionId":"01a0fe9c-df68-...","providerId":"provider-a",
 "externalTransactionId":"ext-bet-1","playerId":"player-42","walletId":"01a0fe9c-df07-...",
 "roundId":"round-1","gameId":"game-1","kind":"BET","status":"PROCESSED",
 "money":{"amount":"25.00","currency":"BRL"},"balance":{"amount":"75.00","currency":"BRL"},
 "occurredAt":"2026-10-02T21:54:48.808248Z","createdAt":"2026-10-02T21:54:48.808248Z",
 "updatedAt":"2026-10-02T21:54:48.811239Z"}
```

### 9.9 Enviar a mesma operação por SQS

A fila aceita o mesmo conteúdo do HTTP; a chave de idempotência vem em
`data.idempotencyKey` (no HTTP ela vem no header). O hash é calculado sobre o conteúdo
canônico, **excluindo** a chave e metadados de transporte — por isso a mesma operação
enviada por HTTP e depois por SQS é reconhecida como **idêntica** e debitada uma única vez.

```bash
# O emulador aceita credenciais fictícias; a região é obrigatória para o aws CLI.
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

QUEUE_URL=$(aws --endpoint-url http://localhost:4566 sqs get-queue-url \
  --queue-name wager-transactions.fifo --query QueueUrl --output text)

aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-group-id "$WALLET_ID" \
  --message-deduplication-id "msg-1" \
  --message-body '{
    "messageId": "msg-1",
    "type": "WagerTransactionRequested",
    "occurredAt": "2026-10-02T12:00:00.000Z",
    "data": {
      "providerId": "provider-a",
      "externalTransactionId": "ext-sqs-1",
      "idempotencyKey": "provider-a:ext-sqs-1",
      "playerId": "player-42",
      "walletId": "'$WALLET_ID'",
      "roundId": "round-1",
      "gameId": "game-1",
      "kind": "BET",
      "money": {"amount": "25.00", "currency": "BRL"}
    }}'
```

- **`message-group-id` por carteira**: preserva a ordem das operações de uma mesma carteira
  sem serializar carteiras distintas.
- **`message-deduplication-id`**: dedup no broker. A dedup da **aplicação** é independente e
  continua valendo após a janela do SQS, via inbox.
- Mensagem **inválida** (campos ausentes, `amount` como número, tipo desconhecido) vai para a
  **DLQ** após `maxReceiveCount` tentativas; erro transitório (banco fora do ar) é
  **reentregue** com backoff exponencial e *jitter*.

Para inspecionar a DLQ:

```bash
aws --endpoint-url http://localhost:4566 sqs get-queue-attributes \
  --queue-url "$(aws --endpoint-url http://localhost:4566 sqs get-queue-url \
     --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)" \
  --attribute-names ApproximateNumberOfMessages
```

---

## 10. Comandos de teste

```bash
make test              # go test ./...            (unitários, sem infraestrutura)
make race              # go test -race ./...      (detector de corrida)
make check             # fmt + vet + test
make test-integration  # Postgres, Keycloak e SQS REAIS, todos os pacotes
make test-all          # check + test-integration
```

| Alvo | Escopo | Prova |
| --- | --- | --- |
| `make test` | unitário | dinheiro/escala/limites, invariantes da carteira, transições de estado, regras dos cinco tipos, hash canônico, política de valor zero, cursor, métricas |
| `make test-integration` | integração | migrations, constraints, imutabilidade do ledger, atomicidade, inbox, reentrega, outbox concorrente, retry, DLQ, recuperação e composição Fx |
| `make test-oidc` | `internal/auth` | fluxo **real** com o Keycloak: token válido, ausente, inválido e expirado; papéis e isolamento |
| `make test-broker` | `internal/broker` | publicação e consumo contra o **SQS emulado real** |
| `make test-consumer` | `internal/worker` | consumidor de ponta a ponta, outbox e referências contra Postgres/SQS reais |
| `make schema-check` | `docs/verify/01_schema.sh` | constraints e triggers em banco descartável, com `expect_fail` para cada rejeição esperada |
| `make demo` | roteiro §13 do enunciado | cenários de concorrência e recuperação na stack viva (§11) |

Detalhes que evitam surpresa:

- **A infraestrutura precisa estar no ar** (`make up`). Os alvos de integração exportam
  `TEST_OIDC_REQUIRED=1` e `TEST_SQS_REQUIRED=1`: sem IdP ou broker os testes **falham**, em
  vez de serem silenciosamente ignorados.
- **Nenhum banco precisa ser criado à mão.** `TEST_POSTGRES_DSN` aponta para o **servidor**;
  cada pacote de teste cria e migra o **seu próprio** banco (`wager_test_usecase`,
  `wager_test_pg`, `wager_test_httpapi`, ...), usando o banco administrativo `postgres`.
  Isso é necessário porque o runner do Go executa binários de pacotes diferentes em paralelo:
  um `TRUNCATE` compartilhado apagaria dados que o outro acabou de inserir.
- Ajuste `TEST_POSTGRES_DSN` se o PostgreSQL não estiver em `localhost:5432`:

  ```bash
  make test-integration TEST_POSTGRES_DSN='postgres://wager:wager@localhost:5433/wager_test?sslmode=disable'
  ```

- `make test-integration` roda com `-race`.

---

## 11. Roteiro de demonstração

O enunciado (§13) pede a comprovação dos cenários de concorrência e recuperação. O roteiro
executa os cenários **contra a stack viva** e imprime `PASS`/`FAIL` por item:

```bash
make demo                 # BASE=http://localhost:8081
make demo DEMO_BASE=http://localhost:8082
```

Variáveis aceitas: `BASE`, `BASES` (portas das instâncias), `KEYCLOAK_URL`, `SQS_ENDPOINT`,
`PSQL_DSN` e `USE_DEV_AUTH=1` (usa o autenticador de desenvolvimento em vez do Keycloak).

Saída verificada nesta stack:

```
1. Mesma aposta 50x em paralelo -> um único débito
  PASS 1 aceita (201), 49 replicadas (200), 0 erros; saldo 75.00
2. Duas apostas de 80.00 sobre saldo de 100.00
  PASS exatamente uma aceita e uma recusada; saldo 20.00 (nunca negativo)
3. Carteiras distintas processando simultaneamente
  PASS 15 apostas aceitas (5 por carteira), as 3 carteiras com saldo 50.00
4. Três instâncias independentes
  PASS 3 instâncias, 30 apostas aceitas (lost update nenhum)
7. REFUND antes da referência -> resolução posterior
  PASS REFUND devolveu PENDING_REFERENCE ... / PASS worker de referências retomou sozinho
8. Mesma operação por HTTP e depois por SQS -> um único débito
  PASS a operação por SQS foi reconhecida como a mesma: saldo 75.00 (um débito)
Conferência final: saldo armazenado x soma do ledger (todas as carteiras)
  PASS N carteira(s) conferida(s), nenhuma divergência

Resumo
  PASS: 9   FAIL: 0
```

Os cenários que exigem **matar processo no meio** — queda do consumidor entre o commit e a
remoção da mensagem, dois publishers disputando a mesma outbox e o reinício — não são
determinísticos por shell. O roteiro imprime o comando de teste exato de cada um
(`make test-consumer` e `make test-integration`), e o teste faz isso descartando a
**primeira** remoção da fila, o que reproduz a janela de forma controlada.

O roteiro exige `python3`; o cenário de SQS é pulado (com aviso `SKIP`) se o `aws` CLI não
estiver disponível ou a fila não existir.

---

## 12. Observabilidade

| Endpoint | Resposta |
| --- | --- |
| `GET /health/live` | `200` — processo vivo, sem dependências |
| `GET /health/ready` | `{"checks":{"postgres":"up"},"status":"ready"}` |
| `GET /metrics` | formato Prometheus (texto) |

Métricas próprias (namespace `wager_`):

| Métrica | Tipo | Rótulos | Significado |
| --- | --- | --- | --- |
| `wager_http_requests_total` | counter | `method`, `route`, `status` | requisições por rota (**template**, não caminho concreto) |
| `wager_http_request_duration_seconds` | histogram | `method`, `route` | latência por rota |
| `wager_idempotent_replays_total` | counter | — | replays idempotentes servidos |
| `wager_transactions_by_status` | gauge | `status` | transações por estado (derivado do banco) |
| `wager_transactions_by_failure_code` | gauge | `code` | rejeições por código de falha (derivado do banco) |
| `wager_outbox_pending_events` | gauge | — | eventos de outbox ainda não publicados |
| `wager_consumer_events_total` | counter | `outcome` | `received`, `processed`, `retried`, `invalid`, `dead_lettered` |
| `wager_reference_resolutions_total` | counter | `outcome` | `rescheduled`, `rejected`, `exhausted`, `errors` |
| `wager_reconciliation_divergences_total` | counter | — | divergências detectadas na reconciliação |

Exemplo real:

```
wager_transactions_by_status{status="PROCESSED"} 269
wager_transactions_by_status{status="PENDING_REFERENCE"} 2
wager_transactions_by_failure_code{code="WALLET_INSUFFICIENT_FUNDS"} 6
wager_outbox_pending_events 5
```

O rótulo `route` usa a **rota do template** (`/wallets/:walletId`) e não o caminho concreto:
com o caminho concreto cada UUID consultado viraria uma série nova, e qualquer cliente poderia
inflar a cardinalidade mandando caminhos aleatórios. O próprio `/metrics` é excluído da
instrumentação de latência para não se medir a si mesmo.

Os coletores padrão do processo (`go_*`, `process_*`) também são expostos — úteis para
diagnosticar vazamento de *goroutine* nos workers.

---

## 13. Solução de problemas

| Sintoma | Causa provável | Ação |
| --- | --- | --- |
| `401` em todas as chamadas de provedor | `OIDC_ISSUER` não corresponde à porta publicada do Keycloak | iguale `OIDC_ISSUER`/`OIDC_TOKEN_URL` a `KEYCLOAK_HOST_PORT` e recrie as instâncias |
| `make run` falha ao subir | `AUTH_MODE=oidc` sem `OIDC_ISSUER`/`OIDC_JWKS_URL` | defina as duas variáveis (o *startup* recusa subir sem validar tokens) |
| `docker compose` falha ao publicar porta | `8080` já ocupada | `KEYCLOAK_HOST_PORT=8090` no `.env` + ajuste do issuer |
| Migration em estado `dirty` | falha no meio de uma migration | `go run ./cmd/migrate force -n <versão>` e reaplique |
| Filas ausentes / erro no *startup* do publisher | MiniStack sem o script de init | `./docs/init/ministack/01-queues.sh` e confira com `make queues` |
| Consumidor envia tudo para a DLQ | `CONSUMER_MAX_RECEIVE_COUNT` ≠ `maxReceiveCount` | iguale os dois valores (§4) |
| DLQ recebe eventos gerados pela própria aplicação | publisher e consumidor compartilham `wager-transactions.fifo` | limitação **conhecida e documentada** (`ARCHITECTURE.md` §9.1, limitação 16): financeiramente correto, porém ruidoso. Correção: fila de saída dedicada |
| Funcionalidade nova retorna `404` no container | imagem construída antes do código atual | `docker compose up -d --build` |
| Testes de integração falham na conexão | infraestrutura fora do ar ou porta diferente | `make up` e/ou `TEST_POSTGRES_DSN` ajustado |

---

## 14. Estrutura do repositório

```
.
├── cmd/
│   ├── api/                     # binário da API
│   └── migrate/                 # CLI de migrations (up/down/version/steps/goto/force/drop)
├── internal/
│   ├── app/                     # composição fx, ciclo de vida, servidor HTTP
│   ├── auth/                    # verificador OIDC/JWKS + autenticador de desenvolvimento
│   ├── broker/                  # publisher SQS e fonte de mensagens
│   ├── config/                  # leitura e validação do ambiente
│   ├── domain/                  # money, wallet, ledger, wagertransaction, erros
│   ├── events/                  # envelopes e eventos de domínio
│   ├── httpapi/                 # rotas, handlers, DTOs, erros, métricas
│   ├── metrics/                 # registry Prometheus e coletores
│   ├── payloadhash/             # hash canônico de idempotência
│   ├── ports/                   # interfaces (inversão de dependência)
│   ├── repository/pg/           # acesso a dados (pool, transações, inbox, outbox)
│   ├── testsupport/             # infraestrutura dos testes de integração (build tag)
│   ├── usecase/                 # regras de aplicação
│   └── worker/                  # consumidor SQS, outbox, referências pendentes
├── migrations/                  # SQL versionado, embutido no binário
├── docs/
│   ├── CHALLENGE.md             # enunciado original, preservado
│   ├── demo/demonstrate.sh      # roteiro de demonstração (make demo)
│   ├── init/keycloak/           # realm de exemplo importado automaticamente
│   ├── init/ministack/          # provisionamento das filas SQS
│   └── verify/01_schema.sh      # provas de constraints/triggers
├── ARCHITECTURE.md              # decisões, interpretações e limitações
├── docker-compose.yml           # postgres + ministack + keycloak + migrate + 3 apps
├── Dockerfile                   # build multi-stage, runtime distroless nonroot
├── Makefile                     # atalhos (help, up, test, demo, migrate-*, ...)
└── .env.example                 # valores locais de exemplo, sem segredos reais
```

Documentos complementares:

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — princípios, transações, constraints, locks,
  idempotência, workers, contrato de mensageria, autorização, ciclo de vida, observabilidade,
  interpretações adotadas e limitações conhecidas.
- [`docs/CHALLENGE.md`](docs/CHALLENGE.md) — enunciado original.
- [`docs/verify/01_schema.sh`](docs/verify/01_schema.sh) — cada rejeição esperada do schema é
  provada com `expect_fail`.