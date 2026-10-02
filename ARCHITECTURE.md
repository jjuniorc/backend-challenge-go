# ARCHITECTURE

Documento com as decisões de arquitetura.

Os comandos de verificação estão no `README.md`, no `Makefile` e em `docs/demo/`; o enunciado original está preservado em `docs/CHALLENGE.md`.

---

## 1. Princípio que governa o desenho da arquitetura

**Nenhuma garantia financeira depende do código da aplicação.** Toda invariante
que não pode ser violada está no schema: `CHECK`, `UNIQUE`, `FOREIGN KEY` e
`TRIGGER`. O código faz o caminho feliz correto e concorrente; o banco é a rede
que impede o estado impossível.

A consequência aparece nos testes: várias asserções verificam que o **banco**
rejeitou, e não que o código lembrou de rejeitar.

---

## 2. Dinheiro

| Decisão | Detalhe |
|---|---|
| Representação | `int64` em unidades mínimas (centavos), escala fixa 2 |
| Ponto flutuante | **Nenhum** `float32`/`float64` em parsing, aritmética, comparação ou serialização |
| Overflow | Sempre rejeitado (`ErrOverflow`), nunca truncado. Limite: `\|valor\| <= 92233720368547758.07` |
| Entrada | Parser manual de string decimal; `"25"`, `"25.5"` e `"025.00"` normalizam para `"25.00"` |
| JSON | `amount` só aceita **string**. `25`, `25.0` e `1e3` são rejeitados (`MONEY_AMOUNT_MUST_BE_STRING`) |

O hash de idempotência usa **sempre** a forma canônica de 2 casas: `"25"` e
`"25.00"` produzem o mesmo hash — é o que faz a mesma operação enviada por HTTP e
por SQS ser reconhecida como a mesma.

**Limitações registradas:**

- A moeda é validada por **forma** (`^[A-Z]{3}$`), não por existência na tabela
  ISO 4217. `XYZ` é bem formado e passa; morre depois na busca da carteira, como
  `WALLET_NOT_FOUND`. A tabela ISO não foi embutida de propósito: o schema usa a
  mesma regra, e entrada e banco concordam.
- `money.UnmarshalJSON` usa `ParseDecimal`, que **aceita negativos**;
  `ParseExternalAmount` é quem rejeitaria, e o caminho JSON não passa por ele. Na
  prática um valor negativo é barrado pela **política de valor por tipo** no
  schema (`BET`/`WIN`/`REFUND`/`ROLLBACK` exigem `> 0`; `LOSS` exige `= 0`). É
  dívida de desenho, e é idêntica entre HTTP e SQS — o que importa para a
  equivalência das duas entradas.

---

## 3. Atomicidade e ordem dentro da transação

Uma operação é **uma transação SQL**. A ordem é deliberada:

```
(inbox, quando há mensagem)      -- dedup durável do transporte
INSERT wager_transactions        -- reivindica a chave de idempotência ANTES de qualquer lock
SELECT ... FOR NO KEY UPDATE     -- trava a carteira
resolução da referência          -- validação + referência interna resolvida
movimento (débito/crédito)       -- saldo em memória
INSERT wallet_ledger_entries     -- lançamento append-only
UPDATE wallets (saldo + versão)  -- condicionado à versão esperada
UPDATE wager_transactions        -- estado terminal + resultado
INSERT outbox_events             -- eventos: NADA é publicado antes do commit
(inbox) Complete                 -- conclusão da mensagem, no MESMO commit
COMMIT
```

Três consequências que valem nomear:

1. **A chave de idempotência é reivindicada primeiro.** A operação nasce
   `PENDING` e **nunca é confirmada nesse estado**: ou avança para um estado
   terminal no mesmo commit, ou o commit inteiro não acontece. Por isso não
   existe `PENDING` órfão para retomar.
2. **Nada é publicado antes do commit.** O evento entra na outbox dentro da
   transação; quem publica é um worker separado, depois. Publicar antes seria a
   falha clássica: o broker recebe e o banco reverte.
3. **A inbox entra no mesmo commit.** Ver §6.3.

---

## 4. Constraints: a fonte da verdade

O que o schema garante (ver `migrations/000001_init.up.sql`):

| Garantia | Mecanismo |
|---|---|
| Saldo nunca negativo | `wallets_balance_non_negative` (`balance_minor >= 0`) |
| Moeda da operação = moeda da carteira | FK composta `(wallet_id, currency) -> wallets(id, currency)` |
| Uma carteira por `(playerId, moeda)` | `wallets_player_currency_uniq` |
| Uma abertura por carteira | índice parcial único em `kind = 'OPENING'` |
| Idempotência por provedor | `UNIQUE (provider_id, idempotency_key)` |
| Identidade da operação externa | `UNIQUE (provider_id, external_transaction_id)` |
| Referência externa obrigatória por tipo | `CHECK` de metadados externos + exigência por `kind` |
| Reversão única por referência | `wt_single_successful_reversal_per_reference`, parcial em `status='PROCESSED'` e `kind IN ('REFUND','ROLLBACK')` |
| Um lançamento por `(carteira, transação)` | `UNIQUE (wallet_id, transaction_id)` no ledger |
| Ledger imutável | `TRIGGER` que rejeita `UPDATE` e `DELETE` |
| Inbox deduplicada | `UNIQUE (consumer_name, message_id)` |
| Outbox idempotente por evento | `UNIQUE (event_id)` |
| Evento publicado não mantém reserva | `outbox_published_releases_lock` |

**A corrida é decidida pelo banco, não pelo código.** Dois pedidos concorrentes
com a mesma chave: um commit vence, o outro recebe violação de unicidade,
reverte, e então é reavaliado como *replay* do vencedor. O código não tenta
"checar antes" — isso seria uma janela de corrida.

---

## 5. Locks: `FOR NO KEY UPDATE`, e o deadlock que ele evita

A serialização por carteira é pessimista: `SELECT ... FOR NO KEY UPDATE` na linha
da carteira. Operações da **mesma** carteira serializam; carteiras distintas
seguem em paralelo.

**Por que não `FOR UPDATE`.** O `INSERT` em `wager_transactions` valida a FK da
carteira e, para isso, o PostgreSQL adquire `FOR KEY SHARE` na linha da carteira e
o mantém até o commit. `FOR UPDATE` **conflita** com `FOR KEY SHARE`: cada
concorrente ficaria preso no upgrade `KEY SHARE -> FOR UPDATE`, produzindo
deadlock (`SQLSTATE 40P01`). `FOR NO KEY UPDATE` é compatível com `FOR KEY SHARE`
e continua exclusivo entre escritores.

Isto **não é teoria**: a primeira versão usava `FOR UPDATE` e o teste das 50
apostas paralelas produzia 24 deadlocks. O teste é a regressão.

**Por que pessimista.** A alternativa (otimista, com retry em conflito de versão)
foi descartada porque o caminho crítico é curto e a contenção é por carteira: o
custo de um retry sob alta concorrência é maior que o de um lock de linha. A
proteção otimista **existe também** (`UpdateBalance` é condicionado à versão
esperada, devolvendo `ErrVersionConflict`), mas como segunda barreira — nenhum
caminho depende dela para estar correto.

**Testes que prendem isso:** 50 apostas idênticas em paralelo produzem um único
débito; duas apostas de 80.00 sobre saldo de 100.00 produzem exatamente uma aceita
e uma recusada; carteiras distintas processam em paralelo.

---

## 6. Idempotência: duas identidades distintas

| Identidade | Chave | Onde vive | Deduplica |
|---|---|---|---|
| **Operação financeira** | `(providerId, idempotencyKey)` | `wager_transactions` | O efeito no dinheiro |
| **Mensagem** | `(consumerName, messageId)` | `inbox_messages` | A entrega do broker |

São coisas diferentes e precisam de garantias diferentes. A primeira é o que
impede pagar duas vezes; a segunda é o que torna a reentrega do broker inofensiva.

### 6.1 Dois hashes, propósitos distintos

- **`payloadhash.Compute`** — SHA-256 sobre JSON canônico dos **campos de
  negócio**. Exclui a chave de idempotência e metadados de transporte. É
  **idêntico entre HTTP e SQS** (testado), e é o que detecta reutilização de chave
  com conteúdo diferente.
- **`worker.PayloadHashOf`** — SHA-256 sobre o **corpo bruto** da mensagem. É a
  identidade de transporte verificada pela inbox. Um corpo reencodado é tratado
  como **mensagem diferente** (fail-safe: vai para a DLQ, e nunca movimenta saldo
  duas vezes).

Confundi-los seria o erro: o primeiro é sobre a *operação*, o segundo sobre a
*mensagem*.

### 6.2 Replay devolve o resultado original

Chave e conteúdo iguais → resposta `idempotentReplay: true` com o **saldo
observado no processamento original**, não o saldo atual (que pode ter mudado com
outras operações). Chave igual com conteúdo diferente → `409`, código
`IDEMPOTENCY_KEY_REUSED`.

### 6.3 Inbox na mesma transação (enunciado §6.5)

O registro do recebimento, a aplicação financeira e a conclusão da mensagem
compartilham **um** commit. Sem isso, uma queda entre "registrei o recebimento" e
"apliquei a operação" deixaria a mensagem marcada como tratada **sem**
movimentação — e a reentrega seria descartada. É o espelho do risco de
movimentação duplicada, e igualmente inaceitável.

O `INSERT` na inbox é o ponto de serialização entre entregas concorrentes: a
perdedora espera a vencedora confirmar e então enxerga a linha concluída
(testado com 8 entregas simultâneas → um débito).

Quando a operação já foi aplicada por outro caminho (o mesmo pedido enviado pelo
HTTP), a transação reverte e o registro da inbox é gravado em transação própria,
para que a mensagem tenha sua linha concluída e a auditoria fique completa.

---

## 7. Referências pendentes e reversões

### 7.1 PENDING_REFERENCE com agendamento durável

Um `REFUND` que chega antes da `BET` referenciada não é rejeitado nem perdido:
fica `PENDING_REFERENCE` com `reference_attempts` e `next_attempt_at`
persistidos, e um evento `WagerTransactionPendingReference` na outbox.

**Política** (`usecase.ReferencePolicy`, vinda da configuração e usada **também**
pelo caso de uso para agendar): `MaxAttempts` (total, contando a tentativa
inicial) ou `TTL` (24h por padrão). Esgotada → finalizada como `REJECTED` com
`REFERENCE_TIMEOUT`, no mesmo commit do evento `WagerTransactionRejected`.

O agendamento é **durável**: sobrevive a reinício, porque está na linha, e não em
memória. É isso que permite a outra instância retomar (enunciado §13 item 8).

### 7.2 Reversões

`REFUND`/`ROLLBACK` exigem referência e são protegidas por
`wt_single_successful_reversal_per_reference`: **uma reversão bem-sucedida por
referência**, garantida pelo índice único.

**Um defeito encontrado por teste, que vale registrar.** Na retomada, a referência
interna resolvida (`reference_transaction_id`) estava sendo gravada na variável
local em vez da cópia que o repositório persiste. Consequência: a reversão era
aplicada, mas o índice — que é **sobre essa coluna** — não a enxergava, porque
`NULL` não colide em índice único. A mesma aposta poderia ser revertida duas vezes.

O teste que pegou foi uma asserção sobre o **campo**, não sobre o saldo: o saldo
estava correto no caso simples. O teste de regressão
(`TestPendingReferenceCannotRefundTheSameReferenceTwice`) falha se isso voltar.

---

## 8. Outbox × referências: dois workers, duas decisões

| | Outbox | Referências pendentes |
|---|---|---|
| Natureza do trabalho | **Efeito externo** (broker) | **Local** (banco) |
| Cabe na transação de negócio? | Não | **Sim** |
| Precisa de lease? | **Sim** (`locked_until`, `locked_by`) | **Não** |
| Reserva abandonada | Recuperada por expiração do lease | **Inexistente**: o rollback devolve o estado |
| Regra de ouro | Publicar **depois** do commit | Reservar **e** gravar o desfecho no **mesmo** commit |

**"Uma tentativa = uma transação"** tem uma consequência que precisa estar
escrita: a reserva **não é durável por si só**. O lock do `FOR UPDATE SKIP
LOCKED` termina no commit; sem lease, uma reserva confirmada sem gravar desfecho
devolve a operação à disputa. Isso é bom — elimina o estado "reservado e
indefinido" que exigiria recuperação — **mas impõe a regra**: um worker que
reserve e processe fora da transação estaria repetindo o padrão da outbox sem o
lease.

Há teste para as duas faces: o claim fora de transação não segura o lock, e uma
reserva confirmada sem desfecho devolve a linha.

**Multi-instância.** Os dois workers usam `FOR UPDATE SKIP LOCKED`, então duas
instâncias nunca pegam o mesmo registro. A outbox ainda tolera uma instância que
morre entre o commit e a publicação (lease vencido); as referências, não precisam
(rollback devolve tudo).

---

## 9. Mensageria SQS

| Decisão | Valor | Motivo |
|---|---|---|
| `MessageGroupId` | `aggregateId` (walletId) | Ordem **dentro** da carteira, paralelismo **entre** carteiras — espelha a serialização do domínio |
| `MessageDeduplicationId` | `eventId` | O eventId é estável e definido na transação de negócio: republicar não duplica. O broker deduplica pela **identidade do evento**, não pelo conteúdo |
| Fila | FIFO + DLQ FIFO com redrive | `maxReceiveCount: 5` |
| Visibility timeout | **Aplicado no recebimento**, pela aplicação | Sobrescreve o atributo da fila: o prazo é decisão da aplicação, não do provisionamento (ver §13, item 12) |

### 9.1 Contrato de roteamento — e um defeito conhecido

**Estado atual: o publicador e o consumidor apontam para a MESMA fila**
(`wager-transactions.fifo`). Como os dois envelopes são diferentes — a entrada tem
`messageId`/`type`, o evento de saída tem `eventId`/`eventType` — o consumidor
classifica cada evento publicado pela própria aplicação como **mensagem inválida**
e o encaminha para a DLQ.

Medido no smoke, com a aplicação em execução:

```
wager_consumer_events_total{outcome="received"} 5
wager_consumer_events_total{outcome="invalid"} 5
wager_consumer_events_total{outcome="dead_lettered"} 5
wager_consumer_events_total{outcome="processed"} 0
```

**Não afeta correção financeira:** nenhuma movimentação é duplicada nem perdida.
O dano é operacional — a DLQ deixa de significar "falhou por um motivo real", e um
alerta de profundidade de DLQ dispararia por ruído que a própria aplicação produz.

**Correção correta:** uma fila de **saída** separada (`wager-events.fifo` +
`wager-events-dlq.fifo`), com `SQS_EVENTS_QUEUE_NAME` / `SQS_EVENTS_QUEUE_URL` na
configuração, usada apenas pelo publicador da outbox.

Por que **provisionar** e não **filtrar por conteúdo**: o SQS não tem seleção por
atributo sem SNS, e "ignorar o que não é meu" dentro do consumidor abriria espaço
para descartar em silêncio uma entrada legítima malformada — trocaria um ruído
operacional por um risco de correção.

**Contrato de roteamento, independentemente da correção:** a fila de entrada
carrega **requisições** (`WagerTransactionRequested`); eventos de domínio são
**saída** e não devem voltar como entrada. Quem precisar reagir a eventos de
domínio (projeções, notificações) consome a fila de eventos — nunca a de
requisições.

### 9.2 Consumidor: classificação, retry e DLQ

- **Sucesso durável** → `Delete` (só depois do commit).
- **Rejeição de negócio** (`REJECTED`) → **delete**, porque é terminal e já foi
  persistida com seu evento. O enunciado permite explicitamente.
- **Falha transitória** (`KindTransient`, `KindInternal`, erro de banco) →
  `Release` via `ChangeMessageVisibility` com backoff exponencial. O prazo do
  retry **não** fica amarrado ao visibility timeout da fila.
- **Falha permanente** (payload inválido, conflito, `KindInvalid`/`KindConflict`)
  → **DLQ imediata**, sem gastar as 5 tentativas: repetir produziria o mesmo
  resultado.
- **Anti-thrash**: ao alcançar `MaxReceiveCount`, o consumidor antecipa o
  encaminhamento à DLQ em vez de deixar o SQS fazer uma rodada que já não teria
  chance. O número é o **mesmo** do redrive da fila.
- **`SIGTERM`**: o tratamento em andamento usa `context.WithoutCancel` com prazo,
  então ele é **concluído** (ou libera a visibilidade) dentro da janela; mensagens
  recebidas e ainda não iniciadas são devolvidas com atraso zero.
- **Corpo malformado nunca chega ao caso de uso**: o parser decide antes, e o
  teste `TestConsumerSendsInvalidMessageToRealDLQ` verifica zero tratamentos.

---

## 10. Autenticação e autorização

- **IdP**: Keycloak real, realm `wager` importado por JSON, fluxo
  `client_credentials` (provedores e serviço interno).
- **Validação de token**: assinatura RSA via JWKS, `iss`, `exp`, `nbf`, `aud`
  (`wager-api`) e `kid`; tolerância de clock skew. `kid` desconhecido dispara
  **refresh do JWKS** (rotação de chave sem reinício).
- **Papéis**: `provider` (envio e consulta das próprias operações), `internal`
  (carteira, ledger, reconciliação).
- **`providerId` vem SEMPRE do token**, nunca do corpo. Um provedor não opera em
  nome de outro, mesmo enviando o campo: o handler compara e devolve `403`.
- **Isolamento entre provedores**: outro provedor consultando a operação alheia
  recebe `404` (não `403`) — não se confirma a existência do recurso.
- **`AUTH_MODE=dev`** existe para testar o contrato HTTP sem IdP e é **rejeitado
  fora de `APP_ENV=local|dev|test`** pela validação de configuração.
- **`/health` e `/metrics` são públicos**: o orquestrador faz a probe antes de
  haver credencial, e o raspador do Prometheus não carrega token de provedor.

---

## 11. Fx e ordem de encerramento

Um único módulo (`internal/app`) conhece o Fx; domínio, casos de uso,
repositórios e HTTP recebem dependências por construtor. O grafo é validado sem
banco e sem rede por `fx.ValidateApp` (`TestModuleIsValid`) — foi esse teste que
pegou, entre outros, um provider recebendo `context.Context`, tipo que o Fx **não**
injeta.

O Fx executa os hooks de parada na ordem **inversa** do registro:

1. **workers** param primeiro (outbox, consumidor, referências), concluindo ou
   liberando o trabalho em andamento;
2. **servidor HTTP** para de aceitar requisições;
3. **pool do PostgreSQL** fecha por último.

Nenhum worker perde a conexão de que ainda precisa, e o último recurso a fechar é
o que todos usam.

Cada worker tem o seu runner e o seu hook, em vez de um hook único: o encerramento
é independente e o log diz qual deles terminou. Um worker desligado
(`OUTBOX_ENABLED=false`, `CONSUMER_ENABLED=false`, `REFERENCE_ENABLED=false`) não
registra hook — e a validação só aceita isso em ambiente local.

---

## 12. Observabilidade

**Log**: `slog` em JSON, com `correlationId` que liga a requisição aos eventos
gerados. No caminho SQS, o `correlationId` recebe o `messageId`, de modo que o
rastro começa na mensagem do broker.

**Métricas** — duas origens, por decisão:

| Origem | Métricas | Por quê |
|---|---|---|
| Instrumentadas | `http_requests_total{method,route,status}`, `http_request_duration_seconds`, `idempotent_replays_total`, `reconciliation_divergences_total` | São eventos que **não deixam rastro persistente**: uma repetição idempotente não cria linha; uma divergência não é gravada |
| Derivadas do banco | `transactions_by_status`, `transactions_by_failure_code`, `outbox_pending_events` | O banco **é** a fonte da verdade. Um contador em memória começaria em zero a cada reinício e não teria como ser conferido |
| Derivadas dos workers | `consumer_events_total{outcome}`, `reference_resolutions_total{outcome}` | Os contadores já existem (`ConsumerStats`, `ReferenceStats`); ler na raspagem evita espalhar Prometheus dentro dos laços |

Cobertura dos oito itens do enunciado §12: resultados por
status, duplicatas, retries, DLQ, conflitos de concorrência, atraso da outbox,
latência de processamento e divergências de reconciliação.

Três decisões de higiene que valem nota:

- **O rótulo das rotas é o template** (`/wallets/:walletId`), nunca o caminho
  concreto — senão cada UUID viraria uma série, e qualquer cliente poderia inflar
  a cardinalidade mandando caminhos aleatórios. Requisição sem rota vira
  `unmatched`.
- **Falha de leitura omite a série** em vez de expor zero. Um
  `outbox_pending_events 0` porque o banco estava fora é **pior que nenhuma
  métrica**: um alerta de "outbox parada" não dispararia.
- **A raspagem tem prazo** (`metricsReadTimeout`, 5s): a consulta derivada não
  pode prender o raspador, e indisponibilidade de métrica não deve parecer
  indisponibilidade da aplicação.

**Readiness** distingue os destinos: `postgres`, `sqs-outbound` e `sqs-inbound`.
Publicador e consumidor devolvem `Name() == "sqs"`, então um verificador nomeado
evita que um sobrescreva o outro no mapa de checks — um readiness que mente sobre
qual destino está fora é pior que um readiness incompleto.

---

## 13. Interpretações adotadas, limitações e trabalho não concluído

### 13.1 Interpretações adotadas do enunciado

1. **"Rejeições de negócio confirmadas são terminais e permitem a remoção da
   mensagem"** → removemos a mensagem; **não** vai para a DLQ. DLQ é para falha
   permanente de *processamento*, não para regra de negócio.
2. **"Erros permanentes ou tentativas esgotadas devem chegar à DLQ"** → separamos
   os dois casos: o payload inválido vai direto, sem consumir as 5 tentativas,
   porque repetir produziria o mesmo resultado.
3. **`messageId` do envelope** (o campo do corpo) é a identidade durável da inbox,
   conforme §10 do enunciado. O `messageId` do broker é usado para a DLQ e auditoria.
4. **`MaxAttempts` conta a tentativa inicial**, que é a que registra
   `attempts = 1`. Com `MaxAttempts = 8` a operação faz 8 tentativas de resolução
   (a inicial e 7 retomadas).
5. **`/metrics` público.** O enunciado pede controle de acesso à *mensageria*; para
   métricas, a decisão foi não exigir token e restringir por rede em produção
   (item 6 abaixo).
6. **Valor zero em `LOSS`** é obrigatório pelo schema, e não uma escolha do
   código: é a política por tipo, verificada em `docs/verify/01_schema.sh`.

### 13.2 Limitações e o que não está pronto

| # | Limitação | Impacto | Caminho |
|---|---|---|---|
| 1 | **TTL de referência não exercitado com idade real** — o teste usa `TTL=1ns` para que a idade em milissegundos o exceda | O caminho de código é o mesmo (`Exhausted(attempts, age)`), mas nada prova que uma operação de 25h com TTL de 24h é rejeitada | `UPDATE ... SET created_at = now() - interval '25 hours'` antes da retomada |
| 2 | **Sem partidas dobradas** — o ledger é de partida simples (um lançamento por movimento) | Não há detecção contábil de desbalanceamento; a reconciliação é soma contra saldo | Modelo de duas pernas com conta de contrapartida |
| 3 | **Sem tracing distribuído (OTel)** | A correlação é por `correlationId` em log; não há spans entre HTTP, banco, outbox e consumidor | Instrumentar os adaptadores com OTel |
| 4 | **Consumidor concorrente apenas entre instâncias** — um laço sequencial por processo | A vazão de um processo é limitada; o paralelismo vem das instâncias e do `MessageGroupId` | N laços concorrentes por processo, com o mesmo `SKIP LOCKED` |
| 5 | **DLQ sem reprocessamento automático** | Redrive de volta é operação manual | Alavanca de redrive documentada como operação de infraestrutura |
| 6 | **`/metrics` sem autenticação** | Expõe volume de negócio a quem alcançar a porta | Rede interna, `NetworkPolicy`, ou porta administrativa separada |
| 7 | **Métricas derivadas fazem `GROUP BY` na raspagem** | Custo cresce com o volume de `wager_transactions` | Tabela de agregados atualizada no commit |
| 8 | **`HandlingTimeout` do consumidor = `CONSUMER_SHUTDOWN_GRACE`** | Um valor para dois conceitos (prazo de tratamento e janela de encerramento); operação legítima acima do prazo gasta uma tentativa | `CONSUMER_HANDLING_TIMEOUT` própria, guiada por medição |

| 9 | **Reconciliação sob demanda, por carteira** | Não há varredura periódica de todas as carteiras | Job periódico somando por faixa, com métrica de divergência |
| 10 | **Sem teste de carga nem dashboards** | A capacidade sob concorrência alta não foi medida além dos testes (50 paralelas) | Carga com k6, dashboards em Grafana |
| 11 | **Sem validação de existência de código ISO 4217** | `XYZ` passa na entrada e falha depois, como `WALLET_NOT_FOUND` | Tabela ISO embutida, se o produto exigir |
| 12 | **`CONSUMER_VISIBILITY_TIMEOUT` tem duas fontes** | A aplicação sobrescreve o atributo da fila; os dois precisam concordar (hoje, 30s em ambos). O valor efetivo vai no log de startup | Uma fonte: só a aplicação, com o atributo da fila como fallback documentado |
| 13 | **`exhaustReference` assume carteira existente** | Se a carteira sumisse, a pendência ficaria em laço em vez de ser finalizada | Tratar `ErrNotFound` marcando `FAILED` em vez de `REJECTED` |
| 14 | **`money.UnmarshalJSON` aceita negativos** | Barrado pela política por tipo no schema, e não na entrada | Usar `ParseExternalAmount` no caminho de entrada externa |
| 15 | **JWKS sem rotação proativa** | A chave é buscada de novo no primeiro `kid` desconhecido | Refresh periódico em background |
| 16 | **Fila de saída e de entrada são a mesma** | O consumidor lê os eventos publicados pela própria aplicação e os encaminha à DLQ: a DLQ perde o significado e alertas de profundidade disparam por ruído. **Não afeta correção financeira** | Provisionar `wager-events.fifo` + DLQ e apontar só o publicador para ela (ver §9.1) |

### 13.3 Dívidas de teste que valem registro

- O teste de reentrega do consumidor depende de **visibility timeout curto
  aplicado pela aplicação** (1s). Ele existe para provar que a configuração é
  efetiva; se voltar a levar 30s, a configuração deixou de valer.
- Os testes de integração usam `TEST_*_REQUIRED=1` para que **não** possam passar
  por skip: pular silenciosamente produziria um "ok" que não prova integração
  real, que é exatamente o que o critério de avaliação quer evitar.
- Os testes que dependem de tempo usam **backoff curto por configuração**, nunca
  `sleep` longo: o caminho de código é o mesmo, e o teste roda em milissegundos.

---

## 14. Como verificar

```sh
# Unidade + domínio, sem infraestrutura
make test

# Integração completa: PostgreSQL, Keycloak e SQS REAIS
docker compose up -d postgres ministack keycloak
make test-integration
```

Alvos específicos:

| Alvo | O que prova |
|---|---|
| `make test-oidc` | Autenticação contra o Keycloak real (token válido, expirado, claims) |
| `make test-broker` | Publicação e leitura contra o SQS emulado real (grupo, dedup, visibilidade, DLQ) |
| `make test-consumer` | Consumidor SQS ponta a ponta com Postgres real (atomia, reentrega, DLQ) |
| `make schema-check` | Constraints, triggers e imutabilidade do ledger num banco descartável |
| `make demo` | Roteiro dos cenários de concorrência e recuperação (enunciado §13) na stack viva |

E a verificação que fecha tudo, também no script de demonstração: **comparar o
saldo armazenado com a soma de créditos menos débitos do ledger, carteira por
carteira**. É a única checagem que pega divergência introduzida por qualquer
caminho, incluindo os que não têm teste dedicado.

