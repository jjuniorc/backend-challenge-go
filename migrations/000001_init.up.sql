-- 000001_init.up.sql
-- Schema inicial do desafio: carteiras, transações de aposta, ledger append-only,
-- inbox e outbox. Toda invariante financeira relevante é imposta AQUI (constraint,
-- índice único ou trigger), independentemente de locks locais ou da deduplicação
-- do SQS, conforme exigido pela seção 5 do README.

-- ─────────────────────────────────────────────────────────────
-- wallets: raiz do agregado financeiro
-- ─────────────────────────────────────────────────────────────
CREATE TABLE wallets (
    id            UUID        PRIMARY KEY,
    player_id     TEXT        NOT NULL,
    currency      TEXT        NOT NULL,
    balance_minor BIGINT      NOT NULL,
    version       BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,

    CONSTRAINT wallets_player_id_not_blank  CHECK (length(btrim(player_id)) > 0),
    CONSTRAINT wallets_currency_iso4217     CHECK (currency ~ '^[A-Z]{3}$'),
    -- Garantia 8: não negatividade imposta pelo schema (rede de segurança do lock)
    CONSTRAINT wallets_balance_non_negative CHECK (balance_minor >= 0),
    CONSTRAINT wallets_version_positive     CHECK (version >= 1),
    CONSTRAINT wallets_updated_at_order     CHECK (updated_at >= created_at),
    -- (playerId, currency) identifica uma única carteira
    CONSTRAINT wallets_player_currency_uniq UNIQUE (player_id, currency),
    -- Alvo das FKs compostas que garantem "moeda da movimentação = moeda da carteira"
    CONSTRAINT wallets_id_currency_uniq     UNIQUE (id, currency)
);

-- ─────────────────────────────────────────────────────────────
-- wager_transactions: operações internas (OPENING) e externas (HTTP/SQS)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE wager_transactions (
    id                       UUID        PRIMARY KEY,
    origin                   TEXT        NOT NULL,
    kind                     TEXT        NOT NULL,
    status                   TEXT        NOT NULL,
    wallet_id                UUID        NOT NULL,
    player_id                TEXT        NOT NULL,
    currency                 TEXT        NOT NULL,
    amount_minor             BIGINT      NOT NULL,

    -- Identificadores externos (nulos em OPENING)
    provider_id              TEXT,
    external_transaction_id  TEXT,
    idempotency_key          TEXT,
    payload_hash             TEXT,
    round_id                 TEXT,
    game_id                  TEXT,

    -- Referências e resultado
    external_reference_id    TEXT,
    reference_transaction_id UUID        REFERENCES wager_transactions(id),
    failure_code             TEXT,
    failure_message          TEXT,
    resulting_balance_minor  BIGINT,

    -- Retomada durável de PENDING_REFERENCE
    reference_attempts       INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at          TIMESTAMPTZ,

    occurred_at              TIMESTAMPTZ NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL,
    updated_at               TIMESTAMPTZ NOT NULL,

    CONSTRAINT wt_origin_valid   CHECK (origin IN ('INTERNAL','EXTERNAL')),
    CONSTRAINT wt_kind_valid     CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
    CONSTRAINT wt_status_valid   CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
    CONSTRAINT wt_currency_iso4217 CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT wt_amount_non_negative CHECK (amount_minor >= 0),

    -- A moeda da operação tem de ser a moeda da carteira (invariante 6.2)
    CONSTRAINT wt_wallet_currency_fk FOREIGN KEY (wallet_id, currency)
        REFERENCES wallets (id, currency),

    -- Política de zero por tipo: BET/WIN/REFUND/ROLLBACK exigem > 0; LOSS exige 0; OPENING > 0
    CONSTRAINT wt_positive_amount_kinds  CHECK (kind NOT IN ('BET','WIN','REFUND','ROLLBACK') OR amount_minor > 0),
    CONSTRAINT wt_loss_zero_amount       CHECK (kind <> 'LOSS' OR amount_minor = 0),
    CONSTRAINT wt_opening_positive_amount CHECK (kind <> 'OPENING' OR amount_minor > 0),

    -- OPENING é reservado à abertura interna
    CONSTRAINT wt_opening_is_internal CHECK (kind <> 'OPENING' OR origin = 'INTERNAL'),

    -- Metadados externos obrigatórios em externas e ausentes em internas
    CONSTRAINT wt_external_metadata_required CHECK (
        origin <> 'EXTERNAL' OR (
            provider_id             IS NOT NULL AND length(btrim(provider_id)) > 0 AND
            external_transaction_id IS NOT NULL AND length(btrim(external_transaction_id)) > 0 AND
            idempotency_key         IS NOT NULL AND length(btrim(idempotency_key)) > 0 AND
            payload_hash            IS NOT NULL AND length(btrim(payload_hash)) > 0
        )
    ),
    CONSTRAINT wt_internal_metadata_absent CHECK (
        origin <> 'INTERNAL' OR (
            provider_id IS NULL AND external_transaction_id IS NULL AND
            idempotency_key IS NULL AND payload_hash IS NULL
        )
    ),

    -- Reversões exigem referenceExternalTransactionId
    CONSTRAINT wt_reversal_requires_external_reference CHECK (
        kind NOT IN ('REFUND','ROLLBACK') OR external_reference_id IS NOT NULL
    ),

    -- Resultado financeiro obrigatório em PROCESSED e proibido fora dele
    CONSTRAINT wt_processed_requires_result_balance CHECK (
        status <> 'PROCESSED' OR resulting_balance_minor IS NOT NULL
    ),
    CONSTRAINT wt_result_balance_only_when_processed CHECK (
        status = 'PROCESSED' OR resulting_balance_minor IS NULL
    ),
    CONSTRAINT wt_result_balance_non_negative CHECK (
        resulting_balance_minor IS NULL OR resulting_balance_minor >= 0
    ),

    -- Rejeição/falha exigem failureCode estável e auditável
    CONSTRAINT wt_failure_code_when_terminal_negative CHECK (
        status NOT IN ('REJECTED','FAILED') OR (failure_code IS NOT NULL AND length(btrim(failure_code)) > 0)
    ),

    CONSTRAINT wt_updated_at_order CHECK (updated_at >= created_at),

    -- Alvo da FK composta do ledger: garante que o lançamento pertence à
    -- MESMA carteira da transação que o originou.
    CONSTRAINT wt_id_wallet_uniq UNIQUE (id, wallet_id)
);

-- Unicidade da chave de idempotência por provedor
CREATE UNIQUE INDEX wt_provider_idempotency_key_uniq
    ON wager_transactions (provider_id, idempotency_key)
    WHERE origin = 'EXTERNAL';

-- Uma operação financeira identificada por (providerId, externalTransactionId)
-- não pode ser reaplicada usando outra chave
CREATE UNIQUE INDEX wt_provider_external_id_uniq
    ON wager_transactions (provider_id, external_transaction_id)
    WHERE origin = 'EXTERNAL';

-- Impede crédito inicial duplicado: no máximo uma OPENING por carteira
CREATE UNIQUE INDEX wt_opening_per_wallet_uniq
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

-- Uma referência não recebe duas reversões bem-sucedidas.
-- Como o índice é sobre a própria referência (e não sobre o par tipo+referência),
-- ele também impede REFUND e ROLLBACK simultâneos sobre a MESMA aposta, que
-- devolveriam o mesmo débito duas vezes. Um ROLLBACK de um REFUND aponta para o
-- REFUND (referência distinta), então continua permitido.
CREATE UNIQUE INDEX wt_single_successful_reversal_per_reference
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');

CREATE INDEX wt_wallet_ledger_cursor_idx ON wager_transactions (wallet_id, created_at DESC, id DESC);
CREATE INDEX wt_pending_reference_idx    ON wager_transactions (next_attempt_at, id)
    WHERE status = 'PENDING_REFERENCE';

-- ─────────────────────────────────────────────────────────────
-- wallet_ledger_entries: ledger append-only
-- ─────────────────────────────────────────────────────────────
CREATE TABLE wallet_ledger_entries (
    id                   UUID        PRIMARY KEY,
    wallet_id            UUID        NOT NULL,
    transaction_id       UUID        NOT NULL,
    direction            TEXT        NOT NULL,
    currency             TEXT        NOT NULL,
    amount_minor         BIGINT      NOT NULL,
    balance_before_minor BIGINT      NOT NULL,
    balance_after_minor  BIGINT      NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL,

    CONSTRAINT wle_direction_valid   CHECK (direction IN ('DEBIT','CREDIT')),
    CONSTRAINT wle_currency_iso4217  CHECK (currency ~ '^[A-Z]{3}$'),

    -- O lançamento pertence à mesma carteira da transação e a moeda confere
    CONSTRAINT wle_transaction_wallet_fk FOREIGN KEY (transaction_id, wallet_id)
        REFERENCES wager_transactions (id, wallet_id),
    CONSTRAINT wle_wallet_currency_fk FOREIGN KEY (wallet_id, currency)
        REFERENCES wallets (id, currency),
    CONSTRAINT wle_amount_positive   CHECK (amount_minor > 0),
    CONSTRAINT wle_balance_before_non_negative CHECK (balance_before_minor >= 0),
    CONSTRAINT wle_balance_after_non_negative  CHECK (balance_after_minor >= 0),

    -- Invariante do lançamento: balanceAfter = balanceBefore ± amount
    CONSTRAINT wle_direction_consistent CHECK (
        (direction = 'DEBIT'  AND balance_after_minor = balance_before_minor - amount_minor) OR
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
    ),

    -- Unicidade por (walletId, transactionId)
    CONSTRAINT wle_wallet_transaction_uniq UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX wle_wallet_cursor_idx ON wallet_ledger_entries (wallet_id, created_at DESC, id DESC);

-- Imutabilidade: correções financeiras exigem NOVOS lançamentos.
CREATE OR REPLACE FUNCTION wallet_ledger_entries_immutable()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries é append-only: % não é permitido', TG_OP
        USING ERRCODE = 'restrict_violation';
END;
$$;

CREATE TRIGGER wle_block_update
    BEFORE UPDATE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_immutable();

CREATE TRIGGER wle_block_delete
    BEFORE DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_immutable();

-- ─────────────────────────────────────────────────────────────
-- inbox_messages: deduplicação durável do consumidor SQS
-- ─────────────────────────────────────────────────────────────
CREATE TABLE inbox_messages (
    id             BIGSERIAL   PRIMARY KEY,
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    payload_hash   TEXT        NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL,
    completed_at   TIMESTAMPTZ,
    transaction_id UUID        REFERENCES wager_transactions(id),

    CONSTRAINT inbox_consumer_name_present CHECK (length(btrim(consumer_name)) > 0),
    CONSTRAINT inbox_message_id_present    CHECK (length(btrim(message_id)) > 0),
    CONSTRAINT inbox_payload_hash_present  CHECK (length(btrim(payload_hash)) > 0),
    CONSTRAINT inbox_consumer_message_uniq UNIQUE (consumer_name, message_id)
);

-- ─────────────────────────────────────────────────────────────
-- outbox_events: publicação transacional (eventos só saem após o commit)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE outbox_events (
    id              BIGSERIAL   PRIMARY KEY,
    event_id        UUID        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL,
    payload         JSONB       NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    locked_until    TIMESTAMPTZ,
    locked_by       TEXT,
    published_at    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL,

    -- eventId estável: republicações preservam a identidade do evento e o
    -- INSERT ... ON CONFLICT (event_id) DO NOTHING torna a reprocessagem segura
    CONSTRAINT outbox_event_id_uniq            UNIQUE (event_id),
    CONSTRAINT outbox_event_version_positive   CHECK (event_version >= 1),
    CONSTRAINT outbox_attempts_non_negative    CHECK (attempts >= 0),
    CONSTRAINT outbox_published_releases_lock  CHECK (published_at IS NULL OR locked_until IS NULL)
);

CREATE INDEX outbox_pending_idx ON outbox_events (next_attempt_at, id) WHERE published_at IS NULL;
