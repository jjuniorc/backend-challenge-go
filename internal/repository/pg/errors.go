package pg

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// SQLSTATE do PostgreSQL usados na tradução.
const (
	sqlStateUniqueViolation     = "23505"
	sqlStateForeignKeyViolation = "23503"
	sqlStateNotNullViolation    = "23502"
	sqlStateCheckViolation      = "23514"
	sqlStateRestrictViolation   = "23001" // usado pelo trigger do ledger
)

// constraintSentinels mapeia o nome da constraint para o erro de domínio
// correspondente. Os nomes são os mesmos validados por docs/verify/01_schema.sh,
// então qualquer divergência entre schema e código aparece como ErrDuplicate
// genérico em vez de passar silenciosamente.
var constraintSentinels = map[string]error{
	"wt_provider_idempotency_key_uniq":            ports.ErrIdempotencyConflict,
	"wt_provider_external_id_uniq":                ports.ErrIdempotencyConflict,
	"wt_single_successful_reversal_per_reference": ports.ErrReferenceAlreadyReversed,
	"wallets_player_currency_uniq":                ports.ErrWalletAlreadyExists,
	"wt_opening_per_wallet_uniq":                  ports.ErrOpeningAlreadyExists,
}

// translate converte erros do driver em sentinelas de ports.
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.ErrNotFound
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}

	switch pgErr.Code {
	case sqlStateUniqueViolation:
		if sentinel, ok := constraintSentinels[pgErr.ConstraintName]; ok {
			return &ports.ConstraintError{Constraint: pgErr.ConstraintName, Err: sentinel}
		}
		return &ports.ConstraintError{Constraint: pgErr.ConstraintName, Err: ports.ErrDuplicate}

	case sqlStateCheckViolation, sqlStateForeignKeyViolation, sqlStateNotNullViolation:
		return &ports.ConstraintError{Constraint: pgErr.ConstraintName, Err: ports.ErrConstraintViolation}

	case sqlStateRestrictViolation:
		return &ports.ConstraintError{Constraint: pgErr.ConstraintName, Err: ports.ErrImmutable}

	default:
		return err
	}
}

// Translate converte um erro do driver nas sentinelas de ports.
//
// Exportado para ferramentas de suporte e diagnóstico que executam SQL fora de
// um repositório de domínio (por exemplo, os testes de integração que verificam
// a classificação de uma rejeição do banco).
func Translate(err error) error { return translate(err) }
