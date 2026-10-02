package usecase

import (
	"context"
	"errors"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// ResolutionOutcome é o desfecho de uma tentativa de resolução de referência.
//
// Os desfechos são quatro porque têm consequências diferentes e observabilidade
// diferente: RESOLVED e REJECTED são terminais, RESCHEDULED continua vivo, e
// EXHAUSTED é terminal por política (TTL/tentativas) — não por regra de negócio.
type ResolutionOutcome string

const (
	// OutcomeResolved: a referência foi encontrada e a operação foi aplicada.
	OutcomeResolved ResolutionOutcome = "RESOLVED"
	// OutcomeRescheduled: a referência ainda não está disponível; nova tentativa
	// agendada com backoff.
	OutcomeRescheduled ResolutionOutcome = "RESCHEDULED"
	// OutcomeRejected: rejeição definitiva por regra de negócio — a referência
	// apareceu, mas é incompatível, ou já possui reversão bem-sucedida.
	OutcomeRejected ResolutionOutcome = "REJECTED"
	// OutcomeExhausted: a política esgotou (tentativas ou TTL) e a operação foi
	// finalizada como REJECTED com o código REFERENCE_TIMEOUT.
	OutcomeExhausted ResolutionOutcome = "EXHAUSTED"
)

// PendingResolution descreve o que aconteceu em uma execução do worker de
// referências. É o insumo de log e de métrica.
type PendingResolution struct {
	// Attempted informa se havia operação vencida para tentar. false significa
	// "nada a fazer", que é o caso normal e não um erro.
	Attempted bool
	// TransactionID identifica a operação tentada.
	TransactionID string
	// Outcome é o desfecho da tentativa.
	Outcome ResolutionOutcome
	// Attempts é quantas tentativas de resolução já ocorreram INCLUINDO esta.
	Attempts int
	// NextAttemptAt é quando a próxima tentativa acontece. Zero exceto em
	// OutcomeRescheduled.
	NextAttemptAt time.Time
}

// ResolveNextPendingReference executa UMA tentativa de resolução de referência.
//
// # Uma tentativa = uma transação
//
// Tudo acontece num único RunInTx: a reserva (FOR UPDATE SKIP LOCKED), a leitura
// da operação, a resolução da referência, a aplicação financeira e a gravação do
// desfecho. Não há lease nem reserva a recuperar — se a instância cair, o
// rollback devolve o estado e a operação continua vencida para a próxima
// tentativa. É por isso que este worker é mais simples que o publicador da
// outbox (que publica um efeito externo e precisa de lease): ver ARCHITECTURE.md.
//
// Disso decorre uma regra, garantida por construção aqui: reservar e gravar o
// desfecho têm de ser o MESMO commit. Não existe caminho que reserve sem
// gravar — reservar sem gravar devolveria a operação à disputa, em laço.
//
// Erros devolvidos são de infraestrutura (banco). O chamador decide a espera da
// próxima rodada; a operação continua vencida e será tentada novamente.
func (uc *ProcessTransaction) ResolveNextPendingReference(ctx context.Context) (PendingResolution, error) {
	now := uc.clock.Now()

	var resolution PendingResolution
	err := uc.store.RunInTx(ctx, func(ctx context.Context, tx ports.Tx) error {
		pending, found, err := tx.Transactions().ClaimDueReference(ctx, now)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}

		resolution.Attempted = true
		resolution.TransactionID = pending.TransactionID

		outcome, attempts, nextAttemptAt, err := uc.attemptReference(ctx, tx, pending, now)
		if err != nil {
			return err
		}
		resolution.Outcome = outcome
		resolution.Attempts = attempts
		resolution.NextAttemptAt = nextAttemptAt
		return nil
	})
	if err != nil {
		return PendingResolution{}, err
	}
	return resolution, nil
}

// attemptReference executa o trabalho de uma tentativa, sempre dentro da
// transação aberta por ResolveNextPendingReference.
func (uc *ProcessTransaction) attemptReference(
	ctx context.Context,
	tx ports.Tx,
	pending ports.PendingReference,
	now time.Time,
) (ResolutionOutcome, int, time.Time, error) {
	attempts := pending.Attempts + 1

	// 1) A política é avaliada ANTES de qualquer leitura: ela decide com o que a
	//    reserva já traz (tentativas e idade). Verificar aqui evita carregar a
	//    operação e travar a carteira para, no fim, apenas rejeitar.
	//
	//    A pergunta é "ainda tenho tentativa disponível?", feita sobre as
	//    tentativas JÁ REALIZADAS (pending.Attempts) — e não sobre a que esta
	//    execução faria. Com MaxAttempts=8 a operação faz 8 tentativas de
	//    resolução (a inicial, que registra attempts=1, e 7 retomadas) antes de
	//    ser finalizada. Contar a tentativa corrente faria MaxAttempts=2 permitir
	//    apenas a inicial, o que não é o que a política promete.
	if uc.referencePolicy.Exhausted(pending.Attempts, pending.Age) {
		if err := uc.exhaustReference(ctx, tx, pending); err != nil {
			return "", 0, time.Time{}, err
		}
		return OutcomeExhausted, attempts, time.Time{}, nil
	}

	// 2) A reserva traz o mínimo; a aplicação precisa do agregado completo.
	transaction, err := tx.Transactions().GetByID(ctx, pending.TransactionID)
	if err != nil {
		return "", 0, time.Time{}, err
	}

	// 3) Trava a carteira: a MESMA serialização por carteira do caminho síncrono.
	w, err := tx.Wallets().GetForUpdate(ctx, transaction.WalletID())
	if err != nil {
		// Na prática não acontece: a FK wt_wallet_currency_fk garante que a
		// carteira existe desde o INSERT, e não há remoção de carteiras. Se
		// acontecer, é inconsistência de dados, não regra de negócio — o erro
		// sobe e a operação continua vencida, sem ser rejeitada por engano.
		return "", 0, time.Time{}, err
	}

	// 4) Localiza a referência. Ausente ou ainda não concluída => espera.
	ref, err := tx.Transactions().FindByProviderAndExternalID(ctx, transaction.ProviderID(), pending.ReferenceExternalID)
	switch {
	case errors.Is(err, ports.ErrNotFound):
		return uc.rescheduleReference(ctx, tx, transaction.ID(), attempts, now)
	case err != nil:
		return "", 0, time.Time{}, err
	}
	if ref.Status() != wagertransaction.StatusProcessed {
		// Existe, mas ainda não concluiu: continua aguardando, porque ela ainda
		// pode avançar.
		return uc.rescheduleReference(ctx, tx, transaction.ID(), attempts, now)
	}

	// 5) Mesmas validações do caminho síncrono. Elas são necessárias aqui porque
	//    a referência foi criada DEPOIS: pode pertencer a outro provedor,
	//    jogador, carteira ou rodada.
	p := preparedFor(transaction)
	if err := validateReference(transaction, ref); err != nil {
		if _, rejectErr := uc.reject(ctx, tx, p, w, domainerr.CodeOf(err), err.Error()); rejectErr != nil {
			return "", 0, time.Time{}, rejectErr
		}
		return OutcomeRejected, attempts, time.Time{}, nil
	}

	reversed, err := tx.Transactions().HasSuccessfulReversal(ctx, ref.ID())
	if err != nil {
		return "", 0, time.Time{}, err
	}
	if reversed {
		if _, rejectErr := uc.reject(ctx, tx, p, w, CodeReferenceAlreadyReversed,
			"a transação referenciada já possui reversão bem-sucedida"); rejectErr != nil {
			return "", 0, time.Time{}, rejectErr
		}
		return OutcomeRejected, attempts, time.Time{}, nil
	}

	// 6) Resolve a referência e APLICA pelo mesmo caminho financeiro do
	//    processamento síncrono. Não há segunda implementação da movimentação.
	//
	//    A resolução acontece em p.transaction — a CÓPIA que vai ao repositório —
	//    e não na variável local lida do banco. Resolver na local deixaria o
	//    commit sem reference_transaction_id, e o índice
	//    wt_single_successful_reversal_per_reference deixaria de enxergar esta
	//    reversão (NULL não colide em índice único): a MESMA aposta poderia ser
	//    revertida uma segunda vez. O caminho síncrono faz exatamente isto.
	if err := p.transaction.ResolveReference(ref.ID(), now); err != nil {
		return "", 0, time.Time{}, err
	}
	if _, err := uc.applyOperation(ctx, tx, p, w, &ref, now); err != nil {
		return "", 0, time.Time{}, err
	}
	return OutcomeResolved, attempts, time.Time{}, nil
}

// rescheduleReference adia a próxima tentativa com backoff exponencial.
//
// A gravação acontece na MESMA transação da reserva: é o que torna a reserva
// significativa. Sem ela, a operação continuaria vencida e seria reservada de
// novo imediatamente, em laço apertado.
func (uc *ProcessTransaction) rescheduleReference(
	ctx context.Context,
	tx ports.Tx,
	transactionID string,
	attempts int,
	now time.Time,
) (ResolutionOutcome, int, time.Time, error) {
	next := now.Add(uc.referencePolicy.Backoff(attempts))
	if err := tx.Transactions().ScheduleReferenceRetry(ctx, transactionID, attempts, next); err != nil {
		return "", 0, time.Time{}, err
	}
	return OutcomeRescheduled, attempts, next, nil
}

// exhaustReference finaliza a operação como REJECTED por esgotamento da
// política.
//
// Reaproveita reject(), que já persiste o estado, o código de falha e o evento
// WagerTransactionRejected no mesmo commit. A transição
// PENDING_REFERENCE -> REJECTED é permitida pelo domínio, então não há máquina
// de estados nova aqui.
func (uc *ProcessTransaction) exhaustReference(ctx context.Context, tx ports.Tx, pending ports.PendingReference) error {
	transaction, err := tx.Transactions().GetByID(ctx, pending.TransactionID)
	if err != nil {
		return err
	}
	w, err := tx.Wallets().GetForUpdate(ctx, transaction.WalletID())
	if err != nil {
		return err
	}

	_, err = uc.reject(ctx, tx, preparedFor(transaction), w, CodeReferenceTimeout,
		"a referência não foi resolvida dentro da política (número de tentativas ou TTL)")
	return err
}

// preparedFor reconstrói o insumo do trabalho financeiro a partir da operação
// persistida.
//
// O correlationId recebe o id da operação: a retomada é uma nova cadeia causal
// (não há requisição HTTP nem mensagem de entrada por trás dela), e esse id é o
// que liga os eventos gerados agora à operação que os originou.
func preparedFor(transaction wagertransaction.Transaction) prepared {
	return prepared{
		transaction:         transaction,
		kind:                transaction.Kind(),
		payloadHash:         transaction.PayloadHash(),
		correlationID:       transaction.ID(),
		referenceExternalID: transaction.ExternalReferenceID(),
	}
}
