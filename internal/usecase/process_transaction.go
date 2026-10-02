package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wallet"
	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/payloadhash"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// ProcessTransaction processa uma operação financeira externa.
//
// É o MESMO caso de uso para HTTP e SQS: ambos montam a mesma
// ProcessTransactionCommand e recebem as mesmas garantias de idempotência.
type ProcessTransaction struct {
	store           ports.Store
	ids             ports.IDGenerator
	clock           ports.Clock
	referencePolicy ReferencePolicy
}

// NewProcessTransaction injeta as dependências do caso de uso.
func NewProcessTransaction(store ports.Store, ids ports.IDGenerator, clock ports.Clock, policy ReferencePolicy) *ProcessTransaction {
	return &ProcessTransaction{
		store:           store,
		ids:             ids,
		clock:           clock,
		referencePolicy: policy.normalized(),
	}
}

// ProcessTransactionCommand é a entrada do caso de uso.
//
// Os campos são exatamente os campos de negócio: a chave de idempotência entra
// como campo próprio (não participa do hash) e os metadados de transporte
// (messageId, headers) não chegam aqui.
type ProcessTransactionCommand struct {
	ProviderID            string
	ExternalTransactionID string
	IdempotencyKey        string
	PlayerID              string
	WalletID              string
	RoundID               string
	GameID                string
	Kind                  string
	Money                 money.Money
	ReferenceExternalID   string
	CorrelationID         string
}

// ProcessTransactionResult é a saída do caso de uso.
type ProcessTransactionResult struct {
	TransactionID    string
	Status           wagertransaction.Status
	Balance          money.Money
	IdempotentReplay bool
	FailureCode      string
	FailureMessage   string
}

// InboxClaim identifica a mensagem de ENTRADA cujo tratamento deve ser
// registrado na mesma transação das alterações de domínio.
//
// A identidade aqui é a da MENSAGEM (consumerName + messageId), diferente da
// identidade da OPERAÇÃO (providerId + idempotencyKey): a primeira deduplica
// entregas do broker, a segunda deduplica o efeito financeiro.
type InboxClaim struct {
	ConsumerName string
	MessageID    string
	PayloadHash  string
	ReceivedAt   time.Time
}

// ExecuteOption ajusta o comportamento de Execute.
type ExecuteOption func(*executeOptions)

type executeOptions struct {
	inbox *InboxClaim
}

// WithInbox liga o tratamento de uma mensagem de entrada à transação do caso de
// uso: o registro do recebimento e a sua conclusão passam a compartilhar o mesmo
// commit das alterações de domínio, do ledger e da outbox (README §6.5).
//
// Sem esta opção o caso de uso se comporta exatamente como antes — é o caminho
// da camada HTTP, onde não existe mensagem a registrar.
func WithInbox(claim InboxClaim) ExecuteOption {
	return func(options *executeOptions) { options.inbox = &claim }
}

// prepared é o resultado do trabalho determinístico feito FORA da transação.
type prepared struct {
	transaction         wagertransaction.Transaction
	kind                wagertransaction.Kind
	payloadHash         string
	correlationID       string
	referenceExternalID string
}

// Execute processa a operação externa.
//
// Fluxo:
//  1. valida a entrada e calcula o hash canônico (fora da transação);
//  2. tenta resolver uma execução anterior da mesma operação (replay);
//  3. executa todo o trabalho restante em UMA transação SQL.
func (uc *ProcessTransaction) Execute(ctx context.Context, cmd ProcessTransactionCommand, opts ...ExecuteOption) (ProcessTransactionResult, error) {
	options := executeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	prep, err := uc.prepare(cmd)
	if err != nil {
		return ProcessTransactionResult{}, err
	}

	if options.inbox == nil {
		// Caminho HTTP: procura uma execução anterior ANTES de abrir a
		// transação, apenas como atalho de latência. A autoridade continua sendo
		// a constraint do banco, que decide a corrida.
		previous, found, err := uc.findExisting(ctx, cmd, prep.payloadHash)
		if err != nil {
			return ProcessTransactionResult{}, err
		}
		if found {
			return previous, nil
		}
	}

	var result ProcessTransactionResult
	err = uc.store.RunInTx(ctx, func(ctx context.Context, tx ports.Tx) error {
		// 0) Inbox: o registro do recebimento entra na MESMA transação das
		//    alterações de domínio, do ledger e da outbox.
		if options.inbox != nil {
			handled, replayed, err := uc.claimInbox(ctx, tx, *options.inbox)
			if err != nil {
				return err
			}
			if handled {
				result = replayed
				return nil
			}
		}

		r, err := uc.process(ctx, tx, prep)
		if err != nil {
			return err
		}
		result = r

		// A conclusão da inbox é confirmada no MESMO commit. Para uma operação
		// que ficou PENDING_REFERENCE isso é correto: a pendência já está
		// persistida com seu agendamento durável e o worker de referências
		// assume a continuidade (README §6.5).
		if options.inbox != nil {
			if err := tx.Inbox().Complete(ctx, options.inbox.ConsumerName, options.inbox.MessageID, r.TransactionID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Corrida na chave de idempotência: outra instância confirmou primeiro.
		// O INSERT é o primeiro passo da transação, logo nada além dele chegou
		// a ser executado antes do rollback; basta reavaliar o registro
		// vencedor, que agora está visível.
		if errors.Is(err, ports.ErrIdempotencyConflict) {
			previous, found, findErr := uc.findExisting(ctx, cmd, prep.payloadHash)
			if findErr != nil {
				return ProcessTransactionResult{}, findErr
			}
			if found {
				// A operação já havia sido aplicada — por outra entrega da
				// mesma mensagem, ou pelo mesmo pedido enviado via HTTP. A
				// transação da inbox foi revertida junto com o rollback, então o
				// registro do recebimento é gravado agora, em transação própria,
				// para que a mensagem tenha sua linha de inbox concluída como
				// qualquer outra.
				if options.inbox != nil {
					if inboxErr := uc.recordInboxReplay(ctx, *options.inbox, previous.TransactionID); inboxErr != nil {
						return ProcessTransactionResult{}, inboxErr
					}
				}
				return previous, nil
			}
		}
		return ProcessTransactionResult{}, err
	}
	return result, nil
}

// findExisting localiza uma execução anterior da MESMA operação e decide entre
// replay e conflito.
//
// A operação tem duas identidades duráveis:
//
//   - (providerId, idempotencyKey), a chave enviada pelo provedor;
//   - (providerId, externalTransactionId), a identidade da operação financeira.
//
// As duas consultas rodam em autocommit, então uma operação concorrente pode
// ficar visível ENTRE elas: procurar pela chave, não achar, e logo depois achar
// pela identidade externa é um cenário normal, não um conflito. Por isso o
// desfecho é decidido pela CHAVE, nunca pela ordem das consultas:
//
//   - chave igual  -> replay (mesma operação, mesmo pedido);
//   - chave diferente para a mesma operação -> conflito.
func (uc *ProcessTransaction) findExisting(ctx context.Context, cmd ProcessTransactionCommand, payloadHash string) (ProcessTransactionResult, bool, error) {
	byKey, err := uc.store.Transactions().FindByProviderAndIdempotencyKey(ctx, cmd.ProviderID, cmd.IdempotencyKey)
	switch {
	case err == nil:
		result, replayErr := uc.replay(ctx, byKey, payloadHash)
		return result, true, replayErr
	case !errors.Is(err, ports.ErrNotFound):
		return ProcessTransactionResult{}, false, err
	}

	byExternal, err := uc.store.Transactions().FindByProviderAndExternalID(ctx, cmd.ProviderID, cmd.ExternalTransactionID)
	switch {
	case err == nil:
		if byExternal.IdempotencyKey() == cmd.IdempotencyKey {
			result, replayErr := uc.replay(ctx, byExternal, payloadHash)
			return result, true, replayErr
		}
		return ProcessTransactionResult{}, false, domainerr.New(domainerr.KindConflict, CodeIdempotencyKeyMismatch,
			"a operação já está registrada sob outra chave de idempotência")
	case !errors.Is(err, ports.ErrNotFound):
		return ProcessTransactionResult{}, false, err
	}

	return ProcessTransactionResult{}, false, nil
}

// prepare valida a entrada, calcula o hash canônico e constrói a transação.
func (uc *ProcessTransaction) prepare(cmd ProcessTransactionCommand) (prepared, error) {
	kind, err := wagertransaction.ParseKind(cmd.Kind)
	if err != nil {
		return prepared{}, err
	}
	if kind == wagertransaction.KindOpening {
		return prepared{}, wagertransaction.ErrOpeningNotExternal
	}
	if !cmd.Money.IsValid() {
		return prepared{}, domainerr.New(domainerr.KindInvalid, "PROCESS_TRANSACTION_INVALID_MONEY",
			"valor monetário inválido")
	}

	hash, err := payloadhash.Compute(payloadhash.Input{
		ProviderID:            cmd.ProviderID,
		ExternalTransactionID: cmd.ExternalTransactionID,
		PlayerID:              cmd.PlayerID,
		WalletID:              cmd.WalletID,
		RoundID:               cmd.RoundID,
		GameID:                cmd.GameID,
		Kind:                  cmd.Kind,
		Money:                 cmd.Money,
		ExternalReferenceID:   cmd.ReferenceExternalID,
	})
	if err != nil {
		return prepared{}, err
	}

	id, err := uc.ids.NewID()
	if err != nil {
		return prepared{}, err
	}
	now := uc.clock.Now()

	transaction, err := wagertransaction.NewExternal(wagertransaction.ExternalParams{
		ID:                    id,
		ProviderID:            cmd.ProviderID,
		ExternalTransactionID: cmd.ExternalTransactionID,
		IdempotencyKey:        cmd.IdempotencyKey,
		PayloadHash:           hash,
		WalletID:              cmd.WalletID,
		PlayerID:              cmd.PlayerID,
		RoundID:               cmd.RoundID,
		GameID:                cmd.GameID,
		Kind:                  kind,
		Money:                 cmd.Money,
		ReferenceExternalID:   cmd.ReferenceExternalID,
		Now:                   now,
	})
	if err != nil {
		return prepared{}, err
	}

	correlationID := strings.TrimSpace(cmd.CorrelationID)
	if correlationID == "" {
		correlationID = transaction.ID()
	}

	return prepared{
		transaction:         transaction,
		kind:                kind,
		payloadHash:         hash,
		correlationID:       correlationID,
		referenceExternalID: cmd.ReferenceExternalID,
	}, nil
}

// process executa todo o trabalho dentro de UMA transação SQL.
func (uc *ProcessTransaction) process(ctx context.Context, tx ports.Tx, p prepared) (ProcessTransactionResult, error) {
	// 1) Reivindica a chave de idempotência ANTES de qualquer lock.
	//    A transação nasce em PENDING e nunca é confirmada nesse estado: ou
	//    avança para um estado terminal no mesmo commit, ou o commit inteiro
	//    não acontece. Por isso não existe PENDING órfão para retomar.
	if err := tx.Transactions().Insert(ctx, p.transaction); err != nil {
		return ProcessTransactionResult{}, err
	}

	// 2) Trava a carteira. Operações da MESMA carteira serializam aqui;
	//    carteiras distintas seguem em paralelo.
	w, err := tx.Wallets().GetForUpdate(ctx, p.transaction.WalletID())
	if err != nil {
		if errors.Is(err, ports.ErrNotFound) {
			return ProcessTransactionResult{}, domainerr.New(domainerr.KindNotFound, CodeWalletNotFound,
				"a carteira informada não existe")
		}
		return ProcessTransactionResult{}, err
	}
	if w.PlayerID() != p.transaction.PlayerID() {
		return uc.reject(ctx, tx, p, w, CodeWalletPlayerMismatch,
			"a carteira não pertence ao jogador informado")
	}
	if w.Currency() != p.transaction.Currency() {
		return uc.reject(ctx, tx, p, w, CodeWalletCurrencyMismatch,
			"a moeda da operação difere da moeda da carteira")
	}

	now := uc.clock.Now()

	// 3) Resolve a referência, quando aplicável.
	var reference *wagertransaction.Transaction
	if p.referenceExternalID != "" {
		ref, err := tx.Transactions().FindByProviderAndExternalID(ctx, p.transaction.ProviderID(), p.referenceExternalID)
		switch {
		case errors.Is(err, ports.ErrNotFound):
			// A referência ainda não chegou: espera com backoff durável.
			return uc.pendingReference(ctx, tx, p, w)
		case err != nil:
			return ProcessTransactionResult{}, err
		}
		if ref.Status() != wagertransaction.StatusProcessed {
			// A referência existe mas ainda não concluiu: continua aguardando,
			// porque ela ainda pode avançar.
			return uc.pendingReference(ctx, tx, p, w)
		}
		if err := validateReference(p.transaction, ref); err != nil {
			return uc.reject(ctx, tx, p, w, domainerr.CodeOf(err), err.Error())
		}
		reversed, err := tx.Transactions().HasSuccessfulReversal(ctx, ref.ID())
		if err != nil {
			return ProcessTransactionResult{}, err
		}
		if reversed {
			return uc.reject(ctx, tx, p, w, CodeReferenceAlreadyReversed,
				"a transação referenciada já possui reversão bem-sucedida")
		}
		// Persiste a referência interna resolvida. É ela que torna a reversão
		// auditável e é a base do índice wt_single_successful_reversal_per_reference,
		// que impede devolver o mesmo débito duas vezes.
		if err := p.transaction.ResolveReference(ref.ID(), now); err != nil {
			return ProcessTransactionResult{}, err
		}
		reference = &ref
	}

	return uc.applyOperation(ctx, tx, p, w, reference, now)
}

// applyOperation define o movimento, aplica as alterações financeiras (saldo e
// ledger) e grava os eventos, tudo dentro da transação corrente e no MESMO
// commit do estado da operação.
//
// Extraído de process() para ser compartilhado com a retomada de
// PENDING_REFERENCE. A diferença entre os dois caminhos está ANTES daqui:
//
//   - a criação reivindica a chave de idempotência (INSERT) e resolve a
//     referência como uma etapa opcional;
//   - a retomada carrega uma operação que já existe e tem a resolução da
//     referência como o próprio objetivo.
//
// Daqui para baixo a regra é idêntica, e é justamente por isso que não pode ser
// duplicada: duas implementações da movimentação de dinheiro divergiriam em
// algum detalhe — arredondamento, ordem de débito, evento faltante — e o erro
// seria silencioso e financeiro.
func (uc *ProcessTransaction) applyOperation(
	ctx context.Context,
	tx ports.Tx,
	p prepared,
	w wallet.Wallet,
	reference *wagertransaction.Transaction,
	now time.Time,
) (ProcessTransactionResult, error) {
	// 4) Define o movimento. ROLLBACK é o contrário do movimento referenciado.
	movement := p.kind.Movement()
	if p.kind == wagertransaction.KindRollback {
		if reference == nil {
			return ProcessTransactionResult{}, domainerr.New(domainerr.KindInternal,
				"REVERSAL_WITHOUT_REFERENCE", "ROLLBACK sem referência resolvida")
		}
		opposite, err := wagertransaction.OppositeOf(reference.Kind())
		if err != nil {
			return uc.reject(ctx, tx, p, w, CodeReferenceKindNotReversible, err.Error())
		}
		movement = opposite
	}

	balanceBefore := w.Balance()
	versionBefore := w.Version()

	var entry *ledger.Entry
	switch movement {
	case wagertransaction.MovementNone:
		// LOSS: sem movimentação, sem lançamento e sem incremento de versão.
		// Ainda assim produz WagerTransactionProcessed (sem WalletBalanceChanged).

	case wagertransaction.MovementDebit:
		if err := w.Debit(p.transaction.Amount(), now); err != nil {
			if errors.Is(err, wallet.ErrInsufficientFunds) {
				// Código distinto para reversão: o provedor precisa saber se
				// foi a aposta que não tinha saldo ou a reversão que não cabe.
				code := CodeWalletInsufficientFunds
				if p.kind.IsReversal() {
					code = CodeReversalInsufficientFunds
				}
				return uc.reject(ctx, tx, p, w, code, "saldo insuficiente para o débito")
			}
			return ProcessTransactionResult{}, err
		}
		built, err := uc.buildEntry(p, w, ledger.DirectionDebit, balanceBefore, now)
		if err != nil {
			return ProcessTransactionResult{}, err
		}
		entry = &built

	case wagertransaction.MovementCredit:
		if err := w.Credit(p.transaction.Amount(), now); err != nil {
			if errors.Is(err, money.ErrOverflow) {
				return uc.reject(ctx, tx, p, w, CodeWalletBalanceOverflow,
					"o crédito excederia o limite representável do saldo")
			}
			return ProcessTransactionResult{}, err
		}
		built, err := uc.buildEntry(p, w, ledger.DirectionCredit, balanceBefore, now)
		if err != nil {
			return ProcessTransactionResult{}, err
		}
		entry = &built

	default:
		return ProcessTransactionResult{}, domainerr.New(domainerr.KindInternal, "MOVEMENT_NOT_IMPLEMENTED",
			fmt.Sprintf("movimento %q não implementado", movement))
	}

	// 5) Persiste lançamento, saldo e estado no mesmo commit.
	if entry != nil {
		if err := tx.Ledger().Insert(ctx, *entry); err != nil {
			return ProcessTransactionResult{}, err
		}
		if err := tx.Wallets().UpdateBalance(ctx, w, versionBefore); err != nil {
			return ProcessTransactionResult{}, err
		}
	}

	if err := p.transaction.MarkProcessed(w.Balance(), now); err != nil {
		return ProcessTransactionResult{}, err
	}
	if err := tx.Transactions().Update(ctx, p.transaction); err != nil {
		return ProcessTransactionResult{}, err
	}

	// 6) Eventos só entram na outbox DENTRO desta transação: nada é publicado
	//    antes do commit.
	if err := uc.publishProcessed(ctx, tx, p, w, entry, now); err != nil {
		return ProcessTransactionResult{}, err
	}

	return ProcessTransactionResult{
		TransactionID: p.transaction.ID(),
		Status:        wagertransaction.StatusProcessed,
		Balance:       w.Balance(),
	}, nil
}

// replay devolve o resultado persistido sem reaplicar a operação.
func (uc *ProcessTransaction) replay(ctx context.Context, existing wagertransaction.Transaction, payloadHash string) (ProcessTransactionResult, error) {
	if existing.PayloadHash() != payloadHash {
		return ProcessTransactionResult{}, domainerr.New(domainerr.KindConflict, CodeIdempotencyKeyReused,
			"chave de idempotência reutilizada com conteúdo diferente")
	}

	result, needsBalance := buildReplayResult(existing)
	if !needsBalance {
		return result, nil
	}

	// Estados sem resultado financeiro (PENDING_REFERENCE, REJECTED, FAILED)
	// devolvem o saldo atual da carteira para que o contrato tenha um valor.
	w, err := uc.store.Wallets().Get(ctx, existing.WalletID())
	if err != nil {
		return ProcessTransactionResult{}, err
	}
	result.Balance = w.Balance()
	return result, nil
}

// buildReplayResult monta o resultado a partir da transação persistida.
//
// Devolve needsBalance=true quando a transação não tem resultado financeiro
// próprio (PENDING_REFERENCE, REJECTED, FAILED): nesse caso quem chama lê o saldo
// da carteira — do store no caminho HTTP, da transação no caminho da inbox. É a
// única parte do replay que depende do contexto de leitura, e por isso ficou
// separada: o formato do resultado tem uma única definição.
func buildReplayResult(existing wagertransaction.Transaction) (ProcessTransactionResult, bool) {
	result := ProcessTransactionResult{
		TransactionID:    existing.ID(),
		Status:           existing.Status(),
		IdempotentReplay: true,
		FailureCode:      existing.FailureCode(),
		FailureMessage:   existing.FailureMessage(),
	}

	// Operação concluída: devolve o saldo OBSERVADO no processamento original,
	// mesmo que a carteira já tenha recebido outras movimentações.
	if existing.Status() == wagertransaction.StatusProcessed && existing.ResultBalance().IsValid() {
		result.Balance = existing.ResultBalance()
		return result, false
	}
	return result, true
}

// claimInbox registra o recebimento da mensagem DENTRO da transação corrente.
//
// Devolve handled=true quando a mensagem já havia sido tratada por uma entrega
// anterior: o resultado persistido é devolvido e o trabalho de domínio NÃO é
// repetido. É isso que torna inofensiva a reentrega que acontece quando o
// processo morre entre o commit e a remoção da mensagem da fila.
//
// O INSERT é o ponto de serialização entre entregas concorrentes da MESMA
// mensagem: a perdedora espera a vencedora confirmar (ou reverter) e então
// enxerga a linha concluída. A decisão é do banco, não do código — o mesmo
// princípio já usado na idempotência da operação.
func (uc *ProcessTransaction) claimInbox(ctx context.Context, tx ports.Tx, claim InboxClaim) (bool, ProcessTransactionResult, error) {
	inserted, err := tx.Inbox().Insert(ctx, ports.InboxMessage{
		ConsumerName: claim.ConsumerName,
		MessageID:    claim.MessageID,
		PayloadHash:  claim.PayloadHash,
		ReceivedAt:   claim.ReceivedAt,
	})
	if err != nil {
		return false, ProcessTransactionResult{}, err
	}
	if inserted {
		return false, ProcessTransactionResult{}, nil
	}

	existing, err := tx.Inbox().Get(ctx, claim.ConsumerName, claim.MessageID)
	if err != nil {
		return false, ProcessTransactionResult{}, err
	}

	if existing.PayloadHash != claim.PayloadHash {
		// Mesma identidade de mensagem com conteúdo diferente: entrega
		// corrompida ou trocada. Falha PERMANENTE — nenhuma alteração de domínio
		// é aplicada, e o rollback desfaz o próprio registro do recebimento.
		return false, ProcessTransactionResult{}, domainerr.New(domainerr.KindConflict, CodeInboxPayloadHashMismatch,
			"reentrega com conteúdo diferente para a mesma mensagem")
	}

	if !existing.Completed || existing.TransactionID == "" {
		// Registrada sem tratamento concluído: não há resultado a repetir.
		// Falha TRANSITÓRIA — outra entrega conclui, reaproveitando a linha.
		return false, ProcessTransactionResult{}, domainerr.New(domainerr.KindTransient, CodeInboxIncomplete,
			"mensagem registrada na inbox sem tratamento concluído")
	}

	transaction, err := tx.Transactions().GetByID(ctx, existing.TransactionID)
	if err != nil {
		return false, ProcessTransactionResult{}, err
	}

	replayed, needsBalance := buildReplayResult(transaction)
	if needsBalance {
		w, err := tx.Wallets().Get(ctx, transaction.WalletID())
		if err != nil {
			return false, ProcessTransactionResult{}, err
		}
		replayed.Balance = w.Balance()
	}
	return true, replayed, nil
}

// recordInboxReplay grava o registro de inbox de uma mensagem cujo EFEITO já
// estava persistido por outra execução.
//
// Acontece quando a operação venceu a corrida por outro caminho (outra entrega
// da mesma mensagem, ou o mesmo pedido enviado por HTTP): a transação da inbox
// foi revertida junto com o rollback, então o registro é gravado em transação
// própria. Sem isso a mensagem ficaria sem linha de inbox e uma reentrega futura
// não teria como ser reconhecida.
func (uc *ProcessTransaction) recordInboxReplay(ctx context.Context, claim InboxClaim, transactionID string) error {
	return uc.store.RunInTx(ctx, func(ctx context.Context, tx ports.Tx) error {
		handled, _, err := uc.claimInbox(ctx, tx, claim)
		if err != nil {
			return err
		}
		if handled {
			// Já registrada e concluída por outra entrega: nada a fazer.
			return nil
		}
		return tx.Inbox().Complete(ctx, claim.ConsumerName, claim.MessageID, transactionID)
	})
}

// reject encerra a operação como rejeição de negócio, persistindo o código de
// falha e o evento correspondente no mesmo commit.
func (uc *ProcessTransaction) reject(ctx context.Context, tx ports.Tx, p prepared, w wallet.Wallet, code, message string) (ProcessTransactionResult, error) {
	now := uc.clock.Now()

	if err := p.transaction.MarkRejected(code, message, now); err != nil {
		return ProcessTransactionResult{}, err
	}
	if err := tx.Transactions().Update(ctx, p.transaction); err != nil {
		return ProcessTransactionResult{}, err
	}

	eventID, err := uc.ids.NewID()
	if err != nil {
		return ProcessTransactionResult{}, err
	}
	event, err := events.NewWagerTransactionRejected(events.Meta{
		EventID:       eventID,
		CorrelationID: p.correlationID,
		OccurredAt:    now,
	}, events.RejectedParams{
		TransactionID:  p.transaction.ID(),
		ProviderID:     p.transaction.ProviderID(),
		WalletID:       p.transaction.WalletID(),
		Kind:           p.transaction.Kind(),
		FailureCode:    code,
		FailureMessage: message,
	})
	if err != nil {
		return ProcessTransactionResult{}, err
	}
	if err := tx.Outbox().Insert(ctx, event); err != nil {
		return ProcessTransactionResult{}, err
	}

	return ProcessTransactionResult{
		TransactionID:  p.transaction.ID(),
		Status:         wagertransaction.StatusRejected,
		Balance:        w.Balance(),
		FailureCode:    code,
		FailureMessage: message,
	}, nil
}

// pendingReference registra a espera pela referência com agendamento durável.
func (uc *ProcessTransaction) pendingReference(ctx context.Context, tx ports.Tx, p prepared, w wallet.Wallet) (ProcessTransactionResult, error) {
	const attempts = 1
	now := uc.clock.Now()

	if err := p.transaction.MarkPendingReference(now); err != nil {
		return ProcessTransactionResult{}, err
	}
	if err := tx.Transactions().Update(ctx, p.transaction); err != nil {
		return ProcessTransactionResult{}, err
	}
	if err := tx.Transactions().ScheduleReferenceRetry(ctx, p.transaction.ID(), attempts,
		now.Add(uc.referencePolicy.Backoff(attempts))); err != nil {
		return ProcessTransactionResult{}, err
	}

	eventID, err := uc.ids.NewID()
	if err != nil {
		return ProcessTransactionResult{}, err
	}
	event, err := events.NewWagerTransactionPendingReference(events.Meta{
		EventID:       eventID,
		CorrelationID: p.correlationID,
		OccurredAt:    now,
	}, events.PendingReferenceParams{
		TransactionID:                  p.transaction.ID(),
		ProviderID:                     p.transaction.ProviderID(),
		WalletID:                       p.transaction.WalletID(),
		Kind:                           p.transaction.Kind(),
		ReferenceExternalTransactionID: p.referenceExternalID,
		Attempts:                       attempts,
	})
	if err != nil {
		return ProcessTransactionResult{}, err
	}
	if err := tx.Outbox().Insert(ctx, event); err != nil {
		return ProcessTransactionResult{}, err
	}

	return ProcessTransactionResult{
		TransactionID: p.transaction.ID(),
		Status:        wagertransaction.StatusPendingReference,
		Balance:       w.Balance(),
	}, nil
}

// publishProcessed grava WagerTransactionProcessed e, quando houve alteração
// efetiva de saldo, WalletBalanceChanged.
func (uc *ProcessTransaction) publishProcessed(ctx context.Context, tx ports.Tx, p prepared, w wallet.Wallet, entry *ledger.Entry, now time.Time) error {
	processedEventID, err := uc.ids.NewID()
	if err != nil {
		return err
	}
	processed, err := events.NewWagerTransactionProcessed(events.Meta{
		EventID:       processedEventID,
		CorrelationID: p.correlationID,
		OccurredAt:    now,
	}, events.ProcessedParams{
		TransactionID: p.transaction.ID(),
		ProviderID:    p.transaction.ProviderID(),
		WalletID:      p.transaction.WalletID(),
		Kind:          p.transaction.Kind(),
		Status:        p.transaction.Status(),
		Money:         p.transaction.Amount(),
		Balance:       w.Balance(),
	})
	if err != nil {
		return err
	}
	if err := tx.Outbox().Insert(ctx, processed); err != nil {
		return err
	}

	// LOSS (e qualquer operação sem movimento) não gera WalletBalanceChanged.
	if entry == nil {
		return nil
	}

	balanceChangedID, err := uc.ids.NewID()
	if err != nil {
		return err
	}
	balanceChanged, err := events.NewWalletBalanceChanged(events.Meta{
		EventID:       balanceChangedID,
		CorrelationID: p.correlationID,
		CausationID:   processedEventID,
		OccurredAt:    now,
	}, events.BalanceChangedParams{
		WalletID:      entry.WalletID(),
		TransactionID: entry.TransactionID(),
		Direction:     entry.Direction(),
		Money:         entry.Amount(),
		BalanceBefore: entry.BalanceBefore(),
		BalanceAfter:  entry.BalanceAfter(),
		WalletVersion: w.Version(),
	})
	if err != nil {
		return err
	}
	return tx.Outbox().Insert(ctx, balanceChanged)
}

func (uc *ProcessTransaction) buildEntry(p prepared, w wallet.Wallet, direction ledger.Direction, balanceBefore money.Money, now time.Time) (ledger.Entry, error) {
	id, err := uc.ids.NewID()
	if err != nil {
		return ledger.Entry{}, err
	}
	return ledger.New(ledger.Params{
		ID:            id,
		WalletID:      w.ID(),
		TransactionID: p.transaction.ID(),
		Direction:     direction,
		Amount:        p.transaction.Amount(),
		BalanceBefore: balanceBefore,
		BalanceAfter:  w.Balance(),
		CreatedAt:     now,
	})
}

// validateReference exige que a operação e sua referência concordem em
// provedor, jogador, carteira, moeda, rodada e valor, e que o tipo referenciado
// seja reversível pelo tipo atual.
func validateReference(current, reference wagertransaction.Transaction) error {
	switch {
	case reference.ProviderID() != current.ProviderID():
		return domainerr.New(domainerr.KindRuleViolation, CodeReferenceProviderMismatch,
			"a transação referenciada pertence a outro provedor")
	case reference.PlayerID() != current.PlayerID():
		return domainerr.New(domainerr.KindRuleViolation, CodeReferencePlayerMismatch,
			"a transação referenciada pertence a outro jogador")
	case reference.WalletID() != current.WalletID():
		return domainerr.New(domainerr.KindRuleViolation, CodeReferenceWalletMismatch,
			"a transação referenciada pertence a outra carteira")
	case reference.Currency() != current.Currency():
		return domainerr.New(domainerr.KindCurrencyMismatch, CodeReferenceCurrencyMismatch,
			"a transação referenciada usa outra moeda")
	case reference.RoundID() != current.RoundID():
		return domainerr.New(domainerr.KindRuleViolation, CodeReferenceRoundMismatch,
			"a transação referenciada pertence a outra rodada")
	case !reference.Amount().Equal(current.Amount()):
		return domainerr.New(domainerr.KindRuleViolation, CodeReferenceAmountMismatch,
			"o valor da reversão difere do valor referenciado")
	}

	switch current.Kind() {
	case wagertransaction.KindRefund:
		if reference.Kind() != wagertransaction.KindBet {
			return domainerr.New(domainerr.KindRuleViolation, CodeReferenceKindNotRefundable,
				"REFUND só devolve uma BET processada")
		}
	case wagertransaction.KindRollback:
		if _, err := wagertransaction.OppositeOf(reference.Kind()); err != nil {
			return domainerr.New(domainerr.KindRuleViolation, CodeReferenceKindNotReversible,
				"ROLLBACK não se aplica a esta referência")
		}
	}
	return nil
}
