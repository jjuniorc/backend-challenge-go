// Package ports declara as interfaces que o domínio e os casos de uso exigem da
// infraestrutura, além das sentinelas de erro de persistência.
//
// O pacote não conhece pgx, SQL, SQS ou HTTP: apenas contratos. Isso mantém o
// caso de uso testável e deixa a implementação livre para trocar de tecnologia
// sem tocar em regra de negócio.
package ports

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/domainerr"

	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wagertransaction"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/wallet"
	"github.com/jjuniorc/backend-challenge-go/internal/events"
)

// Sentinelas de persistência, classificáveis com errors.Is.
var (
	ErrNotFound                 = errors.New("ports: registro não encontrado")
	ErrVersionConflict          = errors.New("ports: conflito de versão (possível lost update)")
	ErrIdempotencyConflict      = errors.New("ports: chave de idempotência já usada com outro conteúdo")
	ErrWalletAlreadyExists      = errors.New("ports: já existe carteira para (playerId, moeda)")
	ErrOpeningAlreadyExists     = errors.New("ports: já existe abertura para a carteira")
	ErrReferenceAlreadyReversed = errors.New("ports: referência já possui reversão bem-sucedida")
	ErrDuplicate                = errors.New("ports: violação de unicidade")
	ErrConstraintViolation      = errors.New("ports: violação de constraint")
	ErrImmutable                = errors.New("ports: registro append-only não aceita alteração")
)

// ConstraintError identifica a constraint violada e carrega a sentinela
// classificável correspondente. O nome da constraint é preservado porque é ele
// que aparece no erro do PostgreSQL e é a evidência de auditoria.
type ConstraintError struct {
	Constraint string
	Err        error
}

func (e *ConstraintError) Error() string {
	return fmt.Sprintf("%s (constraint %s)", e.Err.Error(), e.Constraint)
}

// Unwrap permite errors.Is(err, ErrIdempotencyConflict) e afins.
func (e *ConstraintError) Unwrap() error { return e.Err }

// ConstraintNameOf devolve o nome da constraint violada, ou "" se não houver.
func ConstraintNameOf(err error) string {
	var ce *ConstraintError
	if errors.As(err, &ce) {
		return ce.Constraint
	}
	return ""
}

// Papéis usados na autorização.
const (
	// RoleInternal restringe as operações de carteira ao serviço interno.
	RoleInternal = "internal"
	// RoleProvider restringe o envio de operações aos provedores de jogo.
	RoleProvider = "provider"
)

// Sentinelas de autenticação e autorização, classificáveis com errors.Is.
var (
	ErrMissingCredentials = domainerr.New(domainerr.KindUnauthenticated, "AUTH_MISSING_CREDENTIALS", "credenciais de autenticação ausentes")
	ErrInvalidCredentials = domainerr.New(domainerr.KindUnauthenticated, "AUTH_INVALID_CREDENTIALS", "credenciais de autenticação inválidas")
	ErrForbidden          = domainerr.New(domainerr.KindForbidden, "AUTH_FORBIDDEN", "acesso não autorizado")
)

// Identity é a identidade autenticada.
//
// O providerId autorizado vem SEMPRE da identidade, nunca do corpo da
// requisição: é isso que garante o isolamento entre provedores.
type Identity struct {
	Subject    string
	ProviderID string
	Roles      []string
}

// HasRole informa se a identidade possui o papel informado.
func (i Identity) HasRole(role string) bool {
	for _, r := range i.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Authenticator valida as credenciais de uma requisição e devolve a identidade.
//
// O adaptador recebe o valor BRUTO do header Authorization e decide se ele está
// ausente (ErrMissingCredentials), malformado (ErrInvalidCredentials) ou válido.
// Isso mantém o middleware HTTP idêntico entre a implementação de
// desenvolvimento e a integração OIDC.
type Authenticator interface {
	Authenticate(ctx context.Context, authorizationHeader string) (Identity, error)
}

// HealthChecker é um componente verificável pelo readiness.
type HealthChecker interface {
	Name() string
	Check(ctx context.Context) error
}

// LedgerCursor é a posição na paginação do ledger, ordenada por
// (created_at DESC, id DESC).
type LedgerCursor struct {
	CreatedAt time.Time
	ID        string
}

// LedgerPage é uma página do ledger.
type LedgerPage struct {
	Entries    []ledger.Entry
	NextCursor *LedgerCursor
}

// Clock fornece o instante atual. Injetado para permitir teste determinístico.
type Clock interface {
	Now() time.Time
}

// IDGenerator gera identificadores únicos (UUID v7, ordenável por tempo).
type IDGenerator interface {
	NewID() (string, error)
}

// WalletRepository persiste o agregado carteira.
type WalletRepository interface {
	Insert(ctx context.Context, w wallet.Wallet) error
	Get(ctx context.Context, id string) (wallet.Wallet, error)
	// GetForUpdate lê a carteira com lock pessimista de linha. É o ponto de
	// serialização por carteira: escritores da MESMA carteira serializam aqui,
	// carteiras distintas seguem em paralelo.
	//
	// A implementação usa SELECT ... FOR NO KEY UPDATE, e não FOR UPDATE.
	// Motivo: o INSERT em wager_transactions valida a chave estrangeira da
	// carteira e, para isso, o PostgreSQL adquire FOR KEY SHARE na linha,
	// mantendo-o até o commit. FOR UPDATE conflita com FOR KEY SHARE, então
	// cada concorrente ficaria preso no upgrade KEY SHARE -> FOR UPDATE,
	// produzindo deadlock (SQLSTATE 40P01). FOR NO KEY UPDATE é compatível com
	// FOR KEY SHARE e continua exclusivo entre escritores.
	GetForUpdate(ctx context.Context, id string) (wallet.Wallet, error)
	FindByPlayerAndCurrency(ctx context.Context, playerID string, currency money.Currency) (wallet.Wallet, error)
	// UpdateBalance persiste saldo, versão e updatedAt condicionado à versão
	// esperada (proteção contra lost update, independente do lock).
	// Retorna ErrVersionConflict quando nenhuma linha foi afetada.
	UpdateBalance(ctx context.Context, w wallet.Wallet, expectedVersion int64) error
}

// TransactionRepository persiste as operações.
type TransactionRepository interface {
	Insert(ctx context.Context, t wagertransaction.Transaction) error
	// Update altera apenas campos mutáveis: status, falha, resultado,
	// referência resolvida, tentativas e agendamento. Campos de identidade
	// (origem, tipo, valor, hashes) são imutáveis.
	Update(ctx context.Context, t wagertransaction.Transaction) error
	GetByID(ctx context.Context, id string) (wagertransaction.Transaction, error)
	FindByProviderAndIdempotencyKey(ctx context.Context, providerID, idempotencyKey string) (wagertransaction.Transaction, error)
	FindByProviderAndExternalID(ctx context.Context, providerID, externalID string) (wagertransaction.Transaction, error)
	// HasSuccessfulReversal informa se a transação referenciada já recebeu uma
	// reversão bem-sucedida (REFUND ou ROLLBACK em PROCESSED).
	HasSuccessfulReversal(ctx context.Context, referenceTransactionID string) (bool, error)
	// ScheduleReferenceRetry persiste o agendamento da próxima tentativa de
	// resolução da referência, sem alterar o estado da transação.
	ScheduleReferenceRetry(ctx context.Context, transactionID string, attempts int, nextAttemptAt time.Time) error

	// ClaimDueReference reserva UMA operação PENDING_REFERENCE cujo agendamento
	// já venceu, com FOR UPDATE SKIP LOCKED.
	//
	// Diferente da reserva da outbox, NÃO há lease. A diferença é deliberada:
	// publicar é um efeito externo que não cabe na transação, então a reserva
	// precisa sobreviver a uma queda; já resolver a referência é trabalho
	// inteiramente local, que cabe em UMA tentativa = UMA transação. O lock é
	// mantido até o commit, e uma queda simplesmente reverte tudo — a operação
	// continua vencida para a próxima tentativa, sem reserva a recuperar.
	//
	// Duas instâncias nunca recebem a mesma operação: a segunda não espera nem
	// recebe a linha que a primeira já travou.
	//
	// Deve ser chamado DENTRO de uma transação; fora dela o lock não sobrevive à
	// própria consulta. found=false é o caso normal (nada vencido).
	ClaimDueReference(ctx context.Context, now time.Time) (PendingReference, bool, error)

	// CountByStatus conta as operações por estado, como estão no banco.
	//
	// Existe para as MÉTRICAS derivarem do estado persistido em vez de um contador
	// em memória: o banco é a fonte da verdade, e um contador começaria em zero a
	// cada reinício sem forma de reconciliar com o real.
	CountByStatus(ctx context.Context) (map[string]int64, error)

	// CountByFailureCode conta as operações encerradas com falha, por código.
	//
	// É onde os CONFLITOS ficam visíveis: IDEMPOTENCY_KEY_MISMATCH e
	// IDEMPOTENCY_KEY_REUSED são persistidos como failure_code, então a taxa de
	// conflito é observável sem instrumentar o caminho quente.
	CountByFailureCode(ctx context.Context) (map[string]int64, error)
}

// LedgerRepository persiste lançamentos append-only.
type LedgerRepository interface {
	Insert(ctx context.Context, e ledger.Entry) error
	// ListByWallet devolve uma página do ledger, do mais recente para o mais
	// antigo, com ordenação estável (created_at DESC, id DESC).
	// after == nil começa do topo. NextCursor é preenchido quando há mais
	// lançamentos além da página devolvida.
	ListByWallet(ctx context.Context, walletID string, after *LedgerCursor, limit int) (LedgerPage, error)
	// SumByWallet devolve a soma dos lançamentos em unidades mínimas
	// (créditos positivos, débitos negativos) e a quantidade de lançamentos.
	// É a base da reconciliação: o valor precisa bater com wallets.balance_minor.
	SumByWallet(ctx context.Context, walletID string) (minorUnits int64, entries int, err error)
}

// OutboxRecord é um evento pendente de publicação.
type OutboxRecord struct {
	ID          int64
	EventID     string
	AggregateID string
	EventType   string
	Version     int
	Payload     []byte
	OccurredAt  time.Time
	Attempts    int
}

// ClaimRequest descreve a reserva de um lote da outbox.
type ClaimRequest struct {
	// ClaimerID identifica a instância que reserva o lote (auditoria).
	ClaimerID string
	// Limit é o tamanho máximo do lote.
	Limit int
	// Now é o instante de referência (injetado para teste determinístico).
	Now time.Time
	// Lease é por quanto tempo os registros ficam reservados.
	Lease time.Duration
}

// OutboxFailure descreve uma falha de publicação.
type OutboxFailure struct {
	Attempts      int
	NextAttemptAt time.Time
	LastError     string
}

// OutboxRepository registra eventos para publicação posterior ao commit.
type OutboxRepository interface {
	// Insert é idempotente por eventId (ON CONFLICT DO NOTHING), para que uma
	// republicação preserve a identidade do evento sem duplicar registro.
	Insert(ctx context.Context, e events.Envelope) error

	// ClaimBatch reserva um lote de eventos pendentes usando FOR UPDATE SKIP
	// LOCKED, de modo que múltiplos publicadores nunca recebam o mesmo
	// registro. Registros com reserva VENCIDA (locked_until < now) voltam a ser
	// elegíveis: é assim que o trabalho abandonado por uma instância que caiu
	// entre o commit e a publicação é recuperado por outra.
	ClaimBatch(ctx context.Context, req ClaimRequest) ([]OutboxRecord, error)

	// MarkPublished confirma a publicação e libera a reserva.
	MarkPublished(ctx context.Context, ids []int64) error

	// MarkFailed reagenda o evento com backoff, contabilizando a tentativa.
	MarkFailed(ctx context.Context, id int64, failure OutboxFailure) error

	// CountPending informa quantos eventos aguardam publicação (métrica e
	// readiness de atraso da outbox).
	CountPending(ctx context.Context) (int64, error)
}

// Publisher entrega um evento ao broker.
//
// Publish deve ser seguro para repetição: republicar o mesmo evento preserva a
// identidade (eventId) e não pode duplicar o efeito no consumidor.
type Publisher interface {
	Publish(ctx context.Context, record OutboxRecord) error
	// Name identifica o destino nos logs e health checks.
	Name() string
	// Check verifica a disponibilidade do destino.
	Check(ctx context.Context) error
}

// InboxMessage é o registro durável de recebimento de uma mensagem.
type InboxMessage struct {
	ConsumerName  string
	MessageID     string
	PayloadHash   string
	ReceivedAt    time.Time
	TransactionID string
	Completed     bool
}

// InboxRepository deduplica o consumo de mensagens.
type InboxRepository interface {
	// Insert devolve inserted=false quando a mensagem já foi recebida antes
	// (mesmo consumerName + messageId).
	Insert(ctx context.Context, m InboxMessage) (inserted bool, err error)
	Get(ctx context.Context, consumerName, messageID string) (InboxMessage, error)
	Complete(ctx context.Context, consumerName, messageID, transactionID string) error
}

// Tx é o conjunto de repositórios ligados a uma única transação SQL.
type Tx interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// Store é o acesso ao banco: repositórios em autocommit e execução transacional.
type Store interface {
	Tx
	// RunInTx executa fn dentro de uma transação SQL. Um erro em fn provoca
	// rollback de tudo; o commit só acontece se fn retornar nil.
	RunInTx(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	Ping(ctx context.Context) error
	Close()
}

// ─────────────────────────────────────────────────────────────
// Entrada por broker (consumidor SQS)
// ─────────────────────────────────────────────────────────────

// InboundMessage é uma mensagem recebida do broker de entrada.
//
// ReceiptHandle é a alça de visibilidade: é ela — não o messageId — que autoriza
// confirmar a remoção (Delete) ou devolver a mensagem à fila (Release). O
// messageId é a identidade DURÁVEL, usada pela inbox.
type InboundMessage struct {
	// MessageID é a identidade durável da mensagem no broker.
	MessageID string
	// ReceiptHandle autoriza Delete e Release.
	ReceiptHandle string
	// Body é o corpo bruto recebido. É a base do hash verificado em reentregas:
	// o hash precisa ser do que o broker entregou, não de uma re-serialização.
	Body []byte
	// ReceiveCount é quantas vezes esta mensagem já foi entregue.
	ReceiveCount int
	// GroupID é o MessageGroupId (a carteira), usado em log e correlação.
	GroupID string
	// ReceivedAt é o instante do recebimento.
	ReceivedAt time.Time
}

// MessageSource lê mensagens de entrada e controla a visibilidade.
//
// O contrato separa três desfechos do consumo porque eles têm consequências
// diferentes e irreversíveis na fila:
//
//   - Delete confirma o tratamento durável (a mensagem não volta);
//   - Release devolve a mensagem para reentrega com atraso (falha transitória);
//   - DeadLetter move a mensagem para a DLQ (falha permanente).
type MessageSource interface {
	Name() string
	Check(ctx context.Context) error

	// Receive busca até max mensagens, aguardando wait quando não houver
	// nenhuma. Lote vazio sem erro significa "nada disponível".
	Receive(ctx context.Context, max int, wait time.Duration) ([]InboundMessage, error)

	// Delete remove a mensagem da fila após o commit do tratamento.
	Delete(ctx context.Context, receiptHandle string) error

	// Release torna a mensagem visível de novo após delay (backoff de retry).
	Release(ctx context.Context, receiptHandle string, delay time.Duration) error

	// DeadLetter encaminha a mensagem para a DLQ e remove a original.
	DeadLetter(ctx context.Context, msg InboundMessage, reason string) error
}

// ─────────────────────────────────────────────────────────────
// Referências pendentes (retomada durável de PENDING_REFERENCE)
// ─────────────────────────────────────────────────────────────

// PendingReference é uma operação que aguarda a resolução da referência externa
// para poder ser aplicada.
//
// Carrega o mínimo para uma tentativa: identidade, o que resolver e o que já se
// tentou. O estado completo da transação é lido pelo repositório de transações
// quando a tentativa realmente acontece.
type PendingReference struct {
	TransactionID string
	ProviderID    string
	WalletID      string
	Kind          wagertransaction.Kind
	// ReferenceExternalID é a referência EXTERNA que precisa ser encontrada
	// (o externalTransactionId da operação referenciada, no mesmo provedor).
	ReferenceExternalID string
	// Attempts é quantas tentativas de resolução já houve.
	Attempts int
	// Age é o tempo desde a criação da operação: base do TTL da política. Um
	// TTL baseado na idade, e não só no número de tentativas, é o que impede que
	// uma operação fique pendente indefinidamente quando o backoff chega ao teto.
	Age time.Duration
	// NextAttemptAt é o agendamento que venceu.
	NextAttemptAt time.Time
}
