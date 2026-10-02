package worker

import (
	"context"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
)

// TransactionProcessor é a implementação de InboundProcessor sobre o MESMO caso
// de uso usado pela camada HTTP.
//
// Não há regra de negócio aqui: o trabalho é ligar a identidade da MENSAGEM
// (inbox) ao comando de negócio já traduzido pelo parser. É essa ligação que
// atende ao README §6.5 — o registro da inbox e a conclusão durável do
// tratamento compartilham a transação SQL das alterações de domínio.
type TransactionProcessor struct {
	process      *usecase.ProcessTransaction
	consumerName string
}

// NewTransactionProcessor cria o processador de mensagens de entrada.
func NewTransactionProcessor(process *usecase.ProcessTransaction, consumerName string) *TransactionProcessor {
	return &TransactionProcessor{process: process, consumerName: consumerName}
}

// ProcessInbound executa a operação com o registro de inbox na mesma transação.
//
// Rejeição de negócio NÃO é erro aqui: o caso de uso persiste a transação como
// REJECTED com seu evento e devolve resultado sem erro, o que é terminal para a
// fila (a mensagem pode ser removida). Erros devolvidos são classificados pelo
// consumidor entre transitórios (reentrega) e permanentes (DLQ).
func (p *TransactionProcessor) ProcessInbound(ctx context.Context, msg ports.InboundMessage, in InboundTransaction) error {
	_, err := p.process.Execute(ctx, in.Command, usecase.WithInbox(usecase.InboxClaim{
		ConsumerName: p.consumerName,
		MessageID:    in.MessageID,
		PayloadHash:  in.PayloadHash,
		ReceivedAt:   msg.ReceivedAt,
	}))
	return err
}

var _ InboundProcessor = (*TransactionProcessor)(nil)
