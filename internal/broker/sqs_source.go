package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// Atributos de mensagem gravados pela aplicação ao encaminhar para a DLQ.
const (
	// AttrDeadLetterReason explica por que a mensagem foi para a DLQ.
	AttrDeadLetterReason = "deadLetterReason"
	// AttrOriginalMessageID preserva a identidade da mensagem original.
	AttrOriginalMessageID = "originalMessageId"
	// AttrOriginalReceiveCount registra quantas entregas houve antes da DLQ.
	AttrOriginalReceiveCount = "originalReceiveCount"
)

// SQSMessageSource consome uma fila SQS FIFO e controla a visibilidade das
// mensagens.
//
// A DLQ é resolvida no startup, junto com a fila principal: o encaminhamento de
// uma mensagem inválida não pode depender de descoberta de URL em tempo de
// falha, que é justamente o momento em que a rede está sob suspeita.
type SQSMessageSource struct {
	client   *sqs.Client
	queueURL string
	dlqURL   string
	// visibilityTimeout vai em CADA recebimento, e não apenas no atributo da
	// fila: o prazo fica sob controle da aplicação, explícito na configuração e
	// verificável, em vez de depender de um valor provisionado em outro lugar
	// que poderia divergir em silêncio.
	visibilityTimeout time.Duration
	logger            *slog.Logger
}

// NewSQSMessageSource monta o cliente e resolve as URLs das filas.
//
// visibilityTimeout é por quanto tempo a mensagem fica invisível após o
// recebimento. Zero deixa valer o atributo da fila.
func NewSQSMessageSource(ctx context.Context, cfg config.SQSConfig, visibilityTimeout time.Duration, logger *slog.Logger) (*SQSMessageSource, error) {
	client, err := NewSQSClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	source := &SQSMessageSource{
		client:            client,
		queueURL:          strings.TrimSpace(cfg.QueueURL),
		dlqURL:            strings.TrimSpace(cfg.DLQURL),
		visibilityTimeout: visibilityTimeout,
		logger:            logger,
	}
	if source.queueURL == "" {
		url, err := lookupQueueURL(ctx, client, cfg.QueueName)
		if err != nil {
			return nil, err
		}
		source.queueURL = url
	}
	if source.dlqURL == "" {
		url, err := lookupQueueURL(ctx, client, cfg.DLQName)
		if err != nil {
			return nil, err
		}
		source.dlqURL = url
	}

	logger.Info("consumidor SQS configurado", "queueUrl", source.queueURL, "dlqUrl", source.dlqURL)
	return source, nil
}

// Name identifica o destino nos logs e health checks.
func (s *SQSMessageSource) Name() string { return "sqs" }

// QueueURL devolve a URL resolvida (diagnóstico e testes).
func (s *SQSMessageSource) QueueURL() string { return s.queueURL }

// DLQURL devolve a URL da DLQ resolvida.
func (s *SQSMessageSource) DLQURL() string { return s.dlqURL }

// Check verifica a disponibilidade da fila de entrada (readiness).
func (s *SQSMessageSource) Check(ctx context.Context) error {
	_, err := s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(s.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("broker: fila de entrada indisponível: %w", err)
	}
	return nil
}

// Receive busca até max mensagens, aguardando wait quando não houver nenhuma.
//
// Os atributos de SISTEMA são pedidos explicitamente (e não via "All") porque
// cada um tem um uso definido aqui:
//
//   - MessageGroupId         -> carteira, para log e correlação;
//   - ApproximateReceiveCount -> base do backoff e do limite de tentativas;
//   - MessageDeduplicationId  -> auditoria da deduplicação por identidade.
func (s *SQSMessageSource) Receive(ctx context.Context, max int, wait time.Duration) ([]ports.InboundMessage, error) {
	if max < 1 {
		max = 1
	}
	if max > 10 {
		max = 10 // limite do SQS por chamada
	}
	waitSeconds := int32(wait / time.Second)
	if waitSeconds > 20 {
		waitSeconds = 20 // limite do SQS para long polling
	}

	input := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(s.queueURL),
		MaxNumberOfMessages: int32(max),
		WaitTimeSeconds:     waitSeconds,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameMessageGroupId,
			types.MessageSystemAttributeNameMessageDeduplicationId,
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
		MessageAttributeNames: []string{"All"},
	}
	// O prazo vai no recebimento, não só no atributo da fila: assim o tempo de
	// processamento é uma decisão da aplicação, e não uma consequência do
	// provisionamento feito em outro arquivo.
	if seconds := int32(s.visibilityTimeout / time.Second); seconds > 0 {
		input.VisibilityTimeout = seconds
	}

	output, err := s.client.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("broker: recebendo mensagens: %w", err)
	}

	receivedAt := time.Now().UTC()
	messages := make([]ports.InboundMessage, 0, len(output.Messages))
	for _, message := range output.Messages {
		messages = append(messages, ports.InboundMessage{
			MessageID:     aws.ToString(message.MessageId),
			ReceiptHandle: aws.ToString(message.ReceiptHandle),
			Body:          []byte(aws.ToString(message.Body)),
			ReceiveCount:  receiveCount(message),
			GroupID:       message.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
			ReceivedAt:    receivedAt,
		})
	}
	return messages, nil
}

// Delete confirma o tratamento durável, removendo a mensagem da fila.
//
// É chamado APENAS depois do commit: a ordem inversa perderia a movimentação.
func (s *SQSMessageSource) Delete(ctx context.Context, receiptHandle string) error {
	_, err := s.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("broker: removendo a mensagem da fila: %w", err)
	}
	return nil
}

// Release devolve a mensagem para reentrega após delay.
//
// Usa ChangeMessageVisibility em vez de esperar o visibility timeout: o prazo de
// retry fica independente do prazo de processamento configurado na fila, e o
// backoff passa a ser uma decisão do consumidor — auditável no log.
func (s *SQSMessageSource) Release(ctx context.Context, receiptHandle string, delay time.Duration) error {
	seconds := int32(delay / time.Second)
	if seconds < 0 {
		seconds = 0
	}

	_, err := s.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(s.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: seconds,
	})
	if err != nil {
		return fmt.Errorf("broker: devolvendo a mensagem para reentrega: %w", err)
	}
	return nil
}

// DeadLetter copia a mensagem para a DLQ e só então remove a original.
//
// A ordem importa: se a cópia falhar, a mensagem permanece na fila principal e
// será encaminhada de novo (a DLQ FIFO deduplica pelo MessageDeduplicationId,
// que é o messageId original, evitando duplicata). O inverso — remover antes de
// copiar — perderia a mensagem.
func (s *SQSMessageSource) DeadLetter(ctx context.Context, msg ports.InboundMessage, reason string) error {
	groupID := strings.TrimSpace(msg.GroupID)
	if groupID == "" {
		// A DLQ é FIFO e exige MessageGroupId. Sem o grupo original, usa o
		// próprio messageId: preserva o requisito sem inventar agrupamento.
		groupID = msg.MessageID
	}

	_, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(s.dlqURL),
		MessageBody:            aws.String(string(msg.Body)),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(msg.MessageID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			AttrDeadLetterReason: {
				DataType:    aws.String("String"),
				StringValue: aws.String(reason),
			},
			AttrOriginalMessageID: {
				DataType:    aws.String("String"),
				StringValue: aws.String(msg.MessageID),
			},
			AttrOriginalReceiveCount: {
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.Itoa(msg.ReceiveCount)),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("broker: encaminhando a mensagem %s para a DLQ: %w", msg.MessageID, err)
	}

	if err := s.Delete(ctx, msg.ReceiptHandle); err != nil {
		return err
	}

	s.logger.Warn("mensagem encaminhada para a DLQ",
		"messageId", msg.MessageID, "receiveCount", msg.ReceiveCount, "reason", reason)
	return nil
}

func receiveCount(message types.Message) int {
	raw := message.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		return 1
	}
	return count
}

// lookupQueueURL resolve a URL de uma fila pelo nome.
func lookupQueueURL(ctx context.Context, client *sqs.Client, queueName string) (string, error) {
	output, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		return "", fmt.Errorf("broker: resolvendo a URL da fila %q: %w", queueName, err)
	}
	if aws.ToString(output.QueueUrl) == "" {
		return "", fmt.Errorf("broker: fila %q não encontrada", queueName)
	}
	return aws.ToString(output.QueueUrl), nil
}

var _ ports.MessageSource = (*SQSMessageSource)(nil)
