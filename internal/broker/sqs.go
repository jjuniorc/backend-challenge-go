// Package broker publica eventos no AWS SQS (executado localmente com MiniStack
// ou LocalStack).
package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// SQSPublisher publica eventos numa fila SQS FIFO.
//
// Escolhas de roteamento, documentadas para o ARCHITECTURE.md:
//
//   - MessageGroupId = aggregateId (o walletId). O SQS FIFO garante ordem
//     DENTRO do grupo e paralelismo ENTRE grupos, o que espelha exatamente a
//     coordenação do domínio: operações da mesma carteira são ordenadas,
//     carteiras distintas avançam em paralelo.
//   - MessageDeduplicationId = eventId. Como o eventId é estável e definido na
//     transação de negócio, uma republicação pelo outbox não gera mensagem
//     duplicada — o broker deduplica pela identidade do EVENTO, não pelo
//     conteúdo.
type SQSPublisher struct {
	client   *sqs.Client
	queueURL string
	logger   *slog.Logger
}

// NewSQSClient monta o cliente SQS a partir da configuração.
//
// Exportado para que os testes possam provisionar filas dedicadas sem duplicar
// a construção do cliente (endpoint, região e credenciais).
func NewSQSClient(ctx context.Context, cfg config.SQSConfig) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("broker: carregando configuração da AWS: %w", err)
	}

	return sqs.NewFromConfig(awsCfg, func(options *sqs.Options) {
		if endpoint := strings.TrimSpace(cfg.Endpoint); endpoint != "" {
			// Override de endpoint: aponta o SDK para o emulador local.
			options.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

// NewSQSPublisher monta o cliente e resolve a URL da fila.
//
// A URL é resolvida por GetQueueUrl quando não informada na configuração, para
// não depender do accountId do emulador local.
func NewSQSPublisher(ctx context.Context, cfg config.SQSConfig, logger *slog.Logger) (*SQSPublisher, error) {
	client, err := NewSQSClient(ctx, cfg)
	if err != nil {
		return nil, err
	}

	publisher := &SQSPublisher{client: client, queueURL: strings.TrimSpace(cfg.QueueURL), logger: logger}
	if publisher.queueURL == "" {
		queueURL, err := publisher.resolveQueueURL(ctx, cfg.QueueName)
		if err != nil {
			return nil, err
		}
		publisher.queueURL = queueURL
	}
	logger.Info("publisher SQS configurado", "queueUrl", publisher.queueURL, "region", cfg.Region)
	return publisher, nil
}

// Name identifica o destino nos logs e health checks.
func (p *SQSPublisher) Name() string { return "sqs" }

// QueueURL devolve a URL resolvida (usada por testes e diagnóstico).
func (p *SQSPublisher) QueueURL() string { return p.queueURL }

// Check verifica a disponibilidade da fila (readiness).
func (p *SQSPublisher) Check(ctx context.Context) error {
	_, err := p.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(p.queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		return fmt.Errorf("broker: fila indisponível: %w", err)
	}
	return nil
}

// Publish envia o evento para a fila FIFO.
func (p *SQSPublisher) Publish(ctx context.Context, record ports.OutboxRecord) error {
	input := &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(record.Payload)),
		MessageGroupId:         aws.String(record.AggregateID),
		MessageDeduplicationId: aws.String(record.EventID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(record.EventType),
			},
			"eventVersion": {
				DataType:    aws.String("Number"),
				StringValue: aws.String(fmt.Sprintf("%d", record.Version)),
			},
		},
	}

	output, err := p.client.SendMessage(ctx, input)
	if err != nil {
		return fmt.Errorf("broker: publicando evento %s: %w", record.EventID, err)
	}
	p.logger.Info("evento publicado",
		"eventId", record.EventID,
		"eventType", record.EventType,
		"walletId", record.AggregateID,
		"messageId", aws.ToString(output.MessageId),
	)
	return nil
}

func (p *SQSPublisher) resolveQueueURL(ctx context.Context, queueName string) (string, error) {
	output, err := p.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueName)})
	if err != nil {
		return "", fmt.Errorf("broker: resolvendo a URL da fila %q: %w", queueName, err)
	}
	if aws.ToString(output.QueueUrl) == "" {
		return "", fmt.Errorf("broker: fila %q não encontrada", queueName)
	}
	return aws.ToString(output.QueueUrl), nil
}

var _ ports.Publisher = (*SQSPublisher)(nil)
