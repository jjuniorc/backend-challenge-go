//go:build integration

package broker_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jjuniorc/backend-challenge-go/internal/broker"
	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// Estes testes exercitam o MessageSource contra o broker emulado REAL (sem
// dublê): substituir SQS por mock nos testes é critério eliminatório.
//
// Cada teste provisiona filas FIFO com nome único. O MiniStack persiste estado
// entre execuções, então um MessageDeduplicationId fixo poderia ser deduplicado
// por uma execução anterior, dentro da janela de 5 minutos do SQS.

// sourceTestConfig monta a configuração com as URLs já resolvidas, para que o
// construtor não tente descobri-las por nome.
func sourceTestConfig(queueURL, dlqURL string) config.SQSConfig {
	return config.SQSConfig{
		Region:          "us-east-1",
		Endpoint:        sqsEndpoint(),
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		QueueName:       "fila-nao-resolvida.fifo",
		DLQName:         "dlq-nao-resolvida.fifo",
		QueueURL:        queueURL,
		DLQURL:          dlqURL,
	}
}

// createTestQueueWithDLQ provisiona uma fila FIFO e sua DLQ, com redrive.
//
// visibilitySeconds é o prazo de invisibilidade após o recebimento, e governa a
// reentrega nos testes de falha.
func createTestQueueWithDLQ(t *testing.T, client *sqs.Client, visibilitySeconds int) (queueURL, dlqURL string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()

	dlqName := fmt.Sprintf("wager-src-test-dlq-%d.fifo", suffix)
	dlq, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(dlqName),
		Attributes: map[string]string{"FifoQueue": "true"},
	})
	if err != nil {
		t.Fatalf("criando a DLQ %q: %v", dlqName, err)
	}
	dlqURL = aws.ToString(dlq.QueueUrl)

	dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("obtendo o ARN da DLQ: %v", err)
	}

	queueName := fmt.Sprintf("wager-src-test-%d.fifo", suffix)
	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue":                     "true",
			"VisibilityTimeout":             fmt.Sprintf("%d", visibilitySeconds),
			"ReceiveMessageWaitTimeSeconds": "0",
			"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"5"}`,
				dlqAttrs.Attributes[string(types.QueueAttributeNameQueueArn)]),
		},
	})
	if err != nil {
		t.Fatalf("criando a fila %q: %v", queueName, err)
	}
	queueURL = aws.ToString(created.QueueUrl)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, url := range []string{queueURL, dlqURL} {
			if _, err := client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(url)}); err != nil {
				t.Logf("removendo fila de teste %s: %v", url, err)
			}
		}
	})
	return queueURL, dlqURL
}

// sendTestMessage publica um corpo cru: o formato é o de ENTRADA do consumidor,
// e não o envelope de saída da outbox.
func sendTestMessage(t *testing.T, client *sqs.Client, queueURL, body, groupID, dedupID string) {
	t.Helper()
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(dedupID),
	}); err != nil {
		t.Fatalf("enviando mensagem: %v", err)
	}
}

func uniqueID(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// waitForCond repete a verificação até passar ou o prazo esgotar: um broker
// emulado tem latência variável, e sleep fixo produz teste instável.
func waitForCond(t *testing.T, timeout time.Duration, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("prazo esgotado esperando: %s", what)
}

// ── recebimento e remoção ────────────────────────────────────────────

func TestSQSMessageSourceReceivesAndDeletes(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, sourceTestConfig("", ""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL, dlqURL := createTestQueueWithDLQ(t, client, 30)

	source, err := broker.NewSQSMessageSource(ctx, sourceTestConfig(queueURL, dlqURL), 30*time.Second, discardLogger())
	if err != nil {
		t.Fatalf("NewSQSMessageSource: %v", err)
	}
	if err := source.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	}

	const (
		groupID = "0192f291-27dd-7d3f-8071-5f8685deef37"
		body    = `{"messageId":"m-1","type":"WagerTransactionRequested"}`
	)
	sendTestMessage(t, client, queueURL, body, groupID, uniqueID(t, "dedup"))

	messages, err := source.Receive(ctx, 1, time.Second)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("mensagens recebidas = %d, quer 1", len(messages))
	}

	msg := messages[0]
	if msg.MessageID == "" {
		t.Fatal("MessageID vazio: é a identidade durável da mensagem")
	}
	if msg.ReceiptHandle == "" {
		t.Fatal("ReceiptHandle vazio: sem ele não há Delete nem Release")
	}
	if string(msg.Body) != body {
		t.Fatalf("corpo = %q, quer %q", string(msg.Body), body)
	}
	if msg.GroupID != groupID {
		t.Fatalf("GroupID = %q, quer %q (MessageGroupId é a carteira)", msg.GroupID, groupID)
	}
	if msg.ReceiveCount != 1 {
		t.Fatalf("ReceiveCount = %d, quer 1 (primeira entrega)", msg.ReceiveCount)
	}
	if msg.ReceivedAt.IsZero() {
		t.Fatal("ReceivedAt zerado")
	}

	if err := source.Delete(ctx, msg.ReceiptHandle); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if count := queueMessageCount(t, client, queueURL); count != 0 {
		t.Fatalf("mensagens na fila após o Delete = %d, quer 0", count)
	}
}

func TestSQSMessageSourceReceiveOnEmptyQueueReturnsNothing(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, sourceTestConfig("", ""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL, dlqURL := createTestQueueWithDLQ(t, client, 30)

	source, err := broker.NewSQSMessageSource(ctx, sourceTestConfig(queueURL, dlqURL), 30*time.Second, discardLogger())
	if err != nil {
		t.Fatalf("NewSQSMessageSource: %v", err)
	}

	messages, err := source.Receive(ctx, 10, 0)
	if err != nil {
		t.Fatalf("Receive em fila vazia: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("mensagens = %d, quer 0", len(messages))
	}
}

// ── reentrega ────────────────────────────────────────────────────────

func TestSQSMessageSourceReleaseMakesMessageVisibleAgain(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, sourceTestConfig("", ""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL, dlqURL := createTestQueueWithDLQ(t, client, 30)

	source, err := broker.NewSQSMessageSource(ctx, sourceTestConfig(queueURL, dlqURL), 30*time.Second, discardLogger())
	if err != nil {
		t.Fatalf("NewSQSMessageSource: %v", err)
	}

	sendTestMessage(t, client, queueURL, `{"messageId":"m-release"}`, "grupo-release", uniqueID(t, "dedup"))

	first, err := source.Receive(ctx, 1, time.Second)
	if err != nil {
		t.Fatalf("primeiro Receive: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("primeiro lote = %d, quer 1", len(first))
	}

	// Release sem atraso: a mensagem volta a ficar visível imediatamente.
	if err := source.Release(ctx, first[0].ReceiptHandle, 0); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second, err := source.Receive(ctx, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("segundo Receive: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("segundo lote = %d, quer 1 (Release deveria torná-la visível)", len(second))
	}
	if second[0].ReceiveCount != 2 {
		t.Fatalf("ReceiveCount = %d, quer 2 (é a base do backoff)", second[0].ReceiveCount)
	}
}

func TestSQSMessageSourceReleaseWithDelayHidesTheMessage(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, sourceTestConfig("", ""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL, dlqURL := createTestQueueWithDLQ(t, client, 30)

	source, err := broker.NewSQSMessageSource(ctx, sourceTestConfig(queueURL, dlqURL), 30*time.Second, discardLogger())
	if err != nil {
		t.Fatalf("NewSQSMessageSource: %v", err)
	}

	sendTestMessage(t, client, queueURL, `{"messageId":"m-delay"}`, "grupo-delay", uniqueID(t, "dedup"))

	first, err := source.Receive(ctx, 1, time.Second)
	if err != nil {
		t.Fatalf("primeiro Receive: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("primeiro lote = %d, quer 1", len(first))
	}
	if err := source.Release(ctx, first[0].ReceiptHandle, 3*time.Second); err != nil {
		t.Fatalf("Release com atraso: %v", err)
	}

	// Logo após o Release com atraso, a mensagem NÃO deve reaparecer: é o que
	// faz o backoff significar algo.
	immediate, err := source.Receive(ctx, 1, 0)
	if err != nil {
		t.Fatalf("Receive imediato: %v", err)
	}
	if len(immediate) != 0 {
		t.Fatalf("mensagens imediatas = %d, quer 0 (o atraso deveria escondê-la)", len(immediate))
	}

	var redelivered []ports.InboundMessage
	waitForCond(t, 20*time.Second, "a reentrega após o atraso do Release", func() bool {
		received, err := source.Receive(ctx, 1, time.Second)
		if err != nil {
			return false
		}
		if len(received) == 1 {
			redelivered = received
			return true
		}
		return false
	})
	if redelivered[0].ReceiveCount != 2 {
		t.Fatalf("ReceiveCount = %d, quer 2", redelivered[0].ReceiveCount)
	}
}

// ── DLQ ──────────────────────────────────────────────────────────────

func TestSQSMessageSourceDeadLettersTheMessage(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, sourceTestConfig("", ""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL, dlqURL := createTestQueueWithDLQ(t, client, 30)

	source, err := broker.NewSQSMessageSource(ctx, sourceTestConfig(queueURL, dlqURL), 30*time.Second, discardLogger())
	if err != nil {
		t.Fatalf("NewSQSMessageSource: %v", err)
	}

	const (
		groupID = "0192f291-27dd-7d3f-8071-5f8685deef38"
		body    = `{"messageId":"m-invalida","type":"OutroEvento"}`
	)
	sendTestMessage(t, client, queueURL, body, groupID, uniqueID(t, "dedup"))

	received, err := source.Receive(ctx, 1, time.Second)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("mensagens recebidas = %d, quer 1", len(received))
	}

	if err := source.DeadLetter(ctx, received[0], "mensagem inválida: tipo desconhecido"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	// A original sai da fila principal e a cópia aparece na DLQ.
	waitForCond(t, 10*time.Second, "a fila principal ficar vazia", func() bool {
		return queueMessageCount(t, client, queueURL) == 0
	})
	waitForCond(t, 10*time.Second, "a DLQ receber a mensagem", func() bool {
		return queueMessageCount(t, client, dlqURL) == 1
	})

	// O corpo é preservado e a razão acompanha, para auditoria.
	output, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		MaxNumberOfMessages:   1,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage na DLQ: %v", err)
	}
	if len(output.Messages) != 1 {
		t.Fatalf("mensagens na DLQ = %d, quer 1", len(output.Messages))
	}
	dead := output.Messages[0]

	if got := aws.ToString(dead.Body); got != body {
		t.Fatalf("corpo na DLQ = %q, quer %q", got, body)
	}
	if got := aws.ToString(dead.MessageAttributes[broker.AttrDeadLetterReason].StringValue); got == "" {
		t.Fatal("a razão do encaminhamento deveria acompanhar a mensagem")
	}
	if got := aws.ToString(dead.MessageAttributes[broker.AttrOriginalMessageID].StringValue); got != received[0].MessageID {
		t.Fatalf("originalMessageId = %q, quer %q", got, received[0].MessageID)
	}
}
