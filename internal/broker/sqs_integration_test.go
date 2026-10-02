//go:build integration

package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jjuniorc/backend-challenge-go/internal/broker"
	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/ledger"
	"github.com/jjuniorc/backend-challenge-go/internal/domain/money"
	"github.com/jjuniorc/backend-challenge-go/internal/events"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// Endpoint do broker emulado (MiniStack/LocalStack), com a mesma convenção de
// obrigatoriedade usada nos testes de IdP.
func sqsEndpoint() string {
	if value := strings.TrimSpace(os.Getenv("TEST_SQS_ENDPOINT")); value != "" {
		return value
	}
	return "http://localhost:4566"
}

func requireBroker(t *testing.T) {
	t.Helper()

	required := strings.TrimSpace(os.Getenv("TEST_SQS_REQUIRED")) == "1"
	timeout := 5 * time.Second
	if required {
		timeout = 45 * time.Second
	}

	endpoint := sqsEndpoint()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 3 * time.Second}
	var lastErr error

	for time.Now().Before(deadline) {
		resp, err := client.Get(endpoint + "/_ministack/health")
		if err != nil {
			// LocalStack usa outro caminho de health; tenta o alternativo.
			resp, err = client.Get(endpoint + "/_localstack/health")
		}
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			lastErr = fmt.Errorf("health retornou HTTP %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(time.Second)
	}

	message := fmt.Sprintf("broker emulado indisponível em %s (%v). Suba com: docker compose up -d ministack", endpoint, lastErr)
	if required {
		t.Fatal(message)
	}
	t.Skip(message)
}

func testConfig(queueURL string) config.SQSConfig {
	return config.SQSConfig{
		Region:          "us-east-1",
		Endpoint:        sqsEndpoint(),
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		QueueName:       "wager-transactions-test.fifo",
		QueueURL:        queueURL,
	}
}

// createTestQueue provisiona uma fila FIFO DEDICADA ao teste, evitando
// interferência com a fila do desafio e entre testes.
func createTestQueue(t *testing.T, client *sqs.Client, fifo bool) string {
	t.Helper()
	ctx := context.Background()

	name := fmt.Sprintf("wager-tx-test-%d", time.Now().UnixNano())
	attributes := map[string]string{}
	if fifo {
		name += ".fifo"
		attributes["FifoQueue"] = "true"
	}

	output, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attributes,
	})
	if err != nil {
		t.Fatalf("criando fila de teste %q: %v", name, err)
	}
	queueURL := aws.ToString(output.QueueUrl)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
			t.Logf("removendo fila de teste %s: %v", queueURL, err)
		}
	})
	return queueURL
}

func queueMessageCount(t *testing.T, client *sqs.Client, queueURL string) int {
	t.Helper()
	output, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		t.Fatalf("consultando atributos da fila: %v", err)
	}
	raw := output.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	var count int
	if _, err := fmt.Sscanf(raw, "%d", &count); err != nil {
		t.Fatalf("interpretando contagem de mensagens %q: %v", raw, err)
	}
	return count
}

func walletBalanceChangedRecord(t *testing.T, walletID, eventID, transactionID string, version int64) ports.OutboxRecord {
	t.Helper()

	brl := func(value string) money.Money {
		m, err := money.ParseDecimal(value, "BRL")
		if err != nil {
			t.Fatalf("money(%q): %v", value, err)
		}
		return m
	}
	env, err := events.NewWalletBalanceChanged(events.Meta{
		EventID:       eventID,
		CorrelationID: "corr-sqs-test",
		OccurredAt:    time.Now().UTC(),
	}, events.BalanceChangedParams{
		WalletID:      walletID,
		TransactionID: transactionID,
		Direction:     ledger.DirectionDebit,
		Money:         brl("25.00"),
		BalanceBefore: brl("100.00"),
		BalanceAfter:  brl("75.00"),
		WalletVersion: version,
	})
	if err != nil {
		t.Fatalf("montando evento: %v", err)
	}
	payload, err := env.Payload()
	if err != nil {
		t.Fatalf("serializando evento: %v", err)
	}
	return ports.OutboxRecord{
		EventID:     env.EventID,
		AggregateID: env.AggregateID,
		EventType:   string(env.EventType),
		Version:     env.Version,
		Payload:     payload,
		OccurredAt:  env.OccurredAt,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// ── publicação real ──────────────────────────────────────────────────

func TestSQSPublisherPublishesWithGroupAndDeduplicationIDs(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, testConfig(""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL := createTestQueue(t, client, true)

	publisher, err := broker.NewSQSPublisher(ctx, testConfig(queueURL), discardLogger())
	if err != nil {
		t.Fatalf("NewSQSPublisher: %v", err)
	}
	if err := publisher.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	}

	const (
		walletID      = "00000000-0000-7000-8000-0000000000aa"
		eventID       = "00000000-0000-7000-8000-0000000000e1"
		transactionID = "00000000-0000-7000-8000-0000000000b1"
	)
	record := walletBalanceChangedRecord(t, walletID, eventID, transactionID, 2)

	if err := publisher.Publish(ctx, record); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if count := queueMessageCount(t, client, queueURL); count != 1 {
		t.Fatalf("mensagens na fila = %d, quer 1", count)
	}

	received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   1,
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(received.Messages) != 1 {
		t.Fatalf("mensagens recebidas = %d, quer 1", len(received.Messages))
	}
	message := received.Messages[0]

	// MessageGroupId = walletId: ordem por carteira e paralelismo entre carteiras.
	if got := message.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; got != walletID {
		t.Fatalf("MessageGroupId = %q, quer o walletId %q", got, walletID)
	}
	// MessageDeduplicationId = eventId: republicação não duplica.
	if got := message.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)]; got != eventID {
		t.Fatalf("MessageDeduplicationId = %q, quer o eventId %q", got, eventID)
	}

	// O corpo é o envelope do evento. Compara-se por DECODIFICAÇÃO, não por
	// bytes: o payload passa por jsonb no Postgres, que normaliza a ordem das
	// chaves. A semântica é preservada; a ordem não é contrato.
	var envelope struct {
		EventID     string          `json:"eventId"`
		EventType   string          `json:"eventType"`
		AggregateID string          `json:"aggregateId"`
		Version     int             `json:"version"`
		Data        json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &envelope); err != nil {
		t.Fatalf("decodificando o corpo: %v", err)
	}
	if envelope.EventID != eventID {
		t.Fatalf("eventId no corpo = %q, quer %q", envelope.EventID, eventID)
	}
	if envelope.EventType != "WalletBalanceChanged" {
		t.Fatalf("eventType = %q", envelope.EventType)
	}
	if envelope.AggregateID != walletID {
		t.Fatalf("aggregateId = %q, quer %q", envelope.AggregateID, walletID)
	}
	if envelope.Version != 1 {
		t.Fatalf("version = %d, quer 1", envelope.Version)
	}

	var data struct {
		WalletID      string `json:"walletId"`
		TransactionID string `json:"transactionId"`
		WalletVersion int64  `json:"walletVersion"`
	}
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatalf("decodificando data: %v", err)
	}
	if data.WalletID != walletID || data.TransactionID != transactionID || data.WalletVersion != 2 {
		t.Fatalf("data = %+v", data)
	}

	// Atributos de mensagem facilitam o roteamento/filtro do consumidor.
	if got := aws.ToString(message.MessageAttributes["eventType"].StringValue); got != "WalletBalanceChanged" {
		t.Fatalf("atributo eventType = %q", got)
	}
}

// TestSQSPublisherDeduplicatesRepublish prova que republicar o MESMO evento não
// cria mensagem duplicada: o SQS FIFO deduplica pelo MessageDeduplicationId,
// que é o eventId estável — exatamente a garantia exigida para a recuperação
// após interrupção entre commit e publicação.
func TestSQSPublisherDeduplicatesRepublish(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, testConfig(""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL := createTestQueue(t, client, true)

	publisher, err := broker.NewSQSPublisher(ctx, testConfig(queueURL), discardLogger())
	if err != nil {
		t.Fatalf("NewSQSPublisher: %v", err)
	}

	record := walletBalanceChangedRecord(t,
		"00000000-0000-7000-8000-0000000000ab",
		"00000000-0000-7000-8000-0000000000e2",
		"00000000-0000-7000-8000-0000000000b2",
		3,
	)

	if err := publisher.Publish(ctx, record); err != nil {
		t.Fatalf("primeira publicação: %v", err)
	}
	if err := publisher.Publish(ctx, record); err != nil {
		t.Fatalf("republicação: %v", err)
	}
	if err := publisher.Publish(ctx, record); err != nil {
		t.Fatalf("terceira publicação: %v", err)
	}

	if count := queueMessageCount(t, client, queueURL); count != 1 {
		t.Fatalf("mensagens na fila = %d, quer 1 (deduplicação por eventId)", count)
	}
}

// TestSQSPublisherPreservesOrderWithinWallet prova a garantia de ordem DENTRO
// do grupo (a mesma carteira), que é o que o MessageGroupId oferece.
func TestSQSPublisherPreservesOrderWithinWallet(t *testing.T) {
	requireBroker(t)
	ctx := context.Background()

	client, err := broker.NewSQSClient(ctx, testConfig(""))
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	queueURL := createTestQueue(t, client, true)

	publisher, err := broker.NewSQSPublisher(ctx, testConfig(queueURL), discardLogger())
	if err != nil {
		t.Fatalf("NewSQSPublisher: %v", err)
	}

	const walletID = "00000000-0000-7000-8000-0000000000ac"
	const total = 5
	for i := 1; i <= total; i++ {
		record := walletBalanceChangedRecord(t,
			walletID,
			fmt.Sprintf("00000000-0000-7000-8000-%012d", 900+i),
			fmt.Sprintf("00000000-0000-7000-8000-0000000000c%d", i),
			int64(i),
		)
		if err := publisher.Publish(ctx, record); err != nil {
			t.Fatalf("publicando %d: %v", i, err)
		}
	}

	received, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: total,
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(received.Messages) != total {
		t.Fatalf("mensagens recebidas = %d, quer %d", len(received.Messages), total)
	}

	for i, message := range received.Messages {
		wantVersion := i + 1
		var envelope struct {
			Data struct {
				WalletVersion int64 `json:"walletVersion"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &envelope); err != nil {
			t.Fatalf("decodificando mensagem %d: %v", i, err)
		}
		if envelope.Data.WalletVersion != int64(wantVersion) {
			t.Fatalf("mensagem %d tem walletVersion=%d, quer %d (ordem violada no grupo)",
				i, envelope.Data.WalletVersion, wantVersion)
		}
		if got := message.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]; got != walletID {
			t.Fatalf("mensagem %d: MessageGroupId = %q, quer %q", i, got, walletID)
		}
	}
}
