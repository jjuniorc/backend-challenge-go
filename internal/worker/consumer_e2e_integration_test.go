//go:build integration

package worker

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/jjuniorc/backend-challenge-go/internal/broker"
	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// Estes testes fecham o laço COMPLETO contra o broker emulado real:
//
//	mensagem na fila -> fonte SQS -> consumidor -> caso de uso com inbox ->
//	commit -> remoção da mensagem
//
// A verificação é sempre sobre o resultado DURÁVEL (saldo, ledger, inbox, fila),
// nunca sobre log: é o que o critério de avaliação considera.

func e2eEndpoint() string {
	if value := strings.TrimSpace(os.Getenv("TEST_SQS_ENDPOINT")); value != "" {
		return value
	}
	return "http://localhost:4566"
}

// requireSQSBroker segue a mesma convenção dos testes de IdP: com
// TEST_SQS_REQUIRED=1 a integração é obrigatória (pular produziria um "ok" que
// não prova integração real); sem ela, o poll é curto para não custar dezenas de
// segundos num teste que vai apenas pular.
func requireSQSBroker(t *testing.T) {
	t.Helper()

	required := strings.TrimSpace(os.Getenv("TEST_SQS_REQUIRED")) == "1"
	timeout := 5 * time.Second
	if required {
		timeout = 45 * time.Second
	}

	endpoint := e2eEndpoint()
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 3 * time.Second}
	var lastErr error

	for time.Now().Before(deadline) {
		resp, err := client.Get(endpoint + "/_ministack/health")
		if err != nil {
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

func e2eClient(t *testing.T) *sqs.Client {
	t.Helper()
	client, err := broker.NewSQSClient(context.Background(), config.SQSConfig{
		Region:          "us-east-1",
		Endpoint:        e2eEndpoint(),
		AccessKeyID:     "test",
		SecretAccessKey: "test",
	})
	if err != nil {
		t.Fatalf("NewSQSClient: %v", err)
	}
	return client
}

// e2eQueues provisiona fila + DLQ FIFO com nome único.
//
// visibilitySeconds é decisivo no teste de queda entre commit e remoção: é o
// prazo até a mensagem reaparecer. Um valor curto reproduz o efeito da queda sem
// esperar dezenas de segundos.
//
// O MiniStack persiste estado entre execuções, então nomes e
// MessageDeduplicationId são únicos por execução.
func e2eQueues(t *testing.T, client *sqs.Client, visibilitySeconds int) (queueURL, dlqURL string) {
	t.Helper()
	ctx := context.Background()
	suffix := time.Now().UnixNano()

	dlq, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String(fmt.Sprintf("wager-e2e-dlq-%d.fifo", suffix)),
		Attributes: map[string]string{"FifoQueue": "true"},
	})
	if err != nil {
		t.Fatalf("criando a DLQ: %v", err)
	}
	dlqURL = aws.ToString(dlq.QueueUrl)

	dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("obtendo o ARN da DLQ: %v", err)
	}

	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("wager-e2e-%d.fifo", suffix)),
		Attributes: map[string]string{
			"FifoQueue":                     "true",
			"VisibilityTimeout":             fmt.Sprintf("%d", visibilitySeconds),
			"ReceiveMessageWaitTimeSeconds": "0",
			"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"5"}`,
				dlqAttrs.Attributes[string(types.QueueAttributeNameQueueArn)]),
		},
	})
	if err != nil {
		t.Fatalf("criando a fila: %v", err)
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

func e2eSource(t *testing.T, queueURL, dlqURL string, visibility time.Duration) *broker.SQSMessageSource {
	t.Helper()
	// visibility é aplicado pela APLICAÇÃO em cada recebimento e sobrepõe o
	// atributo da fila. Vem por parâmetro porque um teste que precisa de
	// reentrega rápida configura aqui — no atributo seria ignorado.
	source, err := broker.NewSQSMessageSource(context.Background(), config.SQSConfig{
		Region:          "us-east-1",
		Endpoint:        e2eEndpoint(),
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		QueueName:       "fila-nao-resolvida.fifo",
		DLQName:         "dlq-nao-resolvida.fifo",
		QueueURL:        queueURL,
		DLQURL:          dlqURL,
	}, visibility, discardTestLogger())
	if err != nil {
		t.Fatalf("NewSQSMessageSource: %v", err)
	}
	return source
}

func e2eSend(t *testing.T, client *sqs.Client, queueURL, body, groupID string) {
	t.Helper()
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(fmt.Sprintf("dedup-%d", time.Now().UnixNano())),
	}); err != nil {
		t.Fatalf("enviando mensagem: %v", err)
	}
}

func e2eQueueCount(t *testing.T, client *sqs.Client, queueURL string) int {
	t.Helper()
	output, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	if err != nil {
		t.Fatalf("consultando atributos da fila: %v", err)
	}
	raw := output.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	var count int
	if _, err := fmt.Sscanf(raw, "%d", &count); err != nil {
		t.Fatalf("interpretando a contagem %q: %v", raw, err)
	}
	return count
}

func waitForCondition(t *testing.T, timeout time.Duration, what string, check func() bool) {
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

// startConsumer roda o consumidor em segundo plano e devolve a função de parada.
// A parada também é registrada como cleanup, para nunca deixar goroutine órfã.
func startConsumer(t *testing.T, consumer *SQSConsumer) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run devolveu erro: %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Errorf("o consumidor não encerrou dentro do prazo")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func e2eConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Name:            testConsumerName,
		MaxMessages:     10,
		WaitTime:        time.Second,
		MaxReceiveCount: 5,
		BaseBackoff:     time.Second,
		MaxBackoff:      10 * time.Second,
		HandlingTimeout: 30 * time.Second,
	}
}

// countingProcessor conta os tratamentos e delega ao processador real.
type countingProcessor struct {
	inner InboundProcessor
	mu    sync.Mutex
	calls int
}

func (p *countingProcessor) ProcessInbound(ctx context.Context, msg ports.InboundMessage, in InboundTransaction) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return p.inner.ProcessInbound(ctx, msg, in)
}

func (p *countingProcessor) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// ── laço completo: mensagem válida ───────────────────────────────────

func TestConsumerProcessesMessageFromRealQueue(t *testing.T) {
	requireSQSBroker(t)

	store, process, walletID := setupInbound(t)
	client := e2eClient(t)
	queueURL, dlqURL := e2eQueues(t, client, 30)
	source := e2eSource(t, queueURL, dlqURL, 30*time.Second)

	const messageID = "e2e-msg-principal"
	inbound := inboundFrom(t, inboundParams{
		messageID: messageID, walletID: walletID, externalID: "ext-e2e-1", kind: "BET", amount: "25.00",
	})
	e2eSend(t, client, queueURL, string(inbound.Body), walletID)

	consumer := NewSQSConsumer(source, NewTransactionProcessor(process, testConsumerName), e2eConsumerConfig(), discardTestLogger())
	startConsumer(t, consumer)

	// Espera o efeito DURÁVEL, não um sleep fixo.
	waitForCondition(t, 30*time.Second, "a mensagem ser concluída na inbox", func() bool {
		_, completed := inboundRows(t, messageID)
		return completed == 1
	})

	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos = %d, quer 2 (abertura + débito)", n)
	}
	if total, completed := inboundRows(t, messageID); total != 1 || completed != 1 {
		t.Fatalf("inbox: total = %d, concluídas = %d; quer 1 e 1", total, completed)
	}

	// A mensagem só sai da fila depois do commit, e nada vai para a DLQ.
	waitForCondition(t, 10*time.Second, "a fila principal ficar vazia", func() bool {
		return e2eQueueCount(t, client, queueURL) == 0
	})
	if n := e2eQueueCount(t, client, dlqURL); n != 0 {
		t.Fatalf("mensagens na DLQ = %d, quer 0", n)
	}
}

// ── laço completo: mensagem inválida ─────────────────────────────────

func TestConsumerSendsInvalidMessageToRealDLQ(t *testing.T) {
	requireSQSBroker(t)

	store, process, walletID := setupInbound(t)
	client := e2eClient(t)
	queueURL, dlqURL := e2eQueues(t, client, 30)
	source := e2eSource(t, queueURL, dlqURL, 30*time.Second)

	// Tipo não aceito pelo consumidor: nenhuma regra de negócio pode rodar.
	invalid := `{"messageId":"e2e-msg-invalida","type":"OutroEvento","occurredAt":"2026-09-08T12:00:00.000Z","data":{}}`
	e2eSend(t, client, queueURL, invalid, walletID)

	processor := &countingProcessor{inner: NewTransactionProcessor(process, testConsumerName)}
	consumer := NewSQSConsumer(source, processor, e2eConsumerConfig(), discardTestLogger())
	startConsumer(t, consumer)

	waitForCondition(t, 30*time.Second, "a mensagem chegar à DLQ", func() bool {
		return e2eQueueCount(t, client, dlqURL) == 1
	})

	// Nada de domínio foi tocado.
	if processor.count() != 0 {
		t.Fatalf("tratamentos = %d, quer 0 (mensagem inválida não chega ao caso de uso)", processor.count())
	}
	if got := balanceOf(t, store, walletID); got != "100.00" {
		t.Fatalf("saldo = %s, quer 100.00", got)
	}
	if n := ledgerRows(t, walletID); n != 1 {
		t.Fatalf("lançamentos = %d, quer 1 (apenas a abertura)", n)
	}
	if total, _ := inboundRows(t, "e2e-msg-invalida"); total != 0 {
		t.Fatalf("linhas de inbox = %d, quer 0", total)
	}

	waitForCondition(t, 10*time.Second, "a fila principal ficar vazia", func() bool {
		return e2eQueueCount(t, client, queueURL) == 0
	})
}

// ── queda entre o commit e a remoção da mensagem ─────────────────────

// dropFirstDeleteSource envolve um MessageSource real e descarta a PRIMEIRA
// remoção de mensagem.
//
// Reproduz de forma determinística a falha que o README §12 item 5 exige provar:
// o processo morre DEPOIS do commit e ANTES de remover a mensagem da fila. O
// efeito financeiro já está aplicado; a mensagem reaparece após o visibility
// timeout e é reentregue.
//
// O que se espera na reentrega: o tratamento é reconhecido pela inbox, vira
// replay e NÃO movimenta o saldo de novo.
type dropFirstDeleteSource struct {
	inner   ports.MessageSource
	dropped atomic.Bool
}

func (s *dropFirstDeleteSource) Name() string { return s.inner.Name() }

func (s *dropFirstDeleteSource) Check(ctx context.Context) error { return s.inner.Check(ctx) }

func (s *dropFirstDeleteSource) Receive(ctx context.Context, max int, wait time.Duration) ([]ports.InboundMessage, error) {
	return s.inner.Receive(ctx, max, wait)
}

func (s *dropFirstDeleteSource) Delete(ctx context.Context, receiptHandle string) error {
	if s.dropped.CompareAndSwap(false, true) {
		// A primeira remoção é perdida de propósito: a mensagem permanece na
		// fila, exatamente como se o processo tivesse caído antes de removê-la.
		return nil
	}
	return s.inner.Delete(ctx, receiptHandle)
}

func (s *dropFirstDeleteSource) Release(ctx context.Context, receiptHandle string, delay time.Duration) error {
	return s.inner.Release(ctx, receiptHandle, delay)
}

func (s *dropFirstDeleteSource) DeadLetter(ctx context.Context, msg ports.InboundMessage, reason string) error {
	return s.inner.DeadLetter(ctx, msg, reason)
}

func TestConsumerRedeliveryAfterCommitIsReplayAgainstRealQueue(t *testing.T) {
	requireSQSBroker(t)

	store, process, walletID := setupInbound(t)
	client := e2eClient(t)

	// Visibility de 1s: a mensagem reaparece logo, o que reproduz o efeito da
	// queda sem esperar dezenas de segundos. O valor é aplicado PELA APLICAÇÃO
	// (visibilityTimeout no recebimento), e este teste é a prova de que ele é
	// obedecido.
	// O atributo da fila fica em 30s DE PROPÓSITO e é ignorado: quem governa o
	// prazo é a configuração da aplicação, aplicada em cada recebimento. Se este
	// teste voltar a levar 30s, a configuração deixou de ser efetiva.
	queueURL, dlqURL := e2eQueues(t, client, 30)
	source := &dropFirstDeleteSource{inner: e2eSource(t, queueURL, dlqURL, time.Second)}

	const messageID = "e2e-msg-reentrega"
	inbound := inboundFrom(t, inboundParams{
		messageID: messageID, walletID: walletID, externalID: "ext-e2e-2", kind: "BET", amount: "25.00",
	})
	e2eSend(t, client, queueURL, string(inbound.Body), walletID)

	processor := &countingProcessor{inner: NewTransactionProcessor(process, testConsumerName)}
	consumer := NewSQSConsumer(source, processor, e2eConsumerConfig(), discardTestLogger())
	startConsumer(t, consumer)

	// A reentrega acontece quando a mensagem volta a ficar visível.
	waitForCondition(t, 40*time.Second, "a mensagem ser reentregue e virar replay", func() bool {
		return processor.count() >= 2
	})

	// O efeito financeiro é ÚNICO, apesar das duas entregas.
	if got := balanceOf(t, store, walletID); got != "75.00" {
		t.Fatalf("saldo = %s, quer 75.00 (a reentrega não pode movimentar de novo)", got)
	}
	if n := ledgerRows(t, walletID); n != 2 {
		t.Fatalf("lançamentos = %d, quer 2", n)
	}
	if n := transactionRows(t, walletID); n != 2 {
		t.Fatalf("transações = %d, quer 2", n)
	}
	if total, completed := inboundRows(t, messageID); total != 1 || completed != 1 {
		t.Fatalf("inbox: total = %d, concluídas = %d; quer 1 e 1", total, completed)
	}
	if n := outboxRows(t, "WagerTransactionProcessed"); n != 2 {
		t.Fatalf("WagerTransactionProcessed = %d, quer 2 (o replay não publica de novo)", n)
	}

	// E a mensagem finalmente sai da fila, na segunda remoção.
	waitForCondition(t, 20*time.Second, "a fila principal ficar vazia", func() bool {
		return e2eQueueCount(t, client, queueURL) == 0
	})
	if n := e2eQueueCount(t, client, dlqURL); n != 0 {
		t.Fatalf("mensagens na DLQ = %d, quer 0", n)
	}
}
