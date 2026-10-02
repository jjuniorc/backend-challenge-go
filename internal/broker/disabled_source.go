package broker

import (
	"context"
	"log/slog"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// DisabledMessageSource é um MessageSource sem efeito.
//
// Existe para que a composição tenha o MESMO grafo com o consumo ligado ou
// desligado — sem nil espalhado por dois lugares — e para que os testes da
// camada HTTP não precisem de broker. É gated por CONSUMER_ENABLED=false, que a
// configuração só aceita em ambientes locais.
type DisabledMessageSource struct {
	logger *slog.Logger
}

// NewDisabledMessageSource cria a fonte sem efeito.
func NewDisabledMessageSource(logger *slog.Logger) *DisabledMessageSource {
	return &DisabledMessageSource{logger: logger}
}

// Name identifica a fonte nos logs.
func (s *DisabledMessageSource) Name() string { return "disabled" }

// Check sempre responde saudável: não há destino externo.
func (s *DisabledMessageSource) Check(context.Context) error { return nil }

// Receive não devolve mensagem. Aguarda o tempo de long polling para não girar
// em busy loop caso algum laço seja iniciado por engano.
func (s *DisabledMessageSource) Receive(ctx context.Context, _ int, wait time.Duration) ([]ports.InboundMessage, error) {
	if wait <= 0 {
		return nil, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, nil
	}
}

// Delete não tem efeito.
func (s *DisabledMessageSource) Delete(context.Context, string) error { return nil }

// Release não tem efeito.
func (s *DisabledMessageSource) Release(context.Context, string, time.Duration) error { return nil }

// DeadLetter apenas registra o que teria sido encaminhado.
func (s *DisabledMessageSource) DeadLetter(_ context.Context, msg ports.InboundMessage, reason string) error {
	s.logger.Warn("consumo desabilitado (CONSUMER_ENABLED=false): mensagem não encaminhada",
		"messageId", msg.MessageID, "reason", reason)
	return nil
}

var _ ports.MessageSource = (*DisabledMessageSource)(nil)
