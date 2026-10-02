package broker

import (
	"context"
	"log/slog"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// LogOnlyPublisher registra os eventos que seriam publicados, sem enviar nada.
//
// Existe para que testes da camada HTTP não precisem de broker. É gated por
// OUTBOX_ENABLED=false, que a configuração só aceita em ambientes locais — não
// há como desligar a publicação em produção por descuido de configuração.
type LogOnlyPublisher struct {
	logger *slog.Logger
}

// NewLogOnlyPublisher cria o publicador de log.
func NewLogOnlyPublisher(logger *slog.Logger) *LogOnlyPublisher {
	return &LogOnlyPublisher{logger: logger}
}

// Name identifica o destino.
func (p *LogOnlyPublisher) Name() string { return "disabled" }

// Check sempre responde saudável: não há destino externo.
func (p *LogOnlyPublisher) Check(context.Context) error { return nil }

// Publish apenas registra o evento.
func (p *LogOnlyPublisher) Publish(_ context.Context, record ports.OutboxRecord) error {
	p.logger.Info("publicação desabilitada (OUTBOX_ENABLED=false)",
		"eventId", record.EventID, "eventType", record.EventType)
	return nil
}

var _ ports.Publisher = (*LogOnlyPublisher)(nil)
