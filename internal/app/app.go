// Package app é a raiz de composição da aplicação.
//
// Só este pacote conhece Fx. Domínio, casos de uso, repositórios e a camada HTTP
// continuam independentes: recebem suas dependências por construtor.
//
// A ordem de encerramento segue o fx.Lifecycle, que executa os hooks OnStop na
// ordem INVERSA do registro:
//
//  1. os workers (registrados no último fx.Invoke) param primeiro, concluindo
//     ou liberando o trabalho em andamento;
//  2. o servidor HTTP para em seguida, deixando de aceitar requisições;
//  3. o pool do PostgreSQL (registrado num fx.Provide, portanto antes de tudo)
//     é fechado por último.
//
// Nenhum worker perde a conexão de que ainda precisa, e a última coisa a fechar
// é justamente o recurso que todos usam.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"

	"github.com/jjuniorc/backend-challenge-go/internal/auth"
	"github.com/jjuniorc/backend-challenge-go/internal/broker"
	"github.com/jjuniorc/backend-challenge-go/internal/config"
	"github.com/jjuniorc/backend-challenge-go/internal/httpapi"
	"github.com/jjuniorc/backend-challenge-go/internal/metrics"
	"github.com/jjuniorc/backend-challenge-go/internal/ports"
	"github.com/jjuniorc/backend-challenge-go/internal/repository/pg"
	"github.com/jjuniorc/backend-challenge-go/internal/usecase"
	"github.com/jjuniorc/backend-challenge-go/internal/worker"
)

// Module é a composição completa da aplicação.
var Module = fx.Module("wager",
	fx.Provide(
		config.Load,
		provideLogger,
		provideIDGenerator,
		provideClock,
		provideReferencePolicy,
		provideStore,
		bindStore,
		provideAuthenticator,
		usecase.NewOpenWallet,
		usecase.NewProcessTransaction,
		usecase.NewGetWallet,
		usecase.NewGetLedger,
		usecase.NewGetTransaction,
		usecase.NewReconcile,
		provideMetrics,
		provideHandler,
		provideRouter,
		provideServer,
		providePublisher,
		provideOutboxPublisher,
		provideOutboxRunner,
		provideMessageSource,
		provideInboundProcessor,
		provideConsumer,
		provideConsumerRunner,
		provideReferenceWorker,
		provideReferenceRunner,
	),
	// Registro invertido de propósito: registerWorkers vem DEPOIS de
	// registerHTTPServer, então seus hooks OnStop rodam ANTES dos do servidor.
	// Os workers encerram com o servidor ainda no ar.
	fx.Invoke(registerHTTPServer),
	fx.Invoke(registerWorkers),
)

func provideLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.Log.Level)); err != nil {
		level = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}

func provideIDGenerator() ports.IDGenerator { return usecase.UUIDv7{} }

func provideClock() ports.Clock { return usecase.SystemClock{} }

// provideReferencePolicy publica a política de espera pela referência.
//
// Vem da CONFIGURAÇÃO, e não de DefaultReferencePolicy() fixo, porque o mesmo
// valor governa dois lugares: o agendamento gravado pelo caso de uso quando a
// operação fica pendente, e a decisão de esgotar tomada pelo worker. Duas
// definições poderiam discordar — o caso de uso agendaria uma retomada que o
// worker já consideraria esgotada, e nada apareceria no log.
func provideReferencePolicy(cfg config.Config) usecase.ReferencePolicy {
	return usecase.ReferencePolicy{
		MaxAttempts: cfg.Reference.MaxAttempts,
		TTL:         cfg.Reference.TTL,
		BaseBackoff: cfg.Reference.BaseBackoff,
		MaxBackoff:  cfg.Reference.MaxBackoff,
	}
}

// bindStore publica *pg.DB como ports.Store para os casos de uso, sem que eles
// conheçam a implementação.
func bindStore(db *pg.DB) ports.Store { return db }

// provideStore abre o pool e registra o fechamento no ciclo de vida.
func provideStore(lifecycle fx.Lifecycle, cfg config.Config, logger *slog.Logger) (*pg.DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	store, err := pg.Open(ctx, cfg.DB.DSN, cfg.DB.MaxConns)
	if err != nil {
		return nil, fmt.Errorf("abrindo pool do PostgreSQL: %w", err)
	}
	logger.Info("pool do PostgreSQL aberto", "maxConns", cfg.DB.MaxConns)

	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return store.Ping(pingCtx)
		},
		OnStop: func(context.Context) error {
			store.Close()
			logger.Info("pool do PostgreSQL fechado")
			return nil
		},
	})
	return store, nil
}

func provideAuthenticator(cfg config.Config) (ports.Authenticator, error) {
	return auth.NewAuthenticator(cfg)
}

// namedChecker dá um nome estável a um verificador no mapa de readiness.
//
// Necessário porque os dois destinos SQS (saída e entrada) compartilham o mesmo
// Name() ("sqs"): sem isso, um sobrescreveria o outro no mapa de checks e o
// readiness passaria a mentir sobre qual dos dois está fora.
type namedChecker struct {
	name    string
	checker ports.HealthChecker
}

func (n namedChecker) Name() string { return n.name }

func (n namedChecker) Check(ctx context.Context) error { return n.checker.Check(ctx) }

func provideHandler(
	cfg config.Config,
	openWallet *usecase.OpenWallet,
	process *usecase.ProcessTransaction,
	getWallet *usecase.GetWallet,
	getLedger *usecase.GetLedger,
	getTx *usecase.GetTransaction,
	reconcile *usecase.Reconcile,
	db *pg.DB,
	publisher ports.Publisher,
	consumer *worker.SQSConsumer,
	appMetrics *metrics.Metrics,
	logger *slog.Logger,
) *httpapi.Handler {
	// Readiness de dependências externas (README §9): PostgreSQL e SQS. O
	// verificador de cada destino entra apenas quando o destino está de fato em
	// uso — anunciar "sqs-inbound up" com o consumo desligado seria mentira.
	checkers := []ports.HealthChecker{db}
	if cfg.Worker.OutboxEnabled {
		checkers = append(checkers, namedChecker{name: "sqs-outbound", checker: publisher})
	}
	if cfg.Consumer.Enabled {
		checkers = append(checkers, namedChecker{name: "sqs-inbound", checker: consumer})
	}

	return httpapi.NewHandler(httpapi.HandlerDeps{
		OpenWallet: openWallet,
		Process:    process,
		GetWallet:  getWallet,
		GetLedger:  getLedger,
		GetTx:      getTx,
		Reconcile:  reconcile,
		Checkers:   checkers,
		Metrics:    appMetrics,
		Logger:     logger,
	})
}

func provideRouter(handler *httpapi.Handler, authenticator ports.Authenticator, ids ports.IDGenerator, appMetrics *metrics.Metrics, logger *slog.Logger) *gin.Engine {
	return httpapi.NewRouter(httpapi.RouterDeps{
		Handler:       handler,
		Authenticator: authenticator,
		IDs:           ids,
		Metrics:       appMetrics,
		Logger:        logger,
	})
}

func provideServer(cfg config.Config, router *gin.Engine, logger *slog.Logger) *httpapi.Server {
	return httpapi.NewServer(httpapi.ServerDeps{
		Handler:         router,
		Addr:            cfg.HTTP.Addr,
		ReadTimeout:     cfg.HTTP.ReadTimeout,
		WriteTimeout:    cfg.HTTP.WriteTimeout,
		ShutdownTimeout: cfg.HTTP.ShutdownTimeout,
		Logger:          logger,
	})
}

// providePublisher escolhe o destino dos eventos: SQS ou (quando explicitamente
// desabilitado em ambiente local) um publicador que apenas registra em log.
func providePublisher(cfg config.Config, logger *slog.Logger) (ports.Publisher, error) {
	if !cfg.Worker.OutboxEnabled {
		logger.Warn("publicação da outbox desabilitada (OUTBOX_ENABLED=false)")
		return broker.NewLogOnlyPublisher(logger), nil
	}

	// context.Context NÃO é injetável pelo Fx (não faz parte do grafo de tipos).
	// O prazo aqui é apenas para resolver a URL da fila no startup: falhar cedo
	// se o broker estiver inacessível ou a fila não existir.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return broker.NewSQSPublisher(ctx, cfg.SQS, logger)
}

func provideOutboxPublisher(store ports.Store, publisher ports.Publisher, cfg config.Config, logger *slog.Logger) *worker.OutboxPublisher {
	return worker.NewOutboxPublisher(store, publisher, worker.OutboxConfig{
		ClaimerID:   cfg.InstanceID,
		BatchSize:   cfg.Worker.OutboxBatchSize,
		Interval:    cfg.Worker.OutboxInterval,
		Lease:       cfg.Worker.OutboxLease,
		MaxAttempts: cfg.Worker.OutboxMaxAttempts,
		BaseBackoff: cfg.Worker.OutboxBaseBackoff,
		MaxBackoff:  cfg.Worker.OutboxMaxBackoff,
	}, logger)
}

func provideOutboxRunner(publisher *worker.OutboxPublisher, cfg config.Config, logger *slog.Logger) (*worker.OutboxRunner, error) {
	if !cfg.Worker.OutboxEnabled {
		// Runner sem efeito: mantém o grafo do Fx idêntico com o worker ligado
		// ou desligado, evitando condicionais na composição.
		return worker.NewOutboxRunner(nil, logger), nil
	}
	return worker.NewOutboxRunner(publisher, logger), nil
}

// registerWorkers liga os workers ao ciclo de vida.
//
// Um hook por worker, e não um só: assim o encerramento de cada um é
// independente e o log diz qual deles terminou. Um worker desligado não registra
// hook nenhum — o runner no-op existe apenas para manter o grafo idêntico.
func registerWorkers(
	lifecycle fx.Lifecycle,
	outbox *worker.OutboxRunner,
	consumer *worker.ConsumerRunner,
	reference *worker.ReferenceRunner,
	cfg config.Config,
	logger *slog.Logger,
) {
	if cfg.Worker.OutboxEnabled {
		lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				logger.Info("iniciando publicador da outbox", "instanceId", cfg.InstanceID)
				return outbox.Start(ctx)
			},
			OnStop: func(ctx context.Context) error {
				return outbox.Stop(ctx)
			},
		})
	}

	if cfg.Consumer.Enabled {
		lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				logger.Info("iniciando consumidor da fila de entrada",
					"instanceId", cfg.InstanceID, "consumer", cfg.Consumer.Name)
				return consumer.Start(ctx)
			},
			OnStop: func(ctx context.Context) error {
				return consumer.Stop(ctx)
			},
		})
	}

	if cfg.Reference.Enabled {
		lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				logger.Info("iniciando worker de referências",
					"instanceId", cfg.InstanceID, "interval", cfg.Reference.Interval.String())
				return reference.Start(ctx)
			},
			OnStop: func(ctx context.Context) error {
				return reference.Stop(ctx)
			},
		})
	}
}

// provideMessageSource escolhe a origem das mensagens de entrada: a fila SQS ou
// uma fonte sem efeito quando o consumo está explicitamente desabilitado em
// ambiente local.
func provideMessageSource(cfg config.Config, logger *slog.Logger) (ports.MessageSource, error) {
	if !cfg.Consumer.Enabled {
		logger.Warn("consumo da fila de entrada desabilitado (CONSUMER_ENABLED=false)")
		return broker.NewDisabledMessageSource(logger), nil
	}

	// context.Context NÃO é injetável pelo Fx. O prazo aqui serve apenas para
	// resolver as URLs das filas no startup: falhar cedo se o broker estiver
	// inacessível ou as filas não existirem.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return broker.NewSQSMessageSource(ctx, cfg.SQS, cfg.Consumer.VisibilityTimeout, logger)
}

// provideInboundProcessor liga o tratamento da mensagem ao MESMO caso de uso
// usado pela camada HTTP, com o nome do consumidor gravado na inbox.
//
// O tipo declarado é a interface: a composição escolhe a implementação, e trocar
// o caminho de tratamento (por exemplo, para aceite assíncrono) não exige mexer
// no consumidor.
func provideInboundProcessor(process *usecase.ProcessTransaction, cfg config.Config) worker.InboundProcessor {
	return worker.NewTransactionProcessor(process, cfg.Consumer.Name)
}

func provideConsumer(source ports.MessageSource, processor worker.InboundProcessor, cfg config.Config, logger *slog.Logger) *worker.SQSConsumer {
	return worker.NewSQSConsumer(source, processor, worker.ConsumerConfig{
		Name:            cfg.Consumer.Name,
		MaxMessages:     cfg.Consumer.MaxMessages,
		WaitTime:        cfg.Consumer.WaitTime,
		MaxReceiveCount: cfg.Consumer.MaxReceiveCount,
		BaseBackoff:     cfg.Consumer.BaseBackoff,
		MaxBackoff:      cfg.Worker.OutboxMaxBackoff,
		// O mesmo prazo governa o tratamento de uma mensagem e a janela de
		// encerramento: é o tempo máximo que uma mensagem já em andamento pode
		// levar para concluir depois do SIGTERM. Um prazo próprio para o
		// tratamento seria mais uma variável para o mesmo conceito.
		HandlingTimeout: cfg.Consumer.ShutdownGrace,
	}, logger)
}

// provideConsumerRunner adapta o consumidor ao ciclo de vida.
func provideConsumerRunner(consumer *worker.SQSConsumer, cfg config.Config, logger *slog.Logger) *worker.ConsumerRunner {
	if !cfg.Consumer.Enabled {
		// Runner sem efeito: mantém o grafo do Fx idêntico com o consumo ligado
		// ou desligado, evitando condicionais na composição.
		return worker.NewConsumerRunner(nil, logger)
	}
	return worker.NewConsumerRunner(consumer, logger)
}

// provideReferenceWorker monta o worker de retomada de referências sobre o
// MESMO caso de uso usado pela camada HTTP: a retomada não tem regra própria,
// apenas um gatilho.
func provideReferenceWorker(process *usecase.ProcessTransaction, cfg config.Config, logger *slog.Logger) *worker.ReferenceWorker {
	return worker.NewReferenceWorker(process, worker.ReferenceConfig{
		Interval:     cfg.Reference.Interval,
		ErrorBackoff: cfg.Reference.ErrorBackoff,
	}, logger)
}

// provideReferenceRunner adapta o worker ao ciclo de vida.
func provideReferenceRunner(w *worker.ReferenceWorker, cfg config.Config, logger *slog.Logger) *worker.ReferenceRunner {
	if !cfg.Reference.Enabled {
		// Runner sem efeito: mantém o grafo do Fx idêntico com o worker ligado
		// ou desligado.
		return worker.NewReferenceRunner(nil, logger)
	}
	return worker.NewReferenceRunner(w, logger)
}

func registerHTTPServer(lifecycle fx.Lifecycle, server *httpapi.Server, cfg config.Config, logger *slog.Logger) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := server.Start(ctx); err != nil {
				return err
			}
			logger.Info("servidor HTTP no ar",
				"addr", server.Addr(),
				"instanceId", cfg.InstanceID,
				"env", cfg.Env,
			)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("encerrando servidor HTTP", "instanceId", cfg.InstanceID)
			return server.Stop(ctx)
		},
	})
}
