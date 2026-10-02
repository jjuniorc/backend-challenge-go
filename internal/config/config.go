// Package config carrega e valida a configuração da aplicação a partir do
// ambiente. Nenhuma dependência externa: a injeção no Fx acontece em internal/app.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Modos de autenticação.
const (
	// AuthModeOIDC valida tokens assinados pelo IdP (produção e Compose).
	AuthModeOIDC = "oidc"
	// AuthModeDev usa credenciais de desenvolvimento. Só é aceito em ambientes
	// locais e existe para testar o contrato HTTP sem o IdP.
	AuthModeDev = "dev"
)

// Config é a configuração imutável da aplicação.
type Config struct {
	Env        string
	InstanceID string

	HTTP      HTTPConfig
	DB        DBConfig
	SQS       SQSConfig
	Auth      AuthConfig
	OIDC      OIDCConfig
	Worker    WorkerConfig
	Consumer  ConsumerConfig
	Reference ReferenceConfig
	Log       LogConfig
}

// WorkerConfig parametriza o publicador da outbox.
type WorkerConfig struct {
	// OutboxEnabled permite desligar o publicador em testes que exercitam
	// apenas a camada HTTP. Só é aceito desligado em ambientes locais.
	OutboxEnabled     bool
	OutboxBatchSize   int
	OutboxInterval    time.Duration
	OutboxLease       time.Duration
	OutboxMaxAttempts int
	OutboxBaseBackoff time.Duration
	OutboxMaxBackoff  time.Duration
}

// ConsumerConfig parametriza o consumidor da fila de entrada.
type ConsumerConfig struct {
	// Enabled permite desligar o consumo em testes que exercitam apenas a
	// camada HTTP. Só é aceito desligado em ambientes locais.
	Enabled bool
	// Name identifica o consumidor na inbox (inbox_messages.consumer_name).
	Name string
	// MaxMessages é o tamanho do lote por recebimento (limite do SQS: 10).
	MaxMessages int
	// WaitTime é o long polling por recebimento (limite do SQS: 20s).
	WaitTime time.Duration
	// VisibilityTimeout é por quanto tempo a mensagem fica invisível após o
	// recebimento, acomodando o processamento. O backoff de retry usa
	// Release (ChangeMessageVisibility), então prazos maiores não dependem
	// deste valor.
	VisibilityTimeout time.Duration
	// MaxReceiveCount é o limite de entregas do redrive. Esgotado, o próprio
	// SQS move a mensagem para a DLQ; o consumidor usa o MESMO número para
	// antecipar o encaminhamento e evitar rodadas inúteis.
	MaxReceiveCount int
	// BaseBackoff é o atraso da primeira reentrega após falha transitória.
	BaseBackoff time.Duration
	// ShutdownGrace é o prazo para concluir o trabalho em andamento no
	// SIGTERM antes de liberar a visibilidade para reentrega.
	ShutdownGrace time.Duration
}

// ReferenceConfig parametriza o worker de retomada de referências pendentes.
//
// MaxAttempts/TTL/BaseBackoff/MaxBackoff são a POLÍTICA de espera, e não ajuste
// do laço: o mesmo valor governa o agendamento gravado pelo caso de uso no
// momento em que a operação fica pendente. Uma definição só evita o caso em que
// o agendamento e a decisão de esgotar discordam.
type ReferenceConfig struct {
	// Enabled permite desligar o worker em testes que exercitam apenas a
	// camada HTTP. Só é aceito desligado em ambientes locais.
	Enabled bool
	// Interval é o intervalo entre varreduras quando não há trabalho.
	Interval time.Duration
	// ErrorBackoff é a espera após erro de infraestrutura. Sem ela, um erro
	// persistente repetiria a operação vencida em ciclo apertado, porque o
	// rollback devolve a linha ao estado "vencida".
	ErrorBackoff time.Duration
	// MaxAttempts é o número TOTAL de tentativas de resolução: a inicial (que
	// registra attempts=1) mais as retomadas.
	MaxAttempts int
	// TTL é o tempo máximo desde a criação da operação em que ela pode
	// esperar, independentemente do número de tentativas.
	TTL time.Duration
	// BaseBackoff é o atraso da primeira retentativa.
	BaseBackoff time.Duration
	// MaxBackoff é o teto do backoff exponencial.
	MaxBackoff time.Duration
}

type HTTPConfig struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type DBConfig struct {
	DSN      string
	MaxConns int32
}

type SQSConfig struct {
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	QueueName       string
	DLQName         string
	// QueueURL/DLQURL são opcionais: quando vazias, a aplicação resolve a URL
	// via GetQueueUrl, evitando depender do accountId do emulador.
	QueueURL string
	DLQURL   string
}

// AuthConfig seleciona a implementação de autenticação.
type AuthConfig struct {
	Mode string
}

type OIDCConfig struct {
	// Issuer é a URL de frontend, idêntica à claim `iss` do token.
	Issuer string
	// JWKSURL é a URL de backchannel usada para buscar as chaves públicas.
	JWKSURL string
	// TokenURL é usada por ferramentas e testes para obter tokens.
	TokenURL string
	// Audience é opcional; quando definida, a claim `aud` precisa contê-la.
	Audience string
}

type LogConfig struct {
	Level string
}

// Load lê o ambiente, aplica defaults locais e valida o que é obrigatório.
func Load() (Config, error) {
	cfg := Config{
		Env:        getenv("APP_ENV", "local"),
		InstanceID: getenv("INSTANCE_ID", defaultInstanceID()),
		HTTP: HTTPConfig{
			Addr:            getenv("HTTP_ADDR", ":8080"),
			ReadTimeout:     getdur("HTTP_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    getdur("HTTP_WRITE_TIMEOUT", 15*time.Second),
			ShutdownTimeout: getdur("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second),
		},
		DB: DBConfig{
			DSN:      os.Getenv("POSTGRES_DSN"),
			MaxConns: int32(getint("POSTGRES_MAX_CONNS", 10)),
		},
		SQS: SQSConfig{
			Region:          getenv("AWS_REGION", "us-east-1"),
			Endpoint:        getenv("SQS_ENDPOINT", "http://localhost:4566"),
			AccessKeyID:     getenv("AWS_ACCESS_KEY_ID", "test"),
			SecretAccessKey: getenv("AWS_SECRET_ACCESS_KEY", "test"),
			QueueName:       getenv("SQS_QUEUE_NAME", "wager-transactions.fifo"),
			DLQName:         getenv("SQS_DLQ_NAME", "wager-transactions-dlq.fifo"),
			QueueURL:        os.Getenv("SQS_QUEUE_URL"),
			DLQURL:          os.Getenv("SQS_DLQ_URL"),
		},
		Auth: AuthConfig{Mode: strings.ToLower(getenv("AUTH_MODE", AuthModeOIDC))},
		OIDC: OIDCConfig{
			Issuer:   os.Getenv("OIDC_ISSUER"),
			JWKSURL:  os.Getenv("OIDC_JWKS_URL"),
			TokenURL: os.Getenv("OIDC_TOKEN_URL"),
			Audience: os.Getenv("OIDC_AUDIENCE"),
		},
		Worker: WorkerConfig{
			OutboxEnabled:     getbool("OUTBOX_ENABLED", true),
			OutboxBatchSize:   getint("OUTBOX_BATCH_SIZE", 20),
			OutboxInterval:    getdur("OUTBOX_INTERVAL", time.Second),
			OutboxLease:       getdur("OUTBOX_LEASE", 30*time.Second),
			OutboxMaxAttempts: getint("OUTBOX_MAX_ATTEMPTS", 10),
			OutboxBaseBackoff: getdur("OUTBOX_BASE_BACKOFF", 2*time.Second),
			OutboxMaxBackoff:  getdur("OUTBOX_MAX_BACKOFF", 5*time.Minute),
		},
		Consumer: ConsumerConfig{
			Enabled:           getbool("CONSUMER_ENABLED", true),
			Name:              getenv("CONSUMER_NAME", "wager-transactions"),
			MaxMessages:       getint("CONSUMER_MAX_MESSAGES", 10),
			WaitTime:          getdur("CONSUMER_WAIT_TIME", 20*time.Second),
			VisibilityTimeout: getdur("CONSUMER_VISIBILITY_TIMEOUT", 30*time.Second),
			MaxReceiveCount:   getint("CONSUMER_MAX_RECEIVE_COUNT", 5),
			BaseBackoff:       getdur("CONSUMER_BASE_BACKOFF", 2*time.Second),
			ShutdownGrace:     getdur("CONSUMER_SHUTDOWN_GRACE", 15*time.Second),
		},
		Reference: ReferenceConfig{
			Enabled:      getbool("REFERENCE_ENABLED", true),
			Interval:     getdur("REFERENCE_INTERVAL", time.Second),
			ErrorBackoff: getdur("REFERENCE_ERROR_BACKOFF", 2*time.Second),
			MaxAttempts:  getint("REFERENCE_MAX_ATTEMPTS", 8),
			TTL:          getdur("REFERENCE_TTL", 24*time.Hour),
			BaseBackoff:  getdur("REFERENCE_BASE_BACKOFF", 5*time.Second),
			MaxBackoff:   getdur("REFERENCE_MAX_BACKOFF", 15*time.Minute),
		},
		Log: LogConfig{Level: getenv("LOG_LEVEL", "info")},
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.DB.DSN == "" {
		return errors.New("config: POSTGRES_DSN é obrigatório")
	}
	if c.DB.MaxConns <= 0 {
		return fmt.Errorf("config: POSTGRES_MAX_CONNS deve ser > 0, recebido %d", c.DB.MaxConns)
	}
	if c.HTTP.Addr == "" {
		return errors.New("config: HTTP_ADDR não pode ser vazio")
	}
	if _, err := parseLevel(c.Log.Level); err != nil {
		return err
	}

	switch c.Auth.Mode {
	case AuthModeOIDC:
		var missing []string
		if c.OIDC.Issuer == "" {
			missing = append(missing, "OIDC_ISSUER")
		}
		if c.OIDC.JWKSURL == "" {
			missing = append(missing, "OIDC_JWKS_URL")
		}
		if len(missing) > 0 {
			return fmt.Errorf("config: AUTH_MODE=oidc exige %s", strings.Join(missing, ", "))
		}
	case AuthModeDev:
		switch c.Env {
		case "local", "dev", "test":
		default:
			return fmt.Errorf("config: AUTH_MODE=dev não é permitido com APP_ENV=%q", c.Env)
		}
	default:
		return fmt.Errorf("config: AUTH_MODE inválido: %q (use %q ou %q)", c.Auth.Mode, AuthModeOIDC, AuthModeDev)
	}

	if !c.Worker.OutboxEnabled {
		switch c.Env {
		case "local", "dev", "test":
		default:
			return fmt.Errorf("config: OUTBOX_ENABLED=false não é permitido com APP_ENV=%q", c.Env)
		}
	}
	if c.Worker.OutboxBatchSize <= 0 {
		return fmt.Errorf("config: OUTBOX_BATCH_SIZE deve ser > 0, recebido %d", c.Worker.OutboxBatchSize)
	}
	if c.Worker.OutboxMaxAttempts <= 0 {
		return fmt.Errorf("config: OUTBOX_MAX_ATTEMPTS deve ser > 0, recebido %d", c.Worker.OutboxMaxAttempts)
	}
	if c.Worker.OutboxInterval <= 0 || c.Worker.OutboxLease <= 0 {
		return errors.New("config: OUTBOX_INTERVAL e OUTBOX_LEASE devem ser > 0")
	}

	if !c.Consumer.Enabled {
		switch c.Env {
		case "local", "dev", "test":
		default:
			return fmt.Errorf("config: CONSUMER_ENABLED=false não é permitido com APP_ENV=%q", c.Env)
		}
	}
	if strings.TrimSpace(c.Consumer.Name) == "" {
		return errors.New("config: CONSUMER_NAME não pode ser vazio")
	}
	// Limites do SQS: no máximo 10 mensagens por recebimento e 20s de long polling.
	if c.Consumer.MaxMessages < 1 || c.Consumer.MaxMessages > 10 {
		return fmt.Errorf("config: CONSUMER_MAX_MESSAGES deve estar entre 1 e 10, recebido %d", c.Consumer.MaxMessages)
	}
	if c.Consumer.WaitTime < 0 || c.Consumer.WaitTime > 20*time.Second {
		return fmt.Errorf("config: CONSUMER_WAIT_TIME deve estar entre 0 e 20s, recebido %s", c.Consumer.WaitTime)
	}
	if c.Consumer.VisibilityTimeout <= 0 {
		return errors.New("config: CONSUMER_VISIBILITY_TIMEOUT deve ser > 0")
	}
	if c.Consumer.MaxReceiveCount < 1 {
		return fmt.Errorf("config: CONSUMER_MAX_RECEIVE_COUNT deve ser >= 1, recebido %d", c.Consumer.MaxReceiveCount)
	}
	if c.Consumer.BaseBackoff <= 0 || c.Consumer.ShutdownGrace <= 0 {
		return errors.New("config: CONSUMER_BASE_BACKOFF e CONSUMER_SHUTDOWN_GRACE devem ser > 0")
	}

	if !c.Reference.Enabled {
		switch c.Env {
		case "local", "dev", "test":
		default:
			return fmt.Errorf("config: REFERENCE_ENABLED=false não é permitido com APP_ENV=%q", c.Env)
		}
	}
	if c.Reference.Interval <= 0 || c.Reference.ErrorBackoff <= 0 {
		return errors.New("config: REFERENCE_INTERVAL e REFERENCE_ERROR_BACKOFF devem ser > 0")
	}
	if c.Reference.MaxAttempts < 1 {
		return fmt.Errorf("config: REFERENCE_MAX_ATTEMPTS deve ser >= 1, recebido %d", c.Reference.MaxAttempts)
	}
	if c.Reference.TTL <= 0 {
		return errors.New("config: REFERENCE_TTL deve ser > 0")
	}
	if c.Reference.BaseBackoff <= 0 || c.Reference.MaxBackoff < c.Reference.BaseBackoff {
		return fmt.Errorf("config: REFERENCE_BASE_BACKOFF deve ser > 0 e REFERENCE_MAX_BACKOFF >= ele (base=%s, max=%s)",
			c.Reference.BaseBackoff, c.Reference.MaxBackoff)
	}

	return nil
}

func getbool(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return value
}

// Redacted devolve a config para log, sem segredos nem credenciais.
func (c Config) Redacted() map[string]any {
	return map[string]any{
		"env":        c.Env,
		"instanceId": c.InstanceID,
		"httpAddr":   c.HTTP.Addr,
		"db": map[string]any{
			"maxConns": c.DB.MaxConns,
			"dsnSet":   c.DB.DSN != "",
		},
		"sqs": map[string]any{
			"region":    c.SQS.Region,
			"endpoint":  c.SQS.Endpoint,
			"queueName": c.SQS.QueueName,
			"dlqName":   c.SQS.DLQName,
		},
		"auth": map[string]any{
			"mode":     c.Auth.Mode,
			"issuer":   c.OIDC.Issuer,
			"jwksUrl":  c.OIDC.JWKSURL,
			"audience": c.OIDC.Audience,
		},
		"worker": map[string]any{
			"outboxEnabled":   c.Worker.OutboxEnabled,
			"outboxBatchSize": c.Worker.OutboxBatchSize,
			"outboxInterval":  c.Worker.OutboxInterval.String(),
		},
		"consumer": map[string]any{
			"enabled":           c.Consumer.Enabled,
			"name":              c.Consumer.Name,
			"maxMessages":       c.Consumer.MaxMessages,
			"waitTime":          c.Consumer.WaitTime.String(),
			"visibilityTimeout": c.Consumer.VisibilityTimeout.String(),
			"maxReceiveCount":   c.Consumer.MaxReceiveCount,
		},
		"reference": map[string]any{
			"enabled":     c.Reference.Enabled,
			"interval":    c.Reference.Interval.String(),
			"maxAttempts": c.Reference.MaxAttempts,
			"ttl":         c.Reference.TTL.String(),
			"baseBackoff": c.Reference.BaseBackoff.String(),
		},
		"logLevel": c.Log.Level,
	}
}

func parseLevel(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug", "info", "warn", "error":
		return strings.ToLower(strings.TrimSpace(s)), nil
	default:
		return "", fmt.Errorf("config: LOG_LEVEL inválido: %q (use debug|info|warn|error)", s)
	}
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	return v
}

func getdur(key string, def time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return def
	}
	return v
}

func defaultInstanceID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "local"
}
