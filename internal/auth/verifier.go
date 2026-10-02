package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

// allowedAlgorithms é uma ALLOWLIST explícita.
//
// Aceitar somente RS256 elimina duas classes de ataque conhecidas:
// `alg: none` (token sem assinatura) e confusão de algoritmo (HS256 usando a
// chave pública do IdP como segredo compartilhado).
var allowedAlgorithms = map[string]struct{}{"RS256": {}}

// maxTokenSize limita o tamanho aceito, evitando consumo desproporcional de
// memória com tokens absurdos.
const maxTokenSize = 8 << 10 // 8 KiB

// maxJWKSSize limita o corpo aceito do endpoint JWKS.
const maxJWKSSize = 1 << 20 // 1 MiB

// defaultStartupRetryInterval é o intervalo padrão entre tentativas de carregar
// o JWKS durante o startup.
const defaultStartupRetryInterval = 3 * time.Second

// VerifierConfig parametriza a verificação de tokens.
type VerifierConfig struct {
	// Issuer é comparado com a claim `iss` de forma EXATA.
	Issuer string
	// JWKSURL é o endpoint de chaves públicas (backchannel).
	JWKSURL string
	// Audience é opcional: quando definida, a claim `aud` precisa contê-la.
	Audience string
	// ClockSkew é a tolerância aplicada a exp/nbf.
	ClockSkew time.Duration
	// CacheTTL é por quanto tempo o JWKS é considerado fresco.
	CacheTTL time.Duration
	// HTTPClient permite injetar um cliente específico (testes, proxy).
	HTTPClient *http.Client
	// StartupTimeout é quanto tempo o construtor aguarda o IdP publicar o JWKS.
	// Zero desabilita a espera e falha na primeira tentativa.
	StartupTimeout time.Duration
	// StartupRetryInterval é o intervalo entre tentativas durante o startup.
	StartupRetryInterval time.Duration
	// Logger recebe o acompanhamento das tentativas de startup.
	Logger *slog.Logger
}

// Claims são as informações extraídas de um token válido.
type Claims struct {
	Subject    string
	Issuer     string
	Audience   []string
	ExpiresAt  time.Time
	IssuedAt   time.Time
	NotBefore  time.Time
	ProviderID string
	Roles      []string
}

// Verifier valida tokens OIDC assinados com RS256 contra um JWKS.
//
// Não faz descoberta (discovery): o issuer e a URL do JWKS vêm da configuração.
// Isso é deliberado e resolve o desencontro de hostname entre frontend
// (`iss = http://localhost:8080/...`) e backchannel (`keycloak:8080`), que é a
// causa mais comum de falha de validação dentro de Docker.
type Verifier struct {
	cfg    VerifierConfig
	client *http.Client
	logger *slog.Logger
	cache  jwksCache
}

type jwksCache struct {
	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

// NewVerifier valida a configuração e carrega o JWKS uma vez, para falhar cedo
// caso o IdP esteja inacessível ou mal configurado.
func NewVerifier(ctx context.Context, cfg VerifierConfig) (*Verifier, error) {
	if strings.TrimSpace(cfg.Issuer) == "" {
		return nil, errors.New("auth: OIDC_ISSUER é obrigatório no modo oidc")
	}
	if strings.TrimSpace(cfg.JWKSURL) == "" {
		return nil, errors.New("auth: OIDC_JWKS_URL é obrigatório no modo oidc")
	}
	if cfg.ClockSkew <= 0 {
		cfg.ClockSkew = 30 * time.Second
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 15 * time.Minute
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.StartupRetryInterval <= 0 {
		cfg.StartupRetryInterval = defaultStartupRetryInterval
	}

	verifier := &Verifier{cfg: cfg, client: client, logger: logger}
	if err := verifier.loadJWKSWithRetry(ctx); err != nil {
		return nil, err
	}
	return verifier, nil
}

// loadJWKSWithRetry aguarda o IdP publicar o JWKS.
//
// Motivo: um IdP recém-iniciado leva dezenas de segundos para ficar pronto,
// enquanto a aplicação sobe logo depois das migrations. Sem esta espera, a
// aplicação entraria em ciclo de reinício apenas porque o IdP é lento — e a
// imagem do Keycloak não permite healthcheck por shell, então a espera precisa
// acontecer aqui.
//
// O fail-fast é preservado: esgotado StartupTimeout, o erro é devolvido e a
// aplicação NÃO sobe sem conseguir validar credenciais.
func (v *Verifier) loadJWKSWithRetry(ctx context.Context) error {
	err := v.refresh(ctx)
	if err == nil {
		return nil
	}
	if v.cfg.StartupTimeout <= 0 {
		return err
	}

	deadline := time.Now().Add(v.cfg.StartupTimeout)
	for time.Now().Before(deadline) {
		v.logger.Warn("aguardando o IdP publicar o JWKS",
			"jwksUrl", v.cfg.JWKSURL,
			"retryIn", v.cfg.StartupRetryInterval.String(),
			"err", err,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(v.cfg.StartupRetryInterval):
		}

		if err = v.refresh(ctx); err == nil {
			v.logger.Info("JWKS carregado do IdP", "jwksUrl", v.cfg.JWKSURL)
			return nil
		}
	}

	return fmt.Errorf("auth: JWKS indisponível em %s após %s: %w", v.cfg.JWKSURL, v.cfg.StartupTimeout, err)
}

// Verify valida assinatura, algoritmo, issuer, expiração, nbf e audience, e
// devolve as claims.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (Claims, error) {
	token := strings.TrimSpace(rawToken)
	if token == "" {
		return Claims{}, ports.ErrMissingCredentials
	}
	if len(token) > maxTokenSize {
		return Claims{}, fmt.Errorf("%w: token maior que o limite aceito", ports.ErrInvalidCredentials)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("%w: token não está no formato JWS compacto", ports.ErrInvalidCredentials)
	}
	headerRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: cabeçalho não é base64url", ports.ErrInvalidCredentials)
	}
	payloadRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: payload não é base64url", ports.ErrInvalidCredentials)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: assinatura não é base64url", ports.ErrInvalidCredentials)
	}

	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerRaw, &header); err != nil {
		return Claims{}, fmt.Errorf("%w: cabeçalho inválido", ports.ErrInvalidCredentials)
	}
	if _, ok := allowedAlgorithms[header.Alg]; !ok {
		return Claims{}, fmt.Errorf("%w: algoritmo %q não permitido", ports.ErrInvalidCredentials, header.Alg)
	}
	if strings.TrimSpace(header.Kid) == "" {
		return Claims{}, fmt.Errorf("%w: token sem kid", ports.ErrInvalidCredentials)
	}

	publicKey, err := v.keyFor(ctx, header.Kid)
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %v", ports.ErrInvalidCredentials, err)
	}

	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, digest[:], signature); err != nil {
		return Claims{}, fmt.Errorf("%w: assinatura inválida", ports.ErrInvalidCredentials)
	}

	var raw struct {
		Issuer      string          `json:"iss"`
		Subject     string          `json:"sub"`
		Audience    json.RawMessage `json:"aud"`
		ExpiresAt   int64           `json:"exp"`
		IssuedAt    int64           `json:"iat"`
		NotBefore   int64           `json:"nbf"`
		ProviderID  string          `json:"providerId"`
		RealmAccess struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	if err := json.Unmarshal(payloadRaw, &raw); err != nil {
		return Claims{}, fmt.Errorf("%w: payload inválido", ports.ErrInvalidCredentials)
	}

	if raw.Issuer != v.cfg.Issuer {
		return Claims{}, fmt.Errorf("%w: issuer %q difere do esperado", ports.ErrInvalidCredentials, raw.Issuer)
	}
	if raw.ExpiresAt == 0 {
		return Claims{}, fmt.Errorf("%w: token sem claim exp", ports.ErrInvalidCredentials)
	}

	now := time.Now().UTC()
	skew := v.cfg.ClockSkew
	expiresAt := time.Unix(raw.ExpiresAt, 0).UTC()
	if now.After(expiresAt.Add(skew)) {
		return Claims{}, fmt.Errorf("%w: token expirado em %s", ports.ErrInvalidCredentials, expiresAt.Format(time.RFC3339))
	}

	var notBefore time.Time
	if raw.NotBefore != 0 {
		notBefore = time.Unix(raw.NotBefore, 0).UTC()
		if now.Add(skew).Before(notBefore) {
			return Claims{}, fmt.Errorf("%w: token ainda não é válido", ports.ErrInvalidCredentials)
		}
	}

	var issuedAt time.Time
	if raw.IssuedAt != 0 {
		issuedAt = time.Unix(raw.IssuedAt, 0).UTC()
	}

	audience := decodeAudience(raw.Audience)
	if v.cfg.Audience != "" && !containsString(audience, v.cfg.Audience) {
		return Claims{}, fmt.Errorf("%w: audience %v não contém %q", ports.ErrInvalidCredentials, audience, v.cfg.Audience)
	}

	return Claims{
		Subject:    raw.Subject,
		Issuer:     raw.Issuer,
		Audience:   audience,
		ExpiresAt:  expiresAt,
		IssuedAt:   issuedAt,
		NotBefore:  notBefore,
		ProviderID: strings.TrimSpace(raw.ProviderID),
		Roles:      raw.RealmAccess.Roles,
	}, nil
}

// keyFor devolve a chave do kid informado, atualizando o JWKS quando a chave é
// desconhecida (rotação) ou quando o cache venceu.
func (v *Verifier) keyFor(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.cache.mu.RLock()
	key, found := v.cache.keys[kid]
	fetchedAt := v.cache.fetchedAt
	v.cache.mu.RUnlock()

	stale := time.Since(fetchedAt) > v.cfg.CacheTTL
	if found && !stale {
		return key, nil
	}

	if err := v.refresh(ctx); err != nil {
		if found {
			// Mantém a chave conhecida se a atualização falhar: indisponibilidade
			// temporária do IdP não deve invalidar tokens legítimos.
			return key, nil
		}
		return nil, fmt.Errorf("chave %q desconhecida e JWKS indisponível: %w", kid, err)
	}

	v.cache.mu.RLock()
	key, found = v.cache.keys[kid]
	v.cache.mu.RUnlock()
	if !found {
		return nil, fmt.Errorf("chave %q não encontrada no JWKS", kid)
	}
	return key, nil
}

func (v *Verifier) refresh(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return fmt.Errorf("auth: montando requisição do JWKS: %w", err)
	}
	response, err := v.client.Do(request)
	if err != nil {
		return fmt.Errorf("auth: buscando JWKS em %s: %w", v.cfg.JWKSURL, err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("auth: JWKS retornou HTTP %d", response.StatusCode)
	}

	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, maxJWKSSize)).Decode(&set); err != nil {
		return fmt.Errorf("auth: decodificando JWKS: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, key := range set.Keys {
		if key.Kid == "" {
			continue
		}
		if key.Use != "" && key.Use != "sig" {
			continue
		}
		publicKey, err := rsaPublicKey(key.Kty, key.N, key.E)
		if err != nil {
			continue
		}
		keys[key.Kid] = publicKey
	}
	if len(keys) == 0 {
		return errors.New("auth: JWKS não contém chaves RSA de assinatura utilizáveis")
	}

	v.cache.mu.Lock()
	v.cache.keys = keys
	v.cache.fetchedAt = time.Now()
	v.cache.mu.Unlock()
	return nil
}

// rsaPublicKey monta a chave pública a partir dos parâmetros JWK (n, e).
func rsaPublicKey(kty, n, e string) (*rsa.PublicKey, error) {
	if kty != "RSA" {
		return nil, fmt.Errorf("kty %q não suportado", kty)
	}
	modulusBytes, err := base64.RawURLEncoding.DecodeString(n)
	if err != nil || len(modulusBytes) == 0 {
		return nil, errors.New("parâmetro n inválido")
	}
	exponentBytes, err := base64.RawURLEncoding.DecodeString(e)
	if err != nil || len(exponentBytes) == 0 || len(exponentBytes) > 8 {
		return nil, errors.New("parâmetro e inválido")
	}
	exponent := 0
	for _, b := range exponentBytes {
		exponent = exponent<<8 | int(b)
	}
	if exponent < 3 {
		return nil, errors.New("expoente RSA inválido")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(modulusBytes), E: exponent}, nil
}

// decodeAudience aceita `aud` como string única ou como lista.
func decodeAudience(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		if single == "" {
			return nil
		}
		return []string{single}
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil {
		return list
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
