package auth

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

const testIssuerURL = "http://issuer.test/realms/wager"

// testIssuer gera uma chave RSA própria e publica o JWKS, permitindo exercitar
// TODOS os caminhos de rejeição sem depender do Keycloak.
type testIssuer struct {
	key      *rsa.PrivateKey
	kid      string
	extraKey *rsa.PrivateKey
	extraKid string
	server   *httptest.Server
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gerando chave RSA: %v", err)
	}

	issuer := &testIssuer{key: key, kid: "test-key-1"}

	mux := http.NewServeMux()
	mux.HandleFunc("/certs", func(w http.ResponseWriter, _ *http.Request) {
		keys := []map[string]string{jwkEntry(issuer.key, issuer.kid)}
		if issuer.extraKey != nil {
			keys = append(keys, jwkEntry(issuer.extraKey, issuer.extraKid))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	})
	issuer.server = httptest.NewServer(mux)
	t.Cleanup(issuer.server.Close)

	return issuer
}

func jwkEntry(key *rsa.PrivateKey, kid string) map[string]string {
	return map[string]string{
		"kty": "RSA",
		"kid": kid,
		"use": "sig",
		"alg": "RS256",
		"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
	}
}

func (i *testIssuer) jwksURL() string { return i.server.URL + "/certs" }

func (i *testIssuer) newVerifier(t *testing.T, audience string) *Verifier {
	t.Helper()
	verifier, err := NewVerifier(context.Background(), VerifierConfig{
		Issuer:   testIssuerURL,
		JWKSURL:  i.jwksURL(),
		Audience: audience,
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return verifier
}

func (i *testIssuer) claims(overrides map[string]any) map[string]any {
	claims := map[string]any{
		"iss":          testIssuerURL,
		"sub":          "service-account-provider-a",
		"aud":          []string{"wager-api"},
		"exp":          time.Now().Add(5 * time.Minute).Unix(),
		"iat":          time.Now().Add(-time.Second).Unix(),
		"providerId":   "provider-a",
		"realm_access": map[string]any{"roles": []string{ports.RoleProvider}},
	}
	for key, value := range overrides {
		claims[key] = value
	}
	return claims
}

func (i *testIssuer) sign(t *testing.T, claims map[string]any) string {
	t.Helper()
	return i.signWith(t, map[string]any{"alg": "RS256", "typ": "JWT", "kid": i.kid}, claims, i.key)
}

func (i *testIssuer) signWith(t *testing.T, header, claims map[string]any, key *rsa.PrivateKey) string {
	t.Helper()
	headerJSON, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("serializando cabeçalho: %v", err)
	}
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("serializando claims: %v", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." +
		base64.RawURLEncoding.EncodeToString(payloadJSON)

	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("assinando: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// ── caminho feliz ────────────────────────────────────────────────────

func TestVerifyAcceptsValidToken(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := issuer.newVerifier(t, "wager-api")

	claims, err := verifier.Verify(context.Background(), issuer.sign(t, issuer.claims(nil)))
	if err != nil {
		t.Fatalf("token válido rejeitado: %v", err)
	}
	if claims.ProviderID != "provider-a" {
		t.Fatalf("providerId = %q", claims.ProviderID)
	}
	if !containsString(claims.Roles, ports.RoleProvider) {
		t.Fatalf("roles = %v", claims.Roles)
	}
	if claims.Subject != "service-account-provider-a" {
		t.Fatalf("sub = %q", claims.Subject)
	}
	if claims.ExpiresAt.IsZero() {
		t.Fatal("exp não extraído")
	}
}

func TestVerifyAcceptsSingleStringAudience(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := issuer.newVerifier(t, "wager-api")

	token := issuer.sign(t, issuer.claims(map[string]any{"aud": "wager-api"}))
	if _, err := verifier.Verify(context.Background(), token); err != nil {
		t.Fatalf("aud como string deveria ser aceita: %v", err)
	}
}

func TestVerifySkipsAudienceWhenNotConfigured(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := issuer.newVerifier(t, "")

	token := issuer.sign(t, issuer.claims(map[string]any{"aud": []string{"outro"}}))
	if _, err := verifier.Verify(context.Background(), token); err != nil {
		t.Fatalf("sem audience configurada a claim não deve ser exigida: %v", err)
	}
}

// ── rejeições ────────────────────────────────────────────────────────

func TestVerifyRejects(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := issuer.newVerifier(t, "wager-api")

	tampered := func() string {
		token := issuer.sign(t, issuer.claims(nil))
		return token[:len(token)-4] + "AAAA"
	}()

	hs256 := func() string {
		header := map[string]any{"alg": "HS256", "typ": "JWT", "kid": issuer.kid}
		claims := issuer.claims(nil)
		headerJSON, _ := json.Marshal(header)
		payloadJSON, _ := json.Marshal(claims)
		signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." +
			base64.RawURLEncoding.EncodeToString(payloadJSON)
		mac := hmac.New(sha256.New, []byte("segredo-adivinhado"))
		mac.Write([]byte(signingInput))
		return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	}()

	cases := []struct {
		name   string
		token  string
		target error
	}{
		{"vazio", "", ports.ErrMissingCredentials},
		{"sem tres partes", "a.b", ports.ErrInvalidCredentials},
		{"cabecalho nao base64", "!!!.payload.sig", ports.ErrInvalidCredentials},
		{"algoritmo none", issuer.signWith(t,
			map[string]any{"alg": "none", "typ": "JWT", "kid": issuer.kid}, issuer.claims(nil), issuer.key),
			ports.ErrInvalidCredentials},
		{"algoritmo HS256", hs256, ports.ErrInvalidCredentials},
		{"sem kid", issuer.signWith(t,
			map[string]any{"alg": "RS256", "typ": "JWT"}, issuer.claims(nil), issuer.key),
			ports.ErrInvalidCredentials},
		{"kid desconhecido", issuer.signWith(t,
			map[string]any{"alg": "RS256", "typ": "JWT", "kid": "chave-que-nao-existe"}, issuer.claims(nil), issuer.key),
			ports.ErrInvalidCredentials},
		{"assinatura adulterada", tampered, ports.ErrInvalidCredentials},
		{"assinatura de outra chave", func() string {
			other, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatalf("gerando chave: %v", err)
			}
			return issuer.signWith(t, map[string]any{"alg": "RS256", "typ": "JWT", "kid": issuer.kid}, issuer.claims(nil), other)
		}(), ports.ErrInvalidCredentials},
		{"issuer errado", issuer.sign(t, issuer.claims(map[string]any{"iss": "http://outro-idp/realms/wager"})), ports.ErrInvalidCredentials},
		{"expirado", issuer.sign(t, issuer.claims(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})), ports.ErrInvalidCredentials},
		{"expirado alem da tolerancia", issuer.sign(t, issuer.claims(map[string]any{"exp": time.Now().Add(-time.Minute).Unix()})), ports.ErrInvalidCredentials},
		{"sem exp", issuer.sign(t, issuer.claims(map[string]any{"exp": 0})), ports.ErrInvalidCredentials},
		{"nbf no futuro", issuer.sign(t, issuer.claims(map[string]any{"nbf": time.Now().Add(time.Hour).Unix()})), ports.ErrInvalidCredentials},
		{"audience errada", issuer.sign(t, issuer.claims(map[string]any{"aud": []string{"outro-servico"}})), ports.ErrInvalidCredentials},
		{"payload invalido", "eyJhbGciOiJSUzI1NiIsImtpZCI6InRlc3Qta2V5LTEifQ.!!!.AAAA", ports.ErrInvalidCredentials},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := verifier.Verify(context.Background(), tc.token)
			if err == nil {
				t.Fatal("token deveria ser rejeitado")
			}
			if !errors.Is(err, tc.target) {
				t.Fatalf("erro = %v, quer %v", err, tc.target)
			}
		})
	}
}

func TestVerifyAcceptsTokenWithinClockSkew(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := issuer.newVerifier(t, "wager-api")

	// expirado há 5s, dentro da tolerância padrão de 30s
	token := issuer.sign(t, issuer.claims(map[string]any{"exp": time.Now().Add(-5 * time.Second).Unix()}))
	if _, err := verifier.Verify(context.Background(), token); err != nil {
		t.Fatalf("token dentro da tolerância deveria ser aceito: %v", err)
	}
}

// ── rotação de chave ─────────────────────────────────────────────────

func TestVerifyRefreshesJWKSOnUnknownKeyID(t *testing.T) {
	issuer := newTestIssuer(t)
	verifier := issuer.newVerifier(t, "wager-api")

	// O IdP publica uma segunda chave (rotação).
	extra, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gerando chave: %v", err)
	}
	issuer.extraKey = extra
	issuer.extraKid = "test-key-2"

	token := issuer.signWith(t,
		map[string]any{"alg": "RS256", "typ": "JWT", "kid": "test-key-2"},
		issuer.claims(nil), extra)

	if _, err := verifier.Verify(context.Background(), token); err != nil {
		t.Fatalf("token com kid novo deveria ser aceito após refresh do JWKS: %v", err)
	}
}

func TestNewVerifierFailsWhenJWKSIsUnreachable(t *testing.T) {
	_, err := NewVerifier(context.Background(), VerifierConfig{
		Issuer:   testIssuerURL,
		JWKSURL:  "http://127.0.0.1:1/certs",
		Audience: "wager-api",
	})
	if err == nil {
		t.Fatal("NewVerifier deveria falhar com JWKS inacessível")
	}
}

func TestNewVerifierRequiresIssuerAndJWKS(t *testing.T) {
	if _, err := NewVerifier(context.Background(), VerifierConfig{JWKSURL: "http://x"}); err == nil {
		t.Fatal("deveria exigir issuer")
	}
	if _, err := NewVerifier(context.Background(), VerifierConfig{Issuer: "http://x"}); err == nil {
		t.Fatal("deveria exigir jwksURL")
	}
}

// ── OIDCAuthenticator ────────────────────────────────────────────────

func TestOIDCAuthenticator(t *testing.T) {
	issuer := newTestIssuer(t)
	authenticator, err := NewOIDCAuthenticator(context.Background(), VerifierConfig{
		Issuer:   testIssuerURL,
		JWKSURL:  issuer.jwksURL(),
		Audience: "wager-api",
	})
	if err != nil {
		t.Fatalf("NewOIDCAuthenticator: %v", err)
	}

	t.Run("token valido devolve identidade", func(t *testing.T) {
		identity, err := authenticator.Authenticate(context.Background(),
			"Bearer "+issuer.sign(t, issuer.claims(nil)))
		if err != nil {
			t.Fatalf("erro inesperado: %v", err)
		}
		if identity.ProviderID != "provider-a" || !identity.HasRole(ports.RoleProvider) {
			t.Fatalf("identidade = %+v", identity)
		}
	})

	t.Run("servico interno sem providerId", func(t *testing.T) {
		token := issuer.sign(t, issuer.claims(map[string]any{
			"sub":          "service-account-internal",
			"providerId":   "",
			"realm_access": map[string]any{"roles": []string{ports.RoleInternal}},
		}))
		identity, err := authenticator.Authenticate(context.Background(), "Bearer "+token)
		if err != nil {
			t.Fatalf("erro inesperado: %v", err)
		}
		if identity.ProviderID != "" || !identity.HasRole(ports.RoleInternal) {
			t.Fatalf("identidade = %+v", identity)
		}
	})

	t.Run("header ausente", func(t *testing.T) {
		if _, err := authenticator.Authenticate(context.Background(), ""); !errors.Is(err, ports.ErrMissingCredentials) {
			t.Fatalf("erro = %v, quer ErrMissingCredentials", err)
		}
	})

	t.Run("esquema nao suportado", func(t *testing.T) {
		if _, err := authenticator.Authenticate(context.Background(), "Basic abc"); !errors.Is(err, ports.ErrInvalidCredentials) {
			t.Fatalf("erro = %v, quer ErrInvalidCredentials", err)
		}
	})

	t.Run("bearer vazio", func(t *testing.T) {
		if _, err := authenticator.Authenticate(context.Background(), "Bearer "); !errors.Is(err, ports.ErrInvalidCredentials) {
			t.Fatalf("erro = %v, quer ErrInvalidCredentials", err)
		}
	})

	t.Run("provedor sem claim providerId", func(t *testing.T) {
		token := issuer.sign(t, issuer.claims(map[string]any{"providerId": ""}))
		_, err := authenticator.Authenticate(context.Background(), "Bearer "+token)
		if !errors.Is(err, ports.ErrInvalidCredentials) {
			t.Fatalf("erro = %v, quer ErrInvalidCredentials", err)
		}
	})
}

// ── espera paciente no startup ───────────────────────────────────────

func TestNewVerifierWaitsForJWKSToBecomeAvailable(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gerando chave RSA: %v", err)
	}

	var ready atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/certs", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "ainda subindo", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]string{jwkEntry(key, "chave-1")},
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	// O IdP só fica pronto depois de 200ms.
	go func() {
		time.Sleep(200 * time.Millisecond)
		ready.Store(true)
	}()

	verifier, err := NewVerifier(context.Background(), VerifierConfig{
		Issuer:               testIssuerURL,
		JWKSURL:              server.URL + "/certs",
		StartupTimeout:       10 * time.Second,
		StartupRetryInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("deveria aguardar o JWKS ficar disponível: %v", err)
	}
	if verifier == nil {
		t.Fatal("verifier não deveria ser nulo")
	}
}

func TestNewVerifierGivesUpAfterStartupTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "sempre indisponível", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := NewVerifier(context.Background(), VerifierConfig{
		Issuer:               testIssuerURL,
		JWKSURL:              server.URL + "/certs",
		StartupTimeout:       300 * time.Millisecond,
		StartupRetryInterval: 50 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("deveria desistir após o prazo de startup (fail-fast)")
	}
}
