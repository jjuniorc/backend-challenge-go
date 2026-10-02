package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/jjuniorc/backend-challenge-go/internal/ports"
)

func TestNewDevAuthenticatorRejectsNonLocalEnvironments(t *testing.T) {
	if _, err := NewDevAuthenticator("production"); err == nil {
		t.Fatal("deveria recusar construção fora dos ambientes locais")
	}
	for _, env := range []string{"local", "dev", "test"} {
		if _, err := NewDevAuthenticator(env); err != nil {
			t.Fatalf("ambiente %q deveria ser aceito: %v", env, err)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	a, err := NewDevAuthenticator("test")
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	tests := []struct {
		name      string
		header    string
		wantErr   error
		wantSub   string
		wantProv  string
		wantRoles []string
	}{
		{
			name: "provedor", header: "Bearer dev:svc-provider-a:provider-a:provider",
			wantSub: "svc-provider-a", wantProv: "provider-a", wantRoles: []string{ports.RoleProvider},
		},
		{
			name: "interno com providerId vazio", header: "Bearer dev:svc-internal:-:internal",
			wantSub: "svc-internal", wantProv: "", wantRoles: []string{ports.RoleInternal},
		},
		{
			name: "interno com providerId vazio literal", header: "Bearer dev:svc-internal::internal",
			wantSub: "svc-internal", wantProv: "", wantRoles: []string{ports.RoleInternal},
		},
		{
			name: "multiplos papeis", header: "Bearer dev:svc:provider-a:provider,internal",
			wantSub: "svc", wantProv: "provider-a", wantRoles: []string{"provider", "internal"},
		},
		{name: "header ausente", header: "", wantErr: ports.ErrMissingCredentials},
		{name: "sem bearer", header: "dev:svc:provider-a:provider", wantErr: ports.ErrInvalidCredentials},
		{name: "prefixo errado", header: "Bearer abc", wantErr: ports.ErrInvalidCredentials},
		{name: "partes insuficientes", header: "Bearer dev:svc:provider-a", wantErr: ports.ErrInvalidCredentials},
		{name: "subject vazio", header: "Bearer dev::provider-a:provider", wantErr: ports.ErrInvalidCredentials},
		{name: "sem papeis", header: "Bearer dev:svc:provider-a:", wantErr: ports.ErrInvalidCredentials},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			identity, err := a.Authenticate(context.Background(), tc.header)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("erro = %v, quer %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("erro inesperado: %v", err)
			}
			if identity.Subject != tc.wantSub || identity.ProviderID != tc.wantProv {
				t.Fatalf("identidade = %+v", identity)
			}
			if len(identity.Roles) != len(tc.wantRoles) {
				t.Fatalf("papeis = %v, quer %v", identity.Roles, tc.wantRoles)
			}
			for i, role := range tc.wantRoles {
				if identity.Roles[i] != role {
					t.Fatalf("papeis = %v, quer %v", identity.Roles, tc.wantRoles)
				}
			}
		})
	}
}

func TestIdentityHasRole(t *testing.T) {
	id := ports.Identity{Roles: []string{ports.RoleProvider}}
	if !id.HasRole(ports.RoleProvider) {
		t.Fatal("deveria ter o papel provider")
	}
	if id.HasRole(ports.RoleInternal) {
		t.Fatal("não deveria ter o papel internal")
	}
}
