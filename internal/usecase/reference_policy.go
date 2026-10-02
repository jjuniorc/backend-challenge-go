package usecase

import "time"

// ReferencePolicy define quando uma operação deixa de esperar pela referência.
//
// O README exige: "Um worker deve tentar novamente com backoff exponencial,
// inclusive após reinicialização da aplicação. Defina um número máximo de
// tentativas ou TTL. Quando esgotado, finalize como REJECTED".
type ReferencePolicy struct {
	// MaxAttempts é o número máximo de tentativas de resolução.
	MaxAttempts int
	// TTL é o tempo máximo, desde a criação, em que a operação pode esperar.
	TTL time.Duration
	// BaseBackoff é o atraso da primeira retentativa.
	BaseBackoff time.Duration
	// MaxBackoff é o teto do atraso (evita espera ilimitada).
	MaxBackoff time.Duration
}

// DefaultReferencePolicy devolve os valores usados em produção.
func DefaultReferencePolicy() ReferencePolicy {
	return ReferencePolicy{
		MaxAttempts: 8,
		TTL:         24 * time.Hour,
		BaseBackoff: 5 * time.Second,
		MaxBackoff:  15 * time.Minute,
	}
}

// normalized preenche campos zerados com os defaults.
func (p ReferencePolicy) normalized() ReferencePolicy {
	d := DefaultReferencePolicy()
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}
	if p.TTL <= 0 {
		p.TTL = d.TTL
	}
	if p.BaseBackoff <= 0 {
		p.BaseBackoff = d.BaseBackoff
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = d.MaxBackoff
	}
	if p.MaxBackoff < p.BaseBackoff {
		p.MaxBackoff = p.BaseBackoff
	}
	return p
}

// Backoff devolve o atraso da tentativa informada (1-based), exponencial com teto.
//
//	1 -> 5s, 2 -> 10s, 3 -> 20s, ... até MaxBackoff (15m)
func (p ReferencePolicy) Backoff(attempt int) time.Duration {
	p = p.normalized()
	if attempt < 1 {
		attempt = 1
	}
	delay := p.BaseBackoff
	for i := 1; i < attempt; i++ {
		if delay >= p.MaxBackoff/2 {
			return p.MaxBackoff
		}
		delay *= 2
	}
	if delay > p.MaxBackoff {
		return p.MaxBackoff
	}
	return delay
}

// Exhausted informa se a espera pela referência acabou por tentativas ou por TTL.
func (p ReferencePolicy) Exhausted(attempts int, age time.Duration) bool {
	p = p.normalized()
	return attempts >= p.MaxAttempts || age >= p.TTL
}
