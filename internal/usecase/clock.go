// Package usecase orquestra os casos de uso sobre as interfaces de ports.
//
// O pacote não conhece pgx, SQL, SQS nem HTTP. Toda dependência de tempo e de
// geração de identidade é injetada, o que torna o comportamento determinístico
// em teste e explícito em produção.
package usecase

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// SystemClock é a implementação de ports.Clock usada em produção.
type SystemClock struct{}

// Now devolve o instante atual em UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// UUIDv7 implementa ports.IDGenerator com UUID versão 7.
//
// UUID v7 é ordenável por tempo, o que melhora a localidade de índice em
// comparação ao v4. É também o formato que aparece nos exemplos do README.
type UUIDv7 struct{}

// NewID gera um UUID v7 em formato canônico (36 caracteres).
func (UUIDv7) NewID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("gerando UUID v7: %w", err)
	}
	return id.String(), nil
}
