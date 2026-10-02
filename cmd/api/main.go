// Command api sobe a aplicação.
//
// Toda a composição (configuração, pool, repositórios, casos de uso, handlers e
// servidor) é feita pelo Uber Fx em internal/app. Este arquivo apenas inicia o
// ciclo de vida e trata o resultado.
package main

import (
	"log/slog"
	"os"

	"go.uber.org/fx"

	"github.com/jjuniorc/backend-challenge-go/internal/app"
)

func main() {
	if err := run(); err != nil {
		slog.Error("aplicação encerrada com erro", "err", err)
		os.Exit(1)
	}
}

func run() error {
	application := fx.New(app.Module)

	// Run inicia os componentes, bloqueia até receber SIGINT/SIGTERM e então
	// encerra na ordem inversa de inicialização.
	application.Run()

	if err := application.Err(); err != nil {
		return err
	}
	slog.Info("aplicação encerrada com sucesso")
	return nil
}
