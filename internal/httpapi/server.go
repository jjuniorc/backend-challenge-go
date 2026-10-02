package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server embrulha o http.Server e expõe Start/Stop para o fx.Lifecycle.
type Server struct {
	httpServer      *http.Server
	listener        net.Listener
	logger          *slog.Logger
	shutdownTimeout time.Duration
}

// ServerDeps reúne as dependências do servidor.
type ServerDeps struct {
	Handler         http.Handler
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
}

// NewServer cria o servidor HTTP.
func NewServer(deps ServerDeps) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:              deps.Addr,
			Handler:           deps.Handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       deps.ReadTimeout,
			WriteTimeout:      deps.WriteTimeout,
			IdleTimeout:       60 * time.Second,
		},
		logger:          deps.Logger,
		shutdownTimeout: deps.ShutdownTimeout,
	}
}

// Start abre o listener de forma síncrona — assim uma porta ocupada falha o
// início da aplicação em vez de falhar silenciosamente numa goroutine.
func (s *Server) Start(context.Context) error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("abrindo listener HTTP em %q: %w", s.httpServer.Addr, err)
	}
	s.listener = listener

	go func() {
		if err := s.httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("servidor HTTP encerrou com erro", "err", err)
		}
	}()
	return nil
}

// Stop encerra o servidor aguardando as requisições em andamento dentro do prazo.
func (s *Server) Stop(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("encerrando servidor HTTP: %w", err)
	}
	return nil
}

// Addr devolve o endereço efetivamente escutado (útil quando a porta é 0).
func (s *Server) Addr() string {
	if s.listener == nil {
		return s.httpServer.Addr
	}
	return s.listener.Addr().String()
}
