// Command migrate aplica e reverte as migrations versionadas do schema.
//
// Uso:
//
//	migrate up          aplica todas as migrations pendentes
//	migrate down        reverte TODAS as migrations aplicadas
//	migrate version     mostra a versão atual do schema
//	migrate steps -n 2  aplica (n > 0) ou reverte (n < 0) n migrations
//	migrate goto -n 1   vai para a versão informada
//	migrate force -n 1  força a versão atual (usado para resolver estado "dirty")
//	migrate drop        remove todas as tabelas do schema atual (destrutivo)
//
// Flag:
//
//	-dsn  DSN do PostgreSQL (default: variável de ambiente POSTGRES_DSN)
//
// Exemplos:
//
//	go run ./cmd/migrate up
//	go run ./cmd/migrate up -dsn 'postgres://wager:wager@localhost:5432/wager?sslmode=disable'
//	go run ./cmd/migrate down
package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/jjuniorc/backend-challenge-go/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("comando ausente")
	}
	cmd := args[0]

	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	dsn := fs.String("dsn", os.Getenv("POSTGRES_DSN"), "DSN do PostgreSQL (default: $POSTGRES_DSN)")
	n := fs.Int("n", 0, "número de migrations (usado por steps/goto/force)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	rest := fs.Args()

	if *dsn == "" {
		return errors.New("DSN ausente: informe -dsn ou defina POSTGRES_DSN")
	}

	m, err := newMigrator(*dsn)
	if err != nil {
		return err
	}
	defer func() {
		// Fecha source e conexão. O erro de fechamento não altera o resultado
		// da migration, então é apenas reportado.
		srcErr, dbErr := m.Close()
		if srcErr != nil {
			fmt.Fprintln(os.Stderr, "migrate: fechando fonte:", srcErr)
		}
		if dbErr != nil {
			fmt.Fprintln(os.Stderr, "migrate: fechando banco:", dbErr)
		}
	}()

	switch cmd {
	case "up":
		if err := m.Up(); !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		fmt.Println("migrate: nenhuma migration pendente")

	case "down":
		if err := m.Down(); !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		fmt.Println("migrate: nada a reverter")

	case "steps":
		if len(rest) > 0 {
			return fmt.Errorf("steps não aceita argumentos posicionais: use -n (recebido %q)", rest[0])
		}
		if *n == 0 {
			return errors.New("steps exige -n diferente de zero")
		}
		if err := m.Steps(*n); !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		fmt.Printf("migrate: steps %d sem alteração\n", *n)

	case "goto":
		if *n <= 0 {
			return errors.New("goto exige -n > 0")
		}
		if err := m.Migrate(uint(*n)); !errors.Is(err, migrate.ErrNoChange) {
			return err
		}
		fmt.Printf("migrate: já na versão %d\n", *n)

	case "force":
		if *n < 0 {
			return errors.New("force exige -n >= 0")
		}
		if err := m.Force(*n); err != nil {
			return err
		}
		fmt.Printf("migrate: versão forçada para %d\n", *n)

	case "version":
		v, dirty, err := m.Version()
		switch {
		case errors.Is(err, migrate.ErrNilVersion):
			fmt.Println("migrate: nenhuma migration aplicada")
			return nil
		case err != nil:
			return err
		}
		fmt.Printf("migrate: versão %d (dirty=%t)\n", v, dirty)

	case "drop":
		if err := m.Drop(); err != nil {
			return err
		}
		fmt.Println("migrate: schema removido")

	default:
		usage()
		return fmt.Errorf("comando desconhecido: %q", cmd)
	}

	return nil
}

func newMigrator(dsn string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("abrindo fonte de migrations: %w", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		_ = src.Close()
		return nil, fmt.Errorf("abrindo conexão: %w", err)
	}

	// MultiStatement fica DESABILITADO de propósito: o corpo das migrations é
	// enviado como uma única consulta simples e, portanto, o PostgreSQL o
	// executa como uma transação implícita (tudo ou nada). Ativar o modo
	// multi-statement faria split ingênuo por ';' e quebraria o corpo das
	// funções PL/pgSQL delimitado por $$.
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		_ = db.Close()
		_ = src.Close()
		return nil, fmt.Errorf("inicializando driver de migrations: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		_ = db.Close()
		_ = src.Close()
		return nil, fmt.Errorf("criando migrator: %w", err)
	}
	return m, nil
}

func usage() {
	fmt.Fprint(os.Stderr, `migrate — aplica e reverte as migrations versionadas

Comandos:
  up               aplica todas as migrations pendentes
  down             reverte TODAS as migrations aplicadas
  version          mostra a versão atual do schema
  steps -n N       aplica (N > 0) ou reverte (N < 0) N migrations
  goto -n V        vai para a versão V
  force -n V       força a versão V (resolve estado dirty)
  drop             remove todas as tabelas do schema atual (destrutivo)

Flags:
  -dsn string      DSN do PostgreSQL (default: $POSTGRES_DSN)

Exemplos:
  go run ./cmd/migrate up
  go run ./cmd/migrate down
  go run ./cmd/migrate version

`)
}
