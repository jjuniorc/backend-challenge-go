// Package migrations embute os arquivos SQL versionados na aplicação.
//
// Os arquivos seguem a convenção do golang-migrate:
// <versão>_<nome>.up.sql e <versão>_<nome>.down.sql.
//
// Embutir os arquivos torna a aplicação e a reversão das migrations
// reproduzíveis a partir de um checkout limpo, sem exigir binário externo
// (goose, migrate CLI) instalado na máquina.
package migrations

import "embed"

// FS contém os arquivos de migration na raiz do sistema de arquivos embutido.
//
//go:embed *.sql
var FS embed.FS
