// Package schema embeds the SQL migration set so goose can apply it from a
// compiled binary, independent of the working directory.
package schema

import "embed"

// Migrations holds every numbered goose migration under schema/migrations/.
//
//go:embed migrations/*.sql
var Migrations embed.FS
