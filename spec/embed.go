package spec

import "embed"

//go:embed **/*.proto *.proto
var FS embed.FS
