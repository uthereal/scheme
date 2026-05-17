package proto

import "embed"

//go:embed *.proto
//go:embed core/*.proto
//go:embed postgres/*.proto
//go:embed core/shared/*.proto
var FS embed.FS
