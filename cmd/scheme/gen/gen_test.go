package gen_test

import (
  "context"
  "io"
  "log/slog"
  "os"
  "path/filepath"
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/cmd/scheme/gen"
)

func createTempSchema(t *testing.T, withGo bool) string {
  content := ""
  if withGo {
    content = `
go: {
  package_path: "github.com/foo/bar"
}
database: {
  name: "testdb"
  postgres: {
  }
}
`
  } else {
    content = `
database: {
  name: "testdb"
  postgres: {
  }
}
`
  }

  tmpFile := filepath.Join(t.TempDir(), "schema.textproto")
  err := os.WriteFile(tmpFile, []byte(content), 0644)
  require.NoError(t, err)
  return tmpFile
}

func TestRun(t *testing.T) {
  ctx := context.Background()
  logger := slog.New(slog.NewTextHandler(io.Discard, nil))
  schemaWithGoPath, _ := filepath.Abs(
    "../../../test/testdata/kitchen_sink.textproto",
  )
  schemaPath := createTempSchema(t, false)

  t.Run("panics on nil logger", func(t *testing.T) {
    assert.Panics(t, func() {
      gen.Run(ctx, nil, []string{})
    }, "expected panic on nil logger")
  })

  t.Run("returns 0 on help flag", func(t *testing.T) {
    code := gen.Run(ctx, logger, []string{"-h"})
    assert.Equal(t, 0, code, "expected exit code 0")
  })

  t.Run("returns 1 when -in is missing", func(t *testing.T) {
    code := gen.Run(ctx, logger, []string{})
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 when -out-dir is missing", func(t *testing.T) {
    code := gen.Run(ctx, logger, []string{"-in", schemaPath})
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 when -langs is missing", func(t *testing.T) {
    code := gen.Run(
      ctx,
      logger,
      []string{"-in", schemaPath, "-out-dir", "/tmp"},
    )
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run(
    "returns 1 when go block is missing from schema for go lang",
    func(t *testing.T) {
      code := gen.Run(ctx, logger, []string{
        "-in", schemaPath, "-out-dir", "/tmp",
        "-langs", "go",
      })
      assert.Equal(t, 1, code, "expected exit code 1")
    })

  t.Run("returns 1 on unsupported language", func(t *testing.T) {
    code := gen.Run(ctx, logger, []string{
      "-in", schemaPath, "-out-dir", "/tmp",
      "-langs", "ruby",
    })
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 on invalid schema path", func(t *testing.T) {
    code := gen.Run(ctx, logger, []string{
      "-in", "invalid.textproto",
      "-out-dir", "/tmp",
      "-langs", "go",
    })
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 on invalid textproto format", func(t *testing.T) {
    tmpFile := filepath.Join(t.TempDir(), "bad.textproto")
    err := os.WriteFile(tmpFile, []byte("invalid content"), 0644)
    require.NoError(t, err)

    code := gen.Run(ctx, logger, []string{
      "-in", tmpFile,
      "-out-dir", "/tmp",
      "-langs", "go",
    })
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("successfully generates code", func(t *testing.T) {
    outDir := t.TempDir()
    code := gen.Run(ctx, logger, []string{
      "-in", schemaWithGoPath, "-out-dir", outDir,
      "-langs", "go",
    })
    assert.Equal(t, 0, code, "expected exit code 0")
  })
}
