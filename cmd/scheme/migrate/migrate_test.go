package migrate_test

import (
  "context"
  "io"
  "log/slog"
  "os"
  "path/filepath"
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/cmd/scheme/migrate"
)

func TestRun(t *testing.T) {
  ctx := context.Background()
  logger := slog.New(slog.NewTextHandler(io.Discard, nil))
  schemaPath, _ := filepath.Abs(
    "../../../test/testdata/kitchen_sink.textproto",
  )

  t.Run("panics on nil logger", func(t *testing.T) {
    assert.Panics(t, func() {
      migrate.Run(ctx, nil, []string{})
    }, "expected panic on nil logger")
  })

  t.Run("returns 0 on help flag", func(t *testing.T) {
    code := migrate.Run(ctx, logger, []string{"-h"})
    assert.Equal(t, 0, code, "expected exit code 0")
  })

  t.Run("returns 1 when -in is missing", func(t *testing.T) {
    code := migrate.Run(ctx, logger, []string{})
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 when -db-uri is missing", func(t *testing.T) {
    code := migrate.Run(ctx, logger, []string{"-in", schemaPath})
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 on invalid schema path", func(t *testing.T) {
    code := migrate.Run(ctx, logger, []string{
      "-in", "invalid.textproto",
      "-db-uri", "postgres://user:pass@localhost:5432/db",
    })
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 on invalid textproto format", func(t *testing.T) {
    tmpFile := filepath.Join(t.TempDir(), "bad.textproto")
    err := os.WriteFile(tmpFile, []byte("invalid content"), 0644)
    require.NoError(t, err)

    code := migrate.Run(ctx, logger, []string{
      "-in", tmpFile,
      "-db-uri", "postgres://user:pass@localhost:5432/db",
    })
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("returns 1 when database connection fails", func(t *testing.T) {
    // Valid schema, but connection will fail
    code := migrate.Run(ctx, logger, []string{
      "-in", schemaPath,
      "-db-uri",
      "postgres://invalid:user@127.0.0.1:0/nonexistent?sslmode=disable" +
        "&connect_timeout=1",
    })
    assert.Equal(t, 1, code, "expected exit code 1")
  })
}
