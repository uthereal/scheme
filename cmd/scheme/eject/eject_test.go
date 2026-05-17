package eject_test

import (
  "context"
  "io"
  "log/slog"
  "os"
  "path/filepath"
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/cmd/scheme/eject"
)

func TestRun(t *testing.T) {
  ctx := context.Background()
  logger := slog.New(slog.NewTextHandler(io.Discard, nil))

  t.Run("panics on nil logger", func(t *testing.T) {
    assert.Panics(t, func() {
      eject.Run(ctx, nil, []string{})
    }, "expected panic on nil logger")
  })

  t.Run("returns 0 on help flag", func(t *testing.T) {
    code := eject.Run(ctx, logger, []string{"-h"})
    assert.Equal(t, 0, code, "expected exit code 0")
  })

  t.Run("returns 1 on empty out-dir", func(t *testing.T) {
    code := eject.Run(ctx, logger, []string{"-out-dir", ""})
    assert.Equal(t, 1, code, "expected exit code 1")
  })

  t.Run("successfully ejects protos", func(t *testing.T) {
    outDir := filepath.Join(t.TempDir(), "protos")
    code := eject.Run(ctx, logger, []string{"-out-dir", outDir})
    assert.Equal(t, 0, code, "expected exit code 0")

    // Verify at least one proto file was written
    fileInfo, err := os.Stat(filepath.Join(outDir, "scheme.proto"))
    require.NoError(t, err, "expected scheme.proto to exist")
    assert.NotEmpty(t, fileInfo.Size(), "expected scheme.proto to not be empty")
  })
}
