package eject_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/uthereal/scheme/cmd/scheme/eject"
)

func TestRun(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("panics on nil logger", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic on nil logger")
			}
		}()
		eject.Run(ctx, nil, []string{})
	})

	t.Run("returns 0 on help flag", func(t *testing.T) {
		code := eject.Run(ctx, logger, []string{"-h"})
		if code != 0 {
			t.Errorf("expected exit code 0, got %d", code)
		}
	})

	t.Run("returns 1 on empty out-dir", func(t *testing.T) {
		code := eject.Run(ctx, logger, []string{"-out-dir", ""})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("successfully ejects protos", func(t *testing.T) {
		outDir := filepath.Join(t.TempDir(), "protos")
		code := eject.Run(ctx, logger, []string{"-out-dir", outDir})
		if code != 0 {
			t.Errorf("expected exit code 0, got %d", code)
		}

		// Verify at least one proto file was written
		fileInfo, err := os.Stat(filepath.Join(outDir, "scheme.proto"))
		if err != nil {
			t.Errorf("expected scheme.proto to exist: %v", err)
		}
		if fileInfo.Size() == 0 {
			t.Errorf("expected scheme.proto to not be empty")
		}
	})
}
