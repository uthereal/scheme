package migrate_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/uthereal/scheme/cmd/scheme/migrate"
)

func TestRun(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	schemaPath, _ := filepath.Abs("../../../test/testdata/kitchen_sink.textproto")

	t.Run("panics on nil logger", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Error("expected panic on nil logger")
			}
		}()
		migrate.Run(ctx, nil, []string{})
	})

	t.Run("returns 0 on help flag", func(t *testing.T) {
		code := migrate.Run(ctx, logger, []string{"-h"})
		if code != 0 {
			t.Errorf("expected exit code 0, got %d", code)
		}
	})

	t.Run("returns 1 when -in is missing", func(t *testing.T) {
		code := migrate.Run(ctx, logger, []string{})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 when -db-uri is missing", func(t *testing.T) {
		code := migrate.Run(ctx, logger, []string{"-in", schemaPath})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 on invalid schema path", func(t *testing.T) {
		code := migrate.Run(ctx, logger, []string{
			"-in", "invalid.textproto",
			"-db-uri", "postgres://user:pass@localhost:5432/db",
		})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 on invalid textproto format", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "bad.textproto")
		os.WriteFile(tmpFile, []byte("invalid content"), 0644)
		code := migrate.Run(ctx, logger, []string{
			"-in", tmpFile,
			"-db-uri", "postgres://user:pass@localhost:5432/db",
		})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 when database connection fails", func(t *testing.T) {
		// Valid schema, but connection will fail
		code := migrate.Run(ctx, logger, []string{
			"-in", schemaPath,
			"-db-uri",
			"postgres://invalid:user@127.0.0.1:0/nonexistent?sslmode=disable" +
				"&connect_timeout=1",
		})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})
}
