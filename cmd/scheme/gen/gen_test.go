package gen_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/uthereal/scheme/cmd/scheme/gen"
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
		gen.Run(ctx, nil, []string{})
	})

	t.Run("returns 0 on help flag", func(t *testing.T) {
		code := gen.Run(ctx, logger, []string{"-h"})
		if code != 0 {
			t.Errorf("expected exit code 0, got %d", code)
		}
	})

	t.Run("returns 1 when -in is missing", func(t *testing.T) {
		code := gen.Run(ctx, logger, []string{})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 when -out-dir is missing", func(t *testing.T) {
		code := gen.Run(ctx, logger, []string{"-in", schemaPath})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 when -langs is missing", func(t *testing.T) {
		code := gen.Run(ctx, logger, []string{"-in", schemaPath, "-out-dir", "/tmp"})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run(
		"returns 1 when -go-pkg-prefix is missing for go lang",
		func(t *testing.T) {
			code := gen.Run(ctx, logger, []string{
				"-in", schemaPath,
				"-out-dir", "/tmp",
				"-langs", "go",
			})
			if code != 1 {
				t.Errorf("expected exit code 1, got %d", code)
			}
		})

	t.Run("returns 1 on unsupported language", func(t *testing.T) {
		code := gen.Run(ctx, logger, []string{
			"-in", schemaPath,
			"-out-dir", "/tmp",
			"-langs", "ruby",
		})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 on invalid schema path", func(t *testing.T) {
		code := gen.Run(ctx, logger, []string{
			"-in", "invalid.textproto",
			"-out-dir", "/tmp",
			"-langs", "go",
			"-go-pkg-prefix", "github.com/foo/bar",
		})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("returns 1 on invalid textproto format", func(t *testing.T) {
		tmpFile := filepath.Join(t.TempDir(), "bad.textproto")
		os.WriteFile(tmpFile, []byte("invalid content"), 0644)
		code := gen.Run(ctx, logger, []string{
			"-in", tmpFile,
			"-out-dir", "/tmp",
			"-langs", "go",
			"-go-pkg-prefix", "github.com/foo/bar",
		})
		if code != 1 {
			t.Errorf("expected exit code 1, got %d", code)
		}
	})

	t.Run("successfully generates code", func(t *testing.T) {
		outDir := t.TempDir()
		code := gen.Run(ctx, logger, []string{
			"-in", schemaPath,
			"-out-dir", outDir,
			"-langs", "go",
			"-go-pkg-prefix", "github.com/foo/bar",
		})
		if code != 0 {
			t.Errorf("expected exit code 0, got %d", code)
		}
	})
}
