package eject

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/uthereal/scheme/spec"
)

// Run executes the 'eject' command to unpack the embedded protobuf definitions.
func Run(_ context.Context, logger *slog.Logger, args []string) int {
	if logger == nil {
		panic("logger cannot be nil")
	}

	flags := flag.NewFlagSet("eject", flag.ContinueOnError)
	outDir := flags.String(
		"out-dir",
		".scheme",
		"(optional) directory to write the generated schema definitions",
	)

	flags.SetOutput(os.Stderr)
	err := flags.Parse(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}

	if *outDir == "" {
		logger.Error("The -out-dir flag cannot be empty.")
		fmt.Println()
		flags.Usage()
		return 1
	}

	err = ejectProtos(logger, *outDir)
	if err != nil {
		logger.Error(
			"Failed to eject the schema definitions.",
			slog.Any("error", err),
		)
		return 1
	}

	return 0
}

func ejectProtos(logger *slog.Logger, outDir string) error {
	logger.Info(
		"Ejecting embedded schema definitions.",
		slog.String("output", outDir),
	)

	err := fs.WalkDir(
		spec.FS, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			// Skip the root or any go files
			if path == "." || filepath.Ext(path) == ".go" {
				return nil
			}

			targetPath := filepath.Join(outDir, path)

			if d.IsDir() {
				err := os.MkdirAll(targetPath, 0755)
				if err != nil {
					return fmt.Errorf("failed to create directory %q -> %w", targetPath, err)
				}
				return nil
			}

			logger.Info("Writing schema file.", slog.String("file", targetPath))

			in, err := spec.FS.Open(path)
			if err != nil {
				return fmt.Errorf("failed to open embedded file %q -> %w", path, err)
			}
			defer func(in fs.File) { _ = in.Close() }(in)

			out, err := os.Create(targetPath)
			if err != nil {
				return fmt.Errorf("failed to create target file %q -> %w", targetPath, err)
			}
			defer func(out *os.File) { _ = out.Close() }(out)

			_, err = io.Copy(out, in)
			if err != nil {
				return fmt.Errorf("failed to copy file contents %q -> %w", path, err)
			}

			return nil
		})

	if err != nil {
		return fmt.Errorf("failed to walk embedded file system -> %w", err)
	}

	logger.Info("Successfully ejected all schema definitions.")
	return nil
}
