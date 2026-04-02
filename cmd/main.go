package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/uthereal/scheme/cmd/eject"
	"github.com/uthereal/scheme/cmd/gen"
	"github.com/uthereal/scheme/cmd/migrate"
)

func printHelp() {
	fmt.Println(`Scheme CLI - Database Schema to Code Generator

Usage:
  scheme <command> [options]

Commands:
  eject     Extract embedded protobuf schema definitions for IDE autocomplete
  gen       Generate code from a textproto schema file
  migrate   Diff and apply a textproto schema against an active database
  help      Show this help message

Run 'scheme <command> -h' for more information on a specific command.`)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	if len(os.Args) < 2 {
		logger.Error("Expected a subcommand (e.g., 'gen').")
		fmt.Println()
		printHelp()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "eject":
		os.Exit(eject.Run(ctx, logger, os.Args[2:]))
	case "gen":
		os.Exit(gen.Run(ctx, logger, os.Args[2:]))
	case "migrate":
		os.Exit(migrate.Run(ctx, logger, os.Args[2:]))
	case "help", "-h", "--help":
		printHelp()
		os.Exit(0)
	default:
		logger.Error("Unknown subcommand.", slog.String("subcommand", os.Args[1]))
		fmt.Println()
		printHelp()
		os.Exit(1)
	}
}
