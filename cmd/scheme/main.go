package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/uthereal/scheme/cmd/scheme/eject"
	"github.com/uthereal/scheme/cmd/scheme/gen"
	"github.com/uthereal/scheme/cmd/scheme/migrate"
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
	os.Exit(run(os.Args))
}

func run(args []string) int {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if len(args) < 2 {
		logger.ErrorContext(ctx, "Expected a subcommand (e.g., 'gen').")
		fmt.Println()
		printHelp()
		return 1
	}

	exitCode := 0
	switch args[1] {
	case "eject":
		exitCode = eject.Run(ctx, logger, args[2:])
	case "gen":
		exitCode = gen.Run(ctx, logger, args[2:])
	case "migrate":
		exitCode = migrate.Run(ctx, logger, args[2:])
	case "help", "-h", "--help":
		printHelp()
	default:
		exitCode = 1
		logger.ErrorContext(
			ctx, "Unknown subcommand.", slog.String("subcommand", args[1]),
		)
		fmt.Println()
		printHelp()
	}

	return exitCode
}
