package gen

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/ettle/strcase"
	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/model"
	"github.com/uthereal/scheme/gen/postgres/query"
	"github.com/uthereal/scheme/genproto"
	"github.com/uthereal/scheme/genproto/core"
	"google.golang.org/protobuf/encoding/prototext"
)

func Run(ctx context.Context, logger *slog.Logger, args []string) int {
	if logger == nil {
		panic("logger cannot be nil")
	}

	fs := flag.NewFlagSet("gen", flag.ContinueOnError)
	inPath := fs.String(
		"in",
		"",
		"(required) path to the textproto schema file",
	)
	outDir := fs.String(
		"out-dir",
		"",
		"(required) directory to write the generated code",
	)
	langsFlag := fs.String(
		"langs",
		"",
		"(required) list of target languages (e.g. go,kotlin,c)",
	)
	goPkgPrefix := fs.String(
		"go-pkg-prefix",
		"",
		"(required lang(go)) Go package prefix (e.g. github.com/myorg/gen)",
	)

	fs.SetOutput(os.Stderr)
	err := fs.Parse(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}

	if *inPath == "" {
		logger.ErrorContext(ctx, "The -in flag is required.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	if *outDir == "" {
		logger.ErrorContext(ctx, "The -out-dir flag is required.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	if *langsFlag == "" {
		logger.ErrorContext(ctx, "The -langs flag is required.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	data, err := os.ReadFile(*inPath)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"Failed to read the schema file.",
			slog.String("path", *inPath),
			slog.Any("error", err),
		)
		return 1
	}

	scheme := &genproto.Scheme{}
	err = prototext.Unmarshal(data, scheme)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"Failed to parse the textproto schema.",
			slog.Any("error", err),
		)
		return 1
	}

	rawLangs := strings.Split(*langsFlag, ",")
	var langs []gen.Language
	for _, l := range rawLangs {
		trimmed := strings.ToLower(strings.TrimSpace(l))
		switch trimmed {
		case "go":
			if *goPkgPrefix == "" {
				logger.ErrorContext(
					ctx,
					"The -go-pkg-prefix flag is required for Go generation.",
				)
				fmt.Println()
				fs.Usage()
				return 1
			}
			lang := gen.LangGo
			lang.Options.GoPackagePath = *goPkgPrefix
			langs = append(langs, lang)
		case "":
			continue
		default:
			logger.ErrorContext(
				ctx,
				"Unsupported target language.",
				slog.String("language", trimmed),
			)
			return 1
		}
	}

	if len(langs) == 0 {
		logger.ErrorContext(ctx, "At least one language target must be provided.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	for _, dbDef := range scheme.GetDatabases() {
		switch dbDef.Engine.(type) {
		case *core.Database_Postgres:
			err = generatePostgres(ctx, logger, dbDef, langs, *outDir)
			if err != nil {
				logger.ErrorContext(
					ctx,
					"Failed to generate the postgres database.",
					slog.Any("error", err),
				)
				return 1
			}
		default:
			logger.ErrorContext(
				ctx,
				"An unsupported database engine was provided in the schema.",
			)
			return 1
		}
	}

	return 0
}

func generatePostgres(
	ctx context.Context,
	logger *slog.Logger,
	dbDef *core.Database,
	langs []gen.Language,
	outDir string,
) error {
	if logger == nil {
		panic("logger cannot be nil")
	}
	if dbDef == nil {
		panic("database cannot be nil")
	}

	engine := "postgres"
	dbName := strcase.ToSnake(dbDef.GetName())
	if dbName == "" {
		return errors.New("database name cannot be empty")
	}
	finalDir := filepath.Join(outDir, engine, dbName)

	err := os.MkdirAll(finalDir, 0755)
	if err != nil {
		return fmt.Errorf(
			"failed to create output directory %q -> %w",
			finalDir, err,
		)
	}

	for _, lang := range langs {
		ext := lang.Extension

		modelCode, err := model.GenerateModels(dbDef, lang)
		if err != nil {
			return fmt.Errorf(
				"failed to generate models for %s -> %w",
				lang.Name, err,
			)
		}

		modelPath := filepath.Join(finalDir, "model."+ext)
		err = os.WriteFile(modelPath, []byte(modelCode), 0644)
		if err != nil {
			return fmt.Errorf(
				"failed to write output file %q -> %w",
				modelPath, err,
			)
		}
		logger.InfoContext(
			ctx,
			"Generated model output file.",
			slog.String("path", modelPath),
		)

		queryCode, err := query.GenerateQueryBuilders(dbDef, lang)
		if err != nil {
			return fmt.Errorf(
				"failed to generate queries for %s -> %w",
				lang.Name, err,
			)
		}

		queryPath := filepath.Join(finalDir, "query."+ext)
		err = os.WriteFile(queryPath, []byte(queryCode), 0644)
		if err != nil {
			return fmt.Errorf(
				"failed to write query output file %q -> %w",
				queryPath, err,
			)
		}
		logger.InfoContext(
			ctx,
			"Generated query output file.",
			slog.String("path", queryPath),
		)
	}
	return nil
}
