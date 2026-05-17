package gen

import (
  "context"
  "flag"
  "fmt"
  "log/slog"
  "os"
  "path/filepath"
  "strings"

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
  inPath := fs.String("in", "", "The path to the scheme configuration file.")
  outDir := fs.String("out-dir", "",
    "The root directory where the code should be generated.")
  langsStr := fs.String("langs", "go",
    "Comma-separated list of target languages.")

  err := fs.Parse(args)
  if err != nil {
    if err == flag.ErrHelp {
      return 0
    }
    return 1
  }

  if *inPath == "" || *outDir == "" || *langsStr == "" {
    fs.Usage()
    return 1
  }

  data, err := os.ReadFile(*inPath)
  if err != nil {
    logger.ErrorContext(
      ctx,
      "Failed to read the scheme configuration file.",
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

  langs := make([]gen.Language, 0)
  for _, l := range strings.Split(*langsStr, ",") {
    trimmed := strings.TrimSpace(l)
    if trimmed == "" {
      continue
    }

    if strings.ToLower(trimmed) == "go" {
      langs = append(langs, gen.LangGo)
    } else {
      logger.ErrorContext(
        ctx,
        "Unsupported language target provided.",
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

  dbDef := scheme.GetDatabase()
  if dbDef == nil {
    logger.ErrorContext(ctx, "The scheme does not contain a database definition.")
    return 1
  }

  switch dbDef.Engine.(type) {
  case *core.Database_Postgres:
    err = generatePostgres(ctx, logger, scheme, dbDef, langs, *outDir)
    if err != nil {
      logger.ErrorContext(
        ctx,
        "Failed to generate the postgres code.",
        slog.Any("error", err),
      )
      return 1
    }
  default:
    logger.ErrorContext(
      ctx,
      "Unsupported engine provided in the schema.",
    )
    return 1
  }

  return 0
}

func generatePostgres(
  ctx context.Context,
  logger *slog.Logger,
  schemeDef *genproto.Scheme,
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

  err := os.MkdirAll(outDir, 0755)
  if err != nil {
    return fmt.Errorf(
      "failed to create output directory %q -> %w",
      outDir, err,
    )
  }

  for _, lang := range langs {
    ext := lang.Extension

    modelCode, err := model.GenerateModels(dbDef, lang, schemeDef)
    if err != nil {
      return fmt.Errorf(
        "failed to generate models for %s -> %w",
        lang.Name, err,
      )
    }

    modelPath := filepath.Join(outDir, "model."+ext)
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

    queryCode, err := query.GenerateQueryBuilders(dbDef, lang, schemeDef)
    if err != nil {
      return fmt.Errorf(
        "failed to generate queries for %s -> %w",
        lang.Name, err,
      )
    }

    queryPath := filepath.Join(outDir, "query."+ext)
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
