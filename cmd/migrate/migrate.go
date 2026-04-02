package migrate

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/lib/pq"
	"github.com/uthereal/scheme/genproto/spec"
	"github.com/uthereal/scheme/genproto/spec/core"

	"github.com/uthereal/scheme/migrate/postgres"
	"google.golang.org/protobuf/encoding/prototext"
)

// Run executes the declarative migration sequence against an active database
// connection based on the provided textproto schema file.
func Run(ctx context.Context, logger *slog.Logger, args []string) int {
	if logger == nil {
		panic("logger cannot be nil")
	}

	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	inPath := fs.String(
		"in", "", "(required) path to the textproto schema file",
	)
	dbUri := fs.String(
		"db-uri", "", "(required) PostgreSQL connection URI",
	)
	dryRun := fs.Bool(
		"dry-run", false, "(optional) print actions without applying them",
	)
	force := fs.Bool(
		"force",
		false,
		"(optional) bypass confirmation for destructive actions",
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
		logger.Error("The -in flag is required.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	if *dbUri == "" {
		logger.Error("The -db-uri flag is required.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	data, err := os.ReadFile(*inPath)
	if err != nil {
		logger.Error(
			"Failed to read the schema file.",
			slog.String("path", *inPath),
			slog.Any("error", err),
		)
		return 1
	}

	scheme := &spec.Scheme{}
	err = prototext.Unmarshal(data, scheme)
	if err != nil {
		logger.Error(
			"Failed to parse the textproto schema.",
			slog.Any("error", err),
		)
		return 1
	}

	for _, dbDef := range scheme.GetDatabases() {
		switch dbDef.Engine.(type) {
		case *core.Database_Postgres:
			err := migratePostgres(
				ctx, logger, dbDef, *dbUri, *dryRun, *force,
			)
			if err != nil {
				logger.Error(
					"Failed to migrate database.",
					slog.Any("error", err),
				)
				return 1
			}
		default:
			logger.Error("An unsupported or missing database engine was provided in the schema.")
			return 1
		}
	}

	return 0
}

func migratePostgres(
	ctx context.Context,
	logger *slog.Logger,
	dbDef *core.Database,
	dbUri string,
	dryRun bool,
	force bool,
) error {
	if logger == nil {
		panic("logger cannot be nil")
	}
	if dbDef == nil {
		panic("database cannot be nil")
	}

	pgDatabase := dbDef.GetPostgres()
	if pgDatabase == nil {
		return fmt.Errorf("target database does not contain a postgres block")
	}

	logger.Info(
		"Connecting to the database.",
		slog.String("target", dbDef.GetName()),
	)

	dbConn, err := sql.Open("postgres", dbUri)
	if err != nil {
		return fmt.Errorf("failed to parse database connection URI -> %w", err)
	}
	defer func(dbConn *sql.DB) {
		_ = dbConn.Close()
	}(dbConn)

	err = dbConn.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping the database -> %w", err)
	}

	logger.Info("Inspecting the live database state.")
	liveState, err := postgres.Inspect(ctx, dbConn)
	if err != nil {
		return fmt.Errorf("failed to inspect the live database -> %w", err)
	}

	logger.Info("Calculating structural diffs.")
	differ, err := postgres.NewDiffer(liveState, pgDatabase)
	if err != nil {
		return fmt.Errorf("failed to initialize the differ -> %w", err)
	}

	err = differ.Plan()
	if err != nil {
		return fmt.Errorf("failed to calculate migration plan -> %w", err)
	}

	if len(differ.Actions) == 0 {
		logger.Info("The database is completely up to date.")
		return nil
	}

	var hasDestructive bool
	for i, action := range differ.Actions {
		logger.Info(
			"Migration action planned.",
			slog.Int("step", i+1),
			slog.String("sql", action.SQL),
			slog.Bool("destructive", action.IsDestructive),
		)
		if action.IsDestructive {
			hasDestructive = true
		}
	}

	if dryRun {
		logger.Info("Dry-run complete. No changes were applied.")
		return nil
	}

	if hasDestructive {
		if !force {
			return fmt.Errorf(
				"destructive actions planned; re-run with the -force flag to bypass",
			)
		}
		logger.Warn("Applying destructive changes to the database.")
	}

	logger.Info("Applying migration actions transactionally.")
	err = postgres.Apply(ctx, dbConn, differ.Actions)
	if err != nil {
		return fmt.Errorf("failed to apply the migrations -> %w", err)
	}

	logger.Info("Database successfully migrated.")
	return nil
}
