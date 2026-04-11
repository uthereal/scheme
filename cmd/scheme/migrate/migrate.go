package migrate

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uthereal/scheme/genproto"
	"github.com/uthereal/scheme/genproto/core"

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
		"dry-run",
		false,
		"(optional) print actions without applying",
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
		logger.ErrorContext(ctx, "The -in flag is required.")
		fmt.Println()
		fs.Usage()
		return 1
	}

	if *dbUri == "" {
		logger.ErrorContext(ctx, "The -db-uri flag is required.")
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

	for _, dbDef := range scheme.GetDatabases() {
		switch dbDef.Engine.(type) {
		case *core.Database_Postgres:
			err := migratePostgres(
				ctx, logger, dbDef, *dbUri, *dryRun, *force,
			)
			if err != nil {
				logger.ErrorContext(
					ctx,
					"Failed to migrate database.",
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
		return errors.New(
			"database does not contain a postgres block",
		)
	}

	logger.InfoContext(
		ctx,
		"Connecting to the database.",
		slog.String("target", dbDef.GetName()),
	)

	dbConn, err := sql.Open("pgx", dbUri)
	if err != nil {
		return fmt.Errorf(
			"failed to parse connection URI -> %w",
			err,
		)
	}
	defer func(dbConn *sql.DB) {
		_ = dbConn.Close()
	}(dbConn)

	err = dbConn.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping the database -> %w", err)
	}

	logger.InfoContext(ctx, "Inspecting the live database state.")
	liveState, err := postgres.NewDatabaseStateFromDb(ctx, dbConn)
	if err != nil {
		return fmt.Errorf(
			"failed to inspect the live database -> %w",
			err,
		)
	}

	logger.InfoContext(ctx, "Calculating structural diffs.")
	targetState, err := postgres.NewDatabaseStateFromProto(pgDatabase)
	if err != nil {
		return fmt.Errorf("failed to map target schema -> %w", err)
	}

	actions, err := postgres.ComputeDiff(liveState, targetState)
	if err != nil {
		return fmt.Errorf("failed to plan migration -> %w", err)
	}

	if len(actions) == 0 {
		logger.InfoContext(ctx, "The database is completely up to date.")
		return nil
	}

	var hasDestructive bool
	for i, action := range actions {
		logger.InfoContext(
			ctx,
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
		logger.InfoContext(ctx, "Dry-run complete. No changes were applied.")
		return nil
	}

	if hasDestructive {
		if !force {
			return errors.New(
				"destructive actions; re-run with -force",
			)
		}
		logger.WarnContext(ctx, "Applying destructive changes to the database.")
	}

	logger.InfoContext(ctx, "Applying migration actions transactionally.")
	err = postgres.Apply(ctx, dbConn, actions)
	if err != nil {
		return fmt.Errorf("failed to apply the migrations -> %w", err)
	}

	logger.InfoContext(ctx, "Database successfully migrated.")
	return nil
}
