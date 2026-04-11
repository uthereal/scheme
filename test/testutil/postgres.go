package testutil

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/uthereal/scheme/genproto"
	schemepg "github.com/uthereal/scheme/genproto/postgres"
	pgmigrate "github.com/uthereal/scheme/migrate/postgres"
)

const postgresImage = "postgres:16-alpine"
const sqlDriverName = "pgx"

// PostgresContainer is a Postgres test container.
type PostgresContainer struct {
	*pgmodule.PostgresContainer

	SqlDriverName  string
	dbNameTemplate *string
}

// StartPostgresContainer starts a PostgreSQL container using testcontainers.
func StartPostgresContainer(ctx context.Context) (*PostgresContainer, error) {
	pgContainer, err := pgmodule.Run(ctx,
		postgresImage,
		pgmodule.WithDatabase("testdb"),
		pgmodule.WithUsername("user"),
		pgmodule.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to start postgres container -> %w", err,
		)
	}

	return &PostgresContainer{
		PostgresContainer: pgContainer,
		SqlDriverName:     sqlDriverName,
	}, nil
}

// StopPostgresContainer stops a PostgreSQL container.
func StopPostgresContainer(pgContainer *PostgresContainer) error {
	if pgContainer == nil {
		return nil
	}
	return testcontainers.TerminateContainer(pgContainer)
}

// SetupTemplateDBWithSchema creates a template database and applies the given
// textproto schema.
func (c *PostgresContainer) SetupTemplateDBWithSchema(
	ctx context.Context, dbNameTemplate string, schemaPath string,
) error {
	if c == nil {
		return errors.New("postgres container receiver cannot be nil")
	}

	rawAdminConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf(
			"failed to get base admin connection string -> %w", err,
		)
	}

	adminURL, err := url.Parse(rawAdminConn)
	if err != nil {
		return fmt.Errorf("failed to parse connection string -> %w", err)
	}
	adminURL.Path = "/postgres"
	adminConn := adminURL.String()

	adminDB, err := sql.Open(c.SqlDriverName, adminConn)
	if err != nil {
		return fmt.Errorf("failed to open admin database -> %w", err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(adminDB)

	_, err = adminDB.ExecContext(
		ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbNameTemplate),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create template db %s -> %w", dbNameTemplate, err,
		)
	}

	rawTargetConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to get base connection string -> %w", err)
	}

	targetURL, err := url.Parse(rawTargetConn)
	if err != nil {
		return fmt.Errorf("failed to parse connection string -> %w", err)
	}
	targetURL.Path = "/" + dbNameTemplate
	targetConn := targetURL.String()

	targetDB, err := sql.Open(c.SqlDriverName, targetConn)
	if err != nil {
		return fmt.Errorf("failed to open target database -> %w", err)
	}
	defer func(db *sql.DB) {
		if db == nil {
			return
		}
		_ = db.Close()
	}(targetDB)

	b, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema -> %w", err)
	}

	var sc genproto.Scheme
	err = prototext.Unmarshal(b, &sc)
	if err != nil {
		return fmt.Errorf("failed to unmarshal schema -> %w", err)
	}

	var pgSchema *schemepg.PostgresDatabase
	for _, db := range sc.GetDatabases() {
		if db.GetName() == "postgres" && db.GetPostgres() != nil {
			pgSchema = db.GetPostgres()
			break
		}
	}

	if pgSchema == nil {
		return errors.New("no postgres database found in schema")
	}

	liveState, err := pgmigrate.NewDatabaseStateFromDb(ctx, targetDB)
	if err != nil {
		return fmt.Errorf("failed to inspect target db -> %w", err)
	}

	targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchema)
	if err != nil {
		return fmt.Errorf("failed to map target schema -> %w", err)
	}

	actions, err := pgmigrate.ComputeDiff(liveState, targetState)
	if err != nil {
		return fmt.Errorf("failed to plan migration -> %w", err)
	}

	for i, a := range actions {
		fmt.Printf("ACTION %d: %s\n", i, a.SQL)
	}

	err = pgmigrate.Apply(ctx, targetDB, actions)
	if err != nil {
		return fmt.Errorf("failed to apply migrations -> %w", err)
	}

	err = targetDB.Close()
	if err != nil {
		return fmt.Errorf("failed to close target db connection -> %w", err)
	}
	targetDB = nil

	_, err = adminDB.ExecContext(
		ctx,
		fmt.Sprintf(`ALTER DATABASE "%s" IS_TEMPLATE = true`, dbNameTemplate),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to set template db %s to template -> %w",
			dbNameTemplate, err,
		)
	}

	c.dbNameTemplate = new(string)
	*c.dbNameTemplate = dbNameTemplate
	return nil
}

// CreateIsolatedDB creates a new database cloned from the template.
func (c *PostgresContainer) CreateIsolatedDB(
	ctx context.Context, t *testing.T,
) (*sql.DB, func() error, error) {
	t.Helper()

	if c == nil {
		return nil, nil, errors.New(
			"postgres container receiver cannot be nil",
		)
	}

	if c.dbNameTemplate == nil {
		return nil, nil, errors.New(
			"template database not initialized -> call " +
				"SetupTemplateDBWithSchema first",
		)
	}

	hash := md5.Sum([]byte(t.Name() + time.Now().String()))
	dbName := fmt.Sprintf("test_db_%s", hex.EncodeToString(hash[:10]))

	rawAdminConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to get base admin connection string -> %w", err,
		)
	}

	adminURL, err := url.Parse(rawAdminConn)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to parse connection string -> %w", err,
		)
	}
	adminURL.Path = "/postgres"
	adminConn := adminURL.String()

	adminDB, err := sql.Open(c.SqlDriverName, adminConn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open admin db -> %w", err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(adminDB)

	cmd := fmt.Sprintf(
		`CREATE DATABASE "%s" WITH TEMPLATE "%s"`, dbName, *c.dbNameTemplate,
	)
	_, err = adminDB.ExecContext(ctx, cmd)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to clone template -> %w", err)
	}

	rawConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to get base conn string -> %w", err,
		)
	}

	u, err := url.Parse(rawConn)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to parse conn string URL -> %w", err,
		)
	}
	u.Path = "/" + dbName
	newConn := u.String()

	db, err := sql.Open(c.SqlDriverName, newConn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open isolated db -> %w", err)
	}

	cleanup := func() error {
		cErr := db.Close()
		if cErr != nil {
			return fmt.Errorf("failed to close isolated db -> %w", cErr)
		}

		cleanupDB, cErr := sql.Open(c.SqlDriverName, adminConn)
		if cErr != nil {
			return fmt.Errorf("failed to open cleanup admin db -> %w", cErr)
		}
		defer func(db *sql.DB) {
			_ = db.Close()
		}(cleanupDB)

		drop := fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, dbName)
		_, cErr = cleanupDB.ExecContext(context.Background(), drop)
		if cErr != nil {
			return fmt.Errorf("failed to drop isolated db -> %w", cErr)
		}

		return nil
	}

	return db, cleanup, nil
}

// ConnectionStringForDB returns the connection string for the isolated db.
// This relies purely on the container connection configurations to emit a valid
// postgres data source name targeted at a specified database namespace.
func (c *PostgresContainer) ConnectionStringForDB(
	ctx context.Context, dbName string,
) (string, error) {
	rawConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", fmt.Errorf("failed to get base connection -> %w", err)
	}

	u, err := url.Parse(rawConn)
	if err != nil {
		return "", fmt.Errorf("failed to parse connection url -> %w", err)
	}

	u.Path = "/" + dbName
	return u.String(), nil
}

// SetupTemplateDBWithManualSQL creates a template database and applies the
// given
// sql statements directly to setup the schema.
func (c *PostgresContainer) SetupTemplateDBWithManualSQL(
	ctx context.Context, dbNameTemplate string, statements []string,
) error {
	if c == nil {
		return errors.New("postgres container receiver cannot be nil")
	}

	rawAdminConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to get base admin connection string -> %w", err)
	}

	adminURL, err := url.Parse(rawAdminConn)
	if err != nil {
		return fmt.Errorf("failed to parse connection string -> %w", err)
	}
	adminURL.Path = "/postgres"
	adminConn := adminURL.String()

	adminDB, err := sql.Open(c.SqlDriverName, adminConn)
	if err != nil {
		return fmt.Errorf("failed to open admin database -> %w", err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(adminDB)

	_, err = adminDB.ExecContext(
		ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbNameTemplate),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create template db %s -> %w",
			dbNameTemplate, err,
		)
	}

	rawTargetConn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return fmt.Errorf("failed to get base connection string -> %w", err)
	}

	targetURL, err := url.Parse(rawTargetConn)
	if err != nil {
		return fmt.Errorf("failed to parse connection string -> %w", err)
	}
	targetURL.Path = "/" + dbNameTemplate
	targetConn := targetURL.String()

	targetDB, err := sql.Open(c.SqlDriverName, targetConn)
	if err != nil {
		return fmt.Errorf("failed to open target database -> %w", err)
	}
	defer func(db *sql.DB) {
		if db == nil {
			return
		}
		_ = db.Close()
	}(targetDB)

	for i, stmt := range statements {
		_, err = targetDB.ExecContext(ctx, stmt)
		if err != nil {
			return fmt.Errorf(
				"failed to execute manual statement %d: %s -> %w",
				i, stmt, err,
			)
		}
	}

	err = targetDB.Close()
	if err != nil {
		return fmt.Errorf("failed to close target db connection -> %w", err)
	}
	targetDB = nil

	_, err = adminDB.ExecContext(
		ctx,
		fmt.Sprintf(`ALTER DATABASE "%s" IS_TEMPLATE = true`, dbNameTemplate),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to set template db %s to template -> %w",
			dbNameTemplate, err,
		)
	}

	c.dbNameTemplate = new(string)
	*c.dbNameTemplate = dbNameTemplate
	return nil
}
