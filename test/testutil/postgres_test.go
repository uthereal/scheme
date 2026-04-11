package testutil_test

import (
	"context"
	"testing"

	"github.com/uthereal/scheme/test/testutil"
)

func TestPostgresContainerLifecycle(t *testing.T) {
	ctx := context.Background()

	// 1. Start the container
	pgContainer, err := testutil.StartPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer func() {
		err := testutil.StopPostgresContainer(pgContainer)
		if err != nil {
			t.Errorf("failed to stop postgres container: %v", err)
		}
	}()

	// 2. Setup Template DB with Manual SQL
	err = pgContainer.SetupTemplateDBWithManualSQL(
		ctx, "template_manual", []string{
			"CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT);",
		})
	if err != nil {
		t.Fatalf("failed to setup template db: %v", err)
	}

	// 3. Create Isolated DB
	db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
	if err != nil {
		t.Fatalf("failed to create isolated db: %v", err)
	}
	defer cleanup() // Will be called gracefully

	// 4. Verify the Isolated DB inherited the schema
	var tableName string
	err = db.QueryRowContext(
		ctx,
		"SELECT tablename FROM pg_tables WHERE tablename = 'test_table'",
	).Scan(&tableName)
	if err != nil {
		t.Fatalf("failed to query isolated db: %v", err)
	}
	if tableName != "test_table" {
		t.Errorf("expected test_table, got %s", tableName)
	}

	// 5. Test inserting into isolated DB
	_, err = db.ExecContext(ctx, "INSERT INTO test_table (name) VALUES ('hello')")
	if err != nil {
		t.Fatalf("failed to insert into isolated db: %v", err)
	}

	// 6. Test getting connection string
	connStr, err := pgContainer.ConnectionStringForDB(ctx, "testdb")
	if err != nil {
		t.Errorf("failed to get connection string: %v", err)
	}
	if connStr == "" {
		t.Errorf("expected connection string, got empty string")
	}
}

func TestPostgresContainerSetupWithSchema(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := testutil.StartPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	defer testutil.StopPostgresContainer(pgContainer)

	err = pgContainer.SetupTemplateDBWithSchema(
		ctx, "template_schema", "../testdata/kitchen_sink.textproto",
	)
	if err != nil {
		t.Fatalf("failed to setup template db with schema: %v", err)
	}

	db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
	if err != nil {
		t.Fatalf("failed to create isolated db: %v", err)
	}
	defer cleanup()

	var count int
	err = db.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'",
	).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query isolated db: %v", err)
	}
	if count == 0 {
		t.Errorf("expected tables in public schema, got 0")
	}
}
