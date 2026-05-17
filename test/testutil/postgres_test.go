package testutil_test

import (
  "context"
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/test/testutil"
)

func TestPostgresContainerLifecycle(t *testing.T) {
  ctx := context.Background()

  // 1. Start the container
  pgContainer, err := testutil.StartPostgresContainer(ctx)
  require.NoError(t, err, "failed to start postgres container")
  defer func() {
    err := testutil.StopPostgresContainer(pgContainer)
    assert.NoError(t, err, "failed to stop postgres container")
  }()

  // 2. Setup Template DB with Manual SQL
  err = pgContainer.SetupTemplateDBWithManualSQL(
    ctx, "template_manual", []string{
      "CREATE TABLE test_table (id SERIAL PRIMARY KEY, name TEXT);",
    })
  require.NoError(t, err, "failed to setup template db")

  // 3. Create Isolated DB
  db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
  require.NoError(t, err, "failed to create isolated db")
  defer cleanup() // Will be called gracefully

  // 4. Verify the Isolated DB inherited the schema
  var tableName string
  err = db.QueryRowContext(
    ctx,
    "SELECT tablename FROM pg_tables WHERE tablename = 'test_table'",
  ).Scan(&tableName)
  require.NoError(t, err, "failed to query isolated db")
  assert.Equal(t, "test_table", tableName)

  // 5. Test inserting into isolated DB
  _, err = db.ExecContext(ctx, "INSERT INTO test_table (name) VALUES ('hello')")
  assert.NoError(t, err, "failed to insert into isolated db")

  // 6. Test getting connection string
  connStr, err := pgContainer.ConnectionStringForDB(ctx, "testdb")
  assert.NoError(t, err, "failed to get connection string")
  assert.NotEmpty(t, connStr, "expected connection string")
}

func TestPostgresContainerSetupWithSchema(t *testing.T) {
  ctx := context.Background()

  pgContainer, err := testutil.StartPostgresContainer(ctx)
  require.NoError(t, err, "failed to start postgres container")
  defer func() {
    _ = testutil.StopPostgresContainer(pgContainer)
  }()

  err = pgContainer.SetupTemplateDBWithSchema(
    ctx, "template_schema", "../testdata/kitchen_sink.textproto",
  )
  require.NoError(t, err, "failed to setup template db with schema")

  db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
  require.NoError(t, err, "failed to create isolated db")
  defer cleanup()

  var count int
  err = db.QueryRowContext(
    ctx, "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'",
  ).Scan(&count)
  require.NoError(t, err, "failed to query isolated db")
  assert.NotZero(t, count, "expected tables in public schema")
}
