package migrate_test

import (
  "context"
  "os"
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/genproto"
  schemepg "github.com/uthereal/scheme/genproto/postgres"
  pgmigrate "github.com/uthereal/scheme/migrate/postgres"
  "github.com/uthereal/scheme/test/testutil"
  "google.golang.org/protobuf/encoding/prototext"
)

func TestPostgresMigrateKitchenSink(t *testing.T) {
  ctx := context.Background()

  // Bootstrapping the initial schema into the IS_TEMPLATE container ensures
  // all isolated clones inherit the identical DDL state natively.
  pgContainer, err := testutil.StartPostgresContainer(ctx)
  require.NoError(t, err, "failed to start postgres container")
  defer func() {
    _ = testutil.StopPostgresContainer(pgContainer)
  }()

  schemaPath := "../testdata/kitchen_sink.textproto"

  err = pgContainer.SetupTemplateDBWithSchema(
    ctx, "kitchen_sink_template", schemaPath,
  )
  require.NoError(t, err, "failed to setup template db")

  db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
  require.NoError(t, err, "failed to create isolated db")
  defer func() {
    _ = cleanup()
  }()

  schemaPathV2 := "../testdata/kitchen_sink_v2.textproto"

  parseSchema := func(path string) *schemepg.PostgresDatabase {
    data, readErr := os.ReadFile(path)
    require.NoError(t, readErr, "failed to read schema")

    var sc genproto.Scheme
    unmarshalErr := prototext.Unmarshal(data, &sc)
    require.NoError(t, unmarshalErr, "failed to unmarshal schema")

    database := sc.GetDatabase()
    require.NotNil(t, database,
      "postgres database configuration not found in schema")
    require.Equal(t, "postgres", database.GetName())
    require.NotNil(t, database.GetPostgres())
    return database.GetPostgres()
  }

  t.Run("Subtest 1 - Idempotency (V1 -> V1)", func(t *testing.T) {
    pgSchemaV1 := parseSchema(schemaPath)
    liveState, inspectErr := pgmigrate.NewDatabaseStateFromDb(ctx, db)
    require.NoError(t, inspectErr, "failed to inspect isolated db")

    targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchemaV1)
    require.NoError(t, err, "failed to map target schema")

    actions, err := pgmigrate.ComputeDiff("", false, liveState, targetState)
    require.NoError(t, err, "failed to plan migration")

    if len(actions) > 0 {
      for _, a := range actions {
        t.Logf("Unexpected diff action planned: %s", a.SQL)
      }
    }
    assert.Empty(t, actions, "expected 0 diff actions for V1 idempotency")
  })

  var v2Actions []pgmigrate.MigrationAction

  t.Run("Subtest 2 - Diff Calculation (V1 -> V2)", func(t *testing.T) {
    pgSchemaV2 := parseSchema(schemaPathV2)
    liveState, inspectErr := pgmigrate.NewDatabaseStateFromDb(ctx, db)
    require.NoError(t, inspectErr, "failed to inspect isolated db")

    targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchemaV2)
    require.NoError(t, err, "failed to map target schema")

    actions, err := pgmigrate.ComputeDiff("", false, liveState, targetState)
    require.NoError(t, err, "failed to plan migration")

    v2Actions = actions
    assert.NotEmpty(t, v2Actions, "expected V2 to generate migration actions")

    // Ensure we hit a variety of action types
    actionCounts := make(map[pgmigrate.ActionType]int)
    objectCounts := make(map[pgmigrate.ObjectType]int)

    for _, a := range v2Actions {
      actionCounts[a.Type]++
      objectCounts[a.ObjectType]++
    }

    t.Logf("V2 generated %d actions", len(v2Actions))
    for actionType, count := range actionCounts {
      t.Logf("Action %s: %d", actionType, count)
    }
    for objType, count := range objectCounts {
      t.Logf("Object %s: %d", objType, count)
    }
  })

  t.Run("Subtest 3 - Execution & Verification (Apply V2)", func(t *testing.T) {
    for i, a := range v2Actions {
      t.Logf("V2 ACTION %d: %s", i, a.SQL)
    }
    applyErr := pgmigrate.Apply(ctx, db, v2Actions)
    require.NoError(t, applyErr, "failed to apply V2 migrations")

    // Verify V2 idempotency
    pgSchemaV2 := parseSchema(schemaPathV2)
    liveState, inspectErr := pgmigrate.NewDatabaseStateFromDb(ctx, db)
    require.NoError(t, inspectErr, "failed to inspect isolated db")

    targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchemaV2)
    require.NoError(t, err, "failed to map target schema")

    actions, err := pgmigrate.ComputeDiff("", false, liveState, targetState)
    require.NoError(t, err, "failed to plan migration")

    if len(actions) > 0 {
      for _, a := range actions {
        t.Logf("Unexpected V2 diff action planned: %s", a.SQL)
      }
    }
    assert.Empty(t, actions, "expected 0 diff actions after V2 apply")
  })
}
