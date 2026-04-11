package migrate_test

import (
	"context"
	"os"
	"testing"

	"google.golang.org/protobuf/encoding/prototext"

	"github.com/uthereal/scheme/genproto"
	schemepg "github.com/uthereal/scheme/genproto/postgres"
	pgmigrate "github.com/uthereal/scheme/migrate/postgres"
	"github.com/uthereal/scheme/test/testutil"
)

func TestPostgresMigrateKitchenSink(t *testing.T) {
	ctx := context.Background()

	// Bootstrapping the initial schema into the IS_TEMPLATE container ensures
	// all isolated clones inherit the identical DDL state natively.
	pgContainer, err := testutil.StartPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start postgres container -> %v", err)
	}
	defer func() {
		_ = testutil.StopPostgresContainer(pgContainer)
	}()

	schemaPath := "../testdata/kitchen_sink.textproto"

	err = pgContainer.SetupTemplateDBWithSchema(
		ctx, "kitchen_sink_template", schemaPath,
	)
	if err != nil {
		t.Fatalf("failed to setup template db -> %v", err)
	}

	db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
	if err != nil {
		t.Fatalf("failed to create isolated db -> %v", err)
	}
	defer func() {
		_ = cleanup()
	}()

	schemaPathV2 := "../testdata/kitchen_sink_v2.textproto"

	parseSchema := func(path string) *schemepg.PostgresDatabase {
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("failed to read schema -> %v", readErr)
		}

		var sc genproto.Scheme
		unmarshalErr := prototext.Unmarshal(data, &sc)
		if unmarshalErr != nil {
			t.Fatalf("failed to unmarshal schema -> %v", unmarshalErr)
		}

		for _, database := range sc.GetDatabases() {
			if database.GetName() == "postgres" && database.GetPostgres() != nil {
				return database.GetPostgres()
			}
		}
		t.Fatalf("postgres database configuration not found in schema")
		return nil
	}

	t.Run("Subtest 1 - Idempotency (V1 -> V1)", func(t *testing.T) {
		pgSchemaV1 := parseSchema(schemaPath)
		liveState, inspectErr := pgmigrate.NewDatabaseStateFromDb(ctx, db)
		if inspectErr != nil {
			t.Fatalf("failed to inspect isolated db -> %v", inspectErr)
		}

		targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchemaV1)
		if err != nil {
			t.Fatalf("failed to map target schema -> %v", err)
		}
		actions, err := pgmigrate.ComputeDiff(liveState, targetState)
		if err != nil {
			t.Fatalf("failed to plan migration -> %v", err)
		}

		if len(actions) > 0 {
			for _, a := range actions {
				t.Logf("Unexpected diff action planned: %s", a.SQL)
			}
			t.Fatalf(
				"expected 0 diff actions for V1 idempotency, got %d",
				len(actions),
			)
		}
	})

	var v2Actions []pgmigrate.MigrationAction

	t.Run("Subtest 2 - Diff Calculation (V1 -> V2)", func(t *testing.T) {
		pgSchemaV2 := parseSchema(schemaPathV2)
		liveState, inspectErr := pgmigrate.NewDatabaseStateFromDb(ctx, db)
		if inspectErr != nil {
			t.Fatalf("failed to inspect isolated db -> %v", inspectErr)
		}

		targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchemaV2)
		if err != nil {
			t.Fatalf("failed to map target schema -> %v", err)
		}
		actions, err := pgmigrate.ComputeDiff(liveState, targetState)
		if err != nil {
			t.Fatalf("failed to plan migration -> %v", err)
		}

		v2Actions = actions

		if len(v2Actions) == 0 {
			t.Fatalf("expected V2 to generate migration actions, got 0")
		}

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
		if applyErr != nil {
			t.Fatalf("failed to apply V2 migrations -> %v", applyErr)
		}

		// Verify V2 idempotency
		pgSchemaV2 := parseSchema(schemaPathV2)
		liveState, inspectErr := pgmigrate.NewDatabaseStateFromDb(ctx, db)
		if inspectErr != nil {
			t.Fatalf("failed to inspect isolated db -> %v", inspectErr)
		}

		targetState, err := pgmigrate.NewDatabaseStateFromProto(pgSchemaV2)
		if err != nil {
			t.Fatalf("failed to map target schema -> %v", err)
		}
		actions, err := pgmigrate.ComputeDiff(liveState, targetState)
		if err != nil {
			t.Fatalf("failed to plan migration -> %v", err)
		}

		if len(actions) > 0 {
			for _, a := range actions {
				t.Logf("Unexpected V2 diff action planned: %s", a.SQL)
			}
			t.Fatalf(
				"expected 0 diff actions after V2 apply, got %d",
				len(actions),
			)
		}
	})
}
