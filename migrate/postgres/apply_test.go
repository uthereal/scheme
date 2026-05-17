package postgres_test

import (
  "context"
  "database/sql"
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/migrate/postgres"
)

func TestApply(t *testing.T) {
  t.Run("nil db", func(t *testing.T) {
    ctx := context.Background()
    err := postgres.Apply(
      ctx, nil, []postgres.MigrationAction{{SQL: "SELECT 1"}},
    )
    assert.Error(t, err)
  })

  tests := []struct {
    name        string
    actions     []postgres.MigrationAction
    wantErr     bool
    errContains string
    verify      func(t *testing.T, ctx context.Context, db *sql.DB)
  }{
    {
      name:    "empty actions",
      actions: []postgres.MigrationAction{},
      wantErr: false,
    },
    {
      name: "successful apply",
      actions: []postgres.MigrationAction{
        {SQL: "CREATE TABLE test_table (id int);"},
        {SQL: ""}, // Should be skipped
      },
      wantErr: false,
      verify: func(t *testing.T, ctx context.Context, db *sql.DB) {
        _, err := db.ExecContext(ctx, "SELECT 1 FROM test_table;")
        assert.NoError(t, err, "expected table creation success")
      },
    },
    {
      name: "fail exec context with rollback",
      actions: []postgres.MigrationAction{
        {SQL: "CREATE TABLE should_rollback (id int);"},
        {SQL: "INVALID SQL SYNTAX;"},
      },
      wantErr:     true,
      errContains: "(INVALID SQL SYNTAX;) failed ->",
      verify: func(t *testing.T, ctx context.Context, db *sql.DB) {
        var count int
        q := "SELECT count(*) FROM information_schema.tables " +
          "WHERE table_name = 'should_rollback'"
        err := db.QueryRowContext(ctx, q).Scan(&count)
        require.NoError(t, err)
        assert.Equal(t, 0, count, "rollback table should not exist")
      },
    },
  }

  for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
      ctx := context.Background()

      db, cleanup, err := testContainer.CreateIsolatedDB(ctx, t)
      require.NoError(t, err, "failed to create isolated db")
      defer func() {
        _ = cleanup()
      }()

      err = postgres.Apply(ctx, db, tt.actions)

      if tt.wantErr {
        require.Error(t, err)
        if tt.errContains != "" {
          assert.Contains(t, err.Error(), tt.errContains)
        }
      } else {
        require.NoError(t, err)
      }

      if tt.verify != nil {
        tt.verify(t, ctx, db)
      }
    })
  }
}
