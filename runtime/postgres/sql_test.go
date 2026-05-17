package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestBuildSelectSQL(t *testing.T) {
  colID := postgres.NumberColumn[int]{
    BaseColumn:       "id",
    ComparableColumn: "id",
  }

  tests := []struct {
    name      string
    state     postgres.QueryState
    selectStr string
    tableName string
    withCount bool
    wantSQL   string
    wantArgs  []any
  }{
    {
      name:      "simple select",
      state:     postgres.QueryState{},
      selectStr: "id, name",
      tableName: "users",
      wantSQL:   "SELECT id, name FROM users",
      wantArgs:  nil,
    },
    {
      name: "select with where and alias",
      state: postgres.QueryState{
        Alias: "u",
        Wheres: []postgres.WhereCondition{
          colID.ComparableColumn.Eq(1),
        },
      },
      selectStr: "u.id, u.name",
      tableName: "users",
      wantSQL:   "SELECT u.id, u.name FROM users AS u WHERE id = $1",
      wantArgs:  []any{1},
    },
    {
      name: "select with limit and offset",
      state: postgres.QueryState{
        Limit:  new(10),
        Offset: new(20),
      },
      selectStr: "*",
      tableName: "users",
      wantSQL:   "SELECT * FROM users LIMIT $1 OFFSET $2",
      wantArgs:  []any{10, 20},
    },
  }

  for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
      gotSQL, gotArgs, err := postgres.BuildSelectSQL(
        tt.state, tt.selectStr, tt.tableName, 1, tt.withCount,
      )
      require.NoError(t, err)
      assert.Equal(t, tt.wantSQL, gotSQL)
      assert.Equal(t, tt.wantArgs, gotArgs)
    })
  }
}

func TestBuildExistsSQL(t *testing.T) {
  colID := postgres.ComparableColumn[int]("id")

  state := postgres.QueryState{
    Wheres: []postgres.WhereCondition{colID.Eq(1)},
  }

  gotSQL, gotArgs, err := postgres.BuildExistsSQL(state, "users")
  require.NoError(t, err)
  assert.Equal(t, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", gotSQL)
  assert.Equal(t, []any{1}, gotArgs)
}

func TestBuildDeleteSQL(t *testing.T) {
  colID := postgres.ComparableColumn[int]("id")

  state := postgres.QueryState{
    Wheres: []postgres.WhereCondition{colID.Eq(1)},
  }

  gotSQL, gotArgs, err := postgres.BuildDeleteSQL(state, "users")
  require.NoError(t, err)
  assert.Equal(t, "DELETE FROM users WHERE id = $1", gotSQL)
  assert.Equal(t, []any{1}, gotArgs)
}

func TestBuildUpdateSQL(t *testing.T) {
  colID := postgres.ComparableColumn[int]("id")
  colName := postgres.StringColumn[string]{BaseColumn: "name"}
  colAge := postgres.NumberColumn[int]{BaseColumn: "age"}

  state := postgres.QueryState{
    Wheres: []postgres.WhereCondition{colID.Eq(1)},
  }

  m := map[postgres.Column]any{
    colName: "John",
    colAge:  30,
  }

  gotSQL, gotArgs, err := postgres.BuildUpdateSQL(state, "users", m)
  require.NoError(t, err)

  // Deterministic order: age, name (alphabetical)
  assert.Equal(
    t, "UPDATE users SET age = $1, name = $2 WHERE id = $3", gotSQL,
  )
  assert.Equal(t, []any{30, "John", 1}, gotArgs)
}
