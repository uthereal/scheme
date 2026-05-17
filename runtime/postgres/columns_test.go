package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestBaseColumn(t *testing.T) {
  col := postgres.BaseColumn("id")
  assert.Equal(t, "id", col.String())

  asc := col.Asc()
  sql, args, err := asc.BuildOrderSQL(nil)
  require.NoError(t, err)
  assert.Equal(t, "id ASC", sql)
  assert.Nil(t, args)
}

func TestComparableColumn(t *testing.T) {
  col := postgres.ComparableColumn[int]("age")

  t.Run("Eq", func(t *testing.T) {
    cond := col.Eq(25)
    idx := 1
    sql, args, err := cond.BuildWhereSQL(&idx)
    require.NoError(t, err)
    assert.Equal(t, "age = $1", sql)
    assert.Equal(t, []any{25}, args)
  })

  t.Run("In", func(t *testing.T) {
    cond := col.In(1, 2, 3)
    idx := 1
    sql, args, err := cond.BuildWhereSQL(&idx)
    require.NoError(t, err)
    assert.Equal(t, "age IN ($1, $2, $3)", sql)
    assert.Equal(t, []any{1, 2, 3}, args)
  })

  t.Run("IsNull", func(t *testing.T) {
    cond := col.IsNull()
    idx := 1
    sql, args, err := cond.BuildWhereSQL(&idx)
    require.NoError(t, err)
    assert.Equal(t, "age IS NULL", sql)
    assert.Empty(t, args)
  })
}

func TestArrayableColumn(t *testing.T) {
  col := postgres.ArrayableColumn[string]("tags")

  t.Run("ArrayContains", func(t *testing.T) {
    cond := col.ArrayContains([]string{"go", "sql"})
    idx := 1
    sql, args, err := cond.BuildWhereSQL(&idx)
    require.NoError(t, err)
    assert.Equal(t, "tags @> $1", sql)
    assert.Equal(t, []any{[]string{"go", "sql"}}, args)
  })
}
