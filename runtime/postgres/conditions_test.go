package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestConditions(t *testing.T) {
  t.Run("And", func(t *testing.T) {
    expr1 := postgres.Expr("a = ?", 1)
    expr2 := postgres.Expr("b = ?", 2)
    and := postgres.And(expr1, expr2)

    idx := 1
    sql, args, err := and.BuildWhereSQL(&idx)
    require.NoError(t, err)
    assert.Equal(t, "(a = $1 AND b = $2)", sql)
    assert.Equal(t, []any{1, 2}, args)
    assert.Equal(t, 3, idx)
  })

  t.Run("Or", func(t *testing.T) {
    expr1 := postgres.Expr("a = ?", 1)
    expr2 := postgres.Expr("b = ?", 2)
    or := postgres.Or(expr1, expr2)

    idx := 1
    sql, args, err := or.BuildWhereSQL(&idx)
    require.NoError(t, err)
    assert.Equal(t, "(a = $1 OR b = $2)", sql)
    assert.Equal(t, []any{1, 2}, args)
  })

  t.Run("Not", func(t *testing.T) {
    expr := postgres.Expr("a = ?", 1)
    not := postgres.Not(expr)

    sql, args, err := not.BuildWhereSQL(new(1))
    require.NoError(t, err)
    assert.Equal(t, "NOT (a = $1)", sql)
    assert.Equal(t, []any{1}, args)
  })

  t.Run("Exists", func(t *testing.T) {
    // Mock subquery
    sq := &mockSubQuery{
      sql:  "SELECT 1",
      args: []any{},
    }
    exists := postgres.Exists(sq)

    sql, args, err := exists.BuildWhereSQL(new(1))
    require.NoError(t, err)
    assert.Equal(t, "EXISTS (SELECT 1)", sql)
    assert.Empty(t, args)
  })
}
