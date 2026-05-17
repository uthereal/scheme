package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestQueryState_Immutability(t *testing.T) {
  s1 := postgres.QueryState{}

  s2 := s1.WithAlias("u")
  assert.Empty(t, s1.Alias)
  assert.Equal(t, "u", s2.Alias)

  col1 := postgres.BaseColumn("id")
  s3 := s2.WithSelects(col1)
  assert.Empty(t, s2.Selects)
  assert.Len(t, s3.Selects, 1)

  cond1 := postgres.ComparableColumn[int]("id").Eq(1)
  s4 := s3.WithWheres(cond1)
  assert.Empty(t, s3.Wheres)
  assert.Len(t, s4.Wheres, 1)

  limit := 10
  s5 := s4.WithLimit(limit)
  assert.Nil(t, s4.Limit)
  assert.Equal(t, limit, *s5.Limit)
}

func TestQueryState_WithWheres_IgnoresNil(t *testing.T) {
  s := postgres.QueryState{}
  s = s.WithWheres(nil, postgres.ComparableColumn[int]("id").Eq(1), nil)
  assert.Len(t, s.Wheres, 1)
}
