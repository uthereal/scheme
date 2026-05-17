package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestOrderDirection(t *testing.T) {
  assert.Equal(t, "ASC", string(postgres.OrderAsc))
  assert.Equal(t, "DESC", string(postgres.OrderDesc))
  assert.Equal(t, "ASC NULLS FIRST", string(postgres.OrderAscNullsFirst))
  assert.Equal(t, "ASC NULLS LAST", string(postgres.OrderAscNullsLast))
  assert.Equal(t, "DESC NULLS FIRST", string(postgres.OrderDescNullsFirst))
  assert.Equal(t, "DESC NULLS LAST", string(postgres.OrderDescNullsLast))
}
