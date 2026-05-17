package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestMutateOption(t *testing.T) {
  m := make(map[postgres.Column]any)
  col := postgres.BaseColumn("id")

  var opt postgres.MutateOption = func(m map[postgres.Column]any) {
    m[col] = 123
  }

  opt(m)

  assert.Equal(t, 123, m[col])
}
