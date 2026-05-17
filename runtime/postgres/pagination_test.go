package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestCountPagination(t *testing.T) {
  page := postgres.CountPagination[string]{
    Items:      []string{"a", "b", "c"},
    TotalCount: 10,
  }

  assert.Len(t, page.Items, 3)
  assert.Equal(t, 10, page.TotalCount)
}

func TestSimplePagination(t *testing.T) {
  page := postgres.SimplePagination[int]{
    Items:   []int{1, 2, 3},
    HasMore: true,
  }

  assert.Len(t, page.Items, 3)
  assert.True(t, page.HasMore)
}
