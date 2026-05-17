package postgres_test

import (
  "context"
  "testing"

  "github.com/jackc/pgx/v5/pgxpool"
  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/runtime/postgres"
  "github.com/uthereal/scheme/test/testutil"
)

func TestListener(t *testing.T) {
  ctx := context.Background()

  pgContainer, err := testutil.StartPostgresContainer(ctx)
  require.NoError(t, err)
  defer func() {
    _ = testutil.StopPostgresContainer(pgContainer)
  }()

  connStr, err := pgContainer.ConnectionStringForDB(ctx, "postgres")
  require.NoError(t, err)

  pool, err := pgxpool.New(ctx, connStr)
  require.NoError(t, err)
  defer pool.Close()

  t.Run("NewListener creates a listener with the provided pool",
    func(t *testing.T) {
      listener := postgres.NewListener(pool)
      require.NotNil(t, listener)
      assert.Equal(t, pool, listener.Pool(),
        "Pool() should return the original pool")
    })

  t.Run("Pool returns the underlying connection pool", func(t *testing.T) {
    listener := postgres.NewListener(pool)
    assert.Same(t, pool, listener.Pool(),
      "Pool() should return exactly the same pointer")
  })
}
