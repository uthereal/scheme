package postgres

import (
  "github.com/jackc/pgx/v5/pgxpool"
)

// Listener wraps an underlying pgxpool connection to manage
// Pub/Sub notification listening. It provides a stable handle
// for generated listener functions to access the shared connection pool.
type Listener struct {
  pool *pgxpool.Pool
}

// NewListener creates a new listener instance using the provided pool.
func NewListener(pool *pgxpool.Pool) *Listener {
  return &Listener{
    pool: pool,
  }
}

// Pool returns the underlying pgxpool used by this listener.
func (l *Listener) Pool() *pgxpool.Pool {
  return l.pool
}
