package testutil

import (
	"context"
	"database/sql"
	"testing"
)

// DatabaseProvider represents a generic isolated database engine.
type DatabaseProvider interface {
	// CreateIsolatedDB creates a fresh database cloned from the engine's template.
	// It returns the database connection pool and a cleanup function.
	CreateIsolatedDB(
		ctx context.Context,
		t *testing.T,
	) (*sql.DB, func() error, error)
}
