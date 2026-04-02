package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

// Apply executes a sequence of MigrationActions against the database
// inside a single, unified transaction. If any action fails, the entire
// migration rolls back automatically.
func Apply(
	ctx context.Context, db *sql.DB, actions []MigrationAction,
) (err error) {
	if db == nil {
		return fmt.Errorf("database connection cannot be nil")
	}
	if len(actions) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin migration transaction -> %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for i, action := range actions {
		if action.SQL == "" {
			continue
		}

		_, err = tx.ExecContext(ctx, action.SQL)
		if err != nil {
			return fmt.Errorf(
				"action %d failed (%s) -> %w", i, action.SQL, err,
			)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit migration transaction -> %w", err)
	}

	return nil
}
