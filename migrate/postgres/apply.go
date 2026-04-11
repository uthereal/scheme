package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Apply executes a sequence of MigrationActions against the database
// inside a single transaction.
// If any action fails, the entire migration rolls back automatically.
func Apply(
	ctx context.Context, db *sql.DB, actions []MigrationAction,
) (err error) {
	if db == nil {
		return errors.New("database connection cannot be nil")
	}
	if len(actions) == 0 {
		return nil
	}

	dCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, err := db.BeginTx(dCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin migration transaction -> %w", err)
	}
	defer func() {
		if err == nil {
			return
		}

		_ = tx.Rollback()
	}()

	// Defer all constraints to the end of the transaction to prevent intermediate
	// state violations during complex multi-step migrations.
	_, err = tx.ExecContext(dCtx, "SET CONSTRAINTS ALL DEFERRED;")
	if err != nil {
		return fmt.Errorf("failed to set constraints deferred -> %w", err)
	}

	for i, action := range actions {
		if action.SQL == "" {
			continue
		}

		_, err = tx.ExecContext(dCtx, action.SQL)
		if err != nil {
			return fmt.Errorf(
				"action %d (%s) failed -> %w", i, action.SQL, err,
			)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit migration transaction -> %w", err)
	}

	return nil
}
