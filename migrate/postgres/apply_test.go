package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/scheme/migrate/postgres"
)

// Minimal dummy driver for testing Apply without a real DB.
type mockDriver struct{}

func (d mockDriver) Open(name string) (driver.Conn, error) {
	if name == "fail_open" {
		return nil, errors.New("open error")
	}
	return mockConn{name: name}, nil
}

type mockConn struct{ name string }

func (c mockConn) Prepare(q string) (driver.Stmt, error) {
	return mockStmt{}, nil
}

func (c mockConn) Close() error {
	return nil
}

func (c mockConn) Begin() (driver.Tx, error) {
	if c.name == "fail_begin" {
		return nil, errors.New("begin error")
	}
	return mockTx{name: c.name}, nil
}

func (c mockConn) BeginTx(
	ctx context.Context, opts driver.TxOptions,
) (driver.Tx, error) {
	return c.Begin()
}

func (c mockConn) ExecContext(
	ctx context.Context, q string, args []driver.NamedValue,
) (driver.Result, error) {
	if strings.Contains(q, "FAIL_EXEC") {
		return nil, errors.New("exec error")
	}
	return nil, nil
}

type mockTx struct{ name string }

func (t mockTx) Commit() error {
	if t.name == "fail_commit" {
		return errors.New("commit error")
	}
	return nil
}

func (t mockTx) Rollback() error {
	if t.name == "fail_rollback" {
		return errors.New("rollback error")
	}
	return nil
}

type mockStmt struct{}

func (s mockStmt) Close() error {
	return nil
}

func (s mockStmt) NumInput() int {
	return 0
}

func (s mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (s mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

func init() {
	sql.Register("mock_apply", mockDriver{})
}

func TestApply(t *testing.T) {
	dbOk, _ := sql.Open("mock_apply", "ok")
	dbFailBegin, _ := sql.Open("mock_apply", "fail_begin")
	dbFailCommit, _ := sql.Open("mock_apply", "fail_commit")
	dbFailRollback, _ := sql.Open("mock_apply", "fail_rollback")

	tests := []struct {
		name    string
		db      *sql.DB
		actions []postgres.MigrationAction
		wantErr string
	}{
		{
			name:    "nil db",
			db:      nil,
			actions: []postgres.MigrationAction{{SQL: "SELECT 1"}},
			wantErr: "database connection cannot be nil",
		},
		{
			name:    "empty actions",
			db:      dbOk,
			actions: []postgres.MigrationAction{},
			wantErr: "",
		},
		{
			name: "successful apply",
			db:   dbOk,
			actions: []postgres.MigrationAction{
				{SQL: "CREATE TABLE t (id int);"},
				{SQL: ""}, // Should be skipped
			},
			wantErr: "",
		},
		{
			name:    "fail begin tx",
			db:      dbFailBegin,
			actions: []postgres.MigrationAction{{SQL: "SELECT 1"}},
			wantErr: "failed to begin migration transaction",
		},
		{
			name:    "fail exec context",
			db:      dbOk,
			actions: []postgres.MigrationAction{{SQL: "FAIL_EXEC"}},
			wantErr: "action 0 failed",
		},
		{
			name:    "fail commit",
			db:      dbFailCommit,
			actions: []postgres.MigrationAction{{SQL: "SELECT 1"}},
			wantErr: "failed to commit migration transaction",
		},
		{
			name:    "fail rollback",
			db:      dbFailRollback,
			actions: []postgres.MigrationAction{{SQL: "FAIL_EXEC"}},
			wantErr: "action 0 failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := postgres.Apply(ctx, tt.db, tt.actions)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Apply() error = nil, wantErr %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("Apply() error = %v, wantErr %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("Apply() unexpected error = %v", err)
			}
		})
	}
}
