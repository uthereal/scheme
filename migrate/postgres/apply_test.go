package postgres_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/uthereal/scheme/migrate/postgres"
)

func TestApply(t *testing.T) {
	t.Run("nil db", func(t *testing.T) {
		ctx := context.Background()
		err := postgres.Apply(
			ctx, nil, []postgres.MigrationAction{{SQL: "SELECT 1"}},
		)

		gotErr, wantErr := err != nil, true
		if gotErr != wantErr {
			t.Errorf("Apply(nil) error exists = %v, want %v", gotErr, wantErr)
		}
	})

	tests := []struct {
		name        string
		actions     []postgres.MigrationAction
		wantErr     bool
		errContains string
		verify      func(t *testing.T, ctx context.Context, db *sql.DB)
	}{
		{
			name:    "empty actions",
			actions: []postgres.MigrationAction{},
			wantErr: false,
		},
		{
			name: "successful apply",
			actions: []postgres.MigrationAction{
				{SQL: "CREATE TABLE test_table (id int);"},
				{SQL: ""}, // Should be skipped
			},
			wantErr: false,
			verify: func(t *testing.T, ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, "SELECT 1 FROM test_table;")

				gotErr, wantErr := err != nil, false
				if gotErr != wantErr {
					t.Errorf(
						"expected table creation error = %v, want %v",
						err, wantErr,
					)
				}
			},
		},
		{
			name: "fail exec context with rollback",
			actions: []postgres.MigrationAction{
				{SQL: "CREATE TABLE should_rollback (id int);"},
				{SQL: "INVALID SQL SYNTAX;"},
			},
			wantErr:     true,
			errContains: "(INVALID SQL SYNTAX;) failed ->",
			verify: func(t *testing.T, ctx context.Context, db *sql.DB) {
				var count int
				q := "SELECT count(*) FROM information_schema.tables " +
					"WHERE table_name = 'should_rollback'"
				err := db.QueryRowContext(ctx, q).Scan(&count)

				gotErr, wantErr := err != nil, false
				if gotErr != wantErr {
					t.Fatalf(
						"information_schema query error = %v, want %v",
						err, wantErr,
					)
				}

				gotCount, wantCount := count, 0
				if gotCount != wantCount {
					t.Errorf(
						"rollback table count = %d, want %d",
						gotCount, wantCount,
					)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			db, cleanup, err := testContainer.CreateIsolatedDB(ctx, t)
			if err != nil {
				t.Fatalf("failed to create isolated db -> %v", err)
			}
			defer func() {
				_ = cleanup()
			}()

			err = postgres.Apply(ctx, db, tt.actions)

			gotErr, wantErr := err != nil, tt.wantErr
			if gotErr != wantErr {
				t.Fatalf("Apply() error exists = %v, want %v", gotErr, wantErr)
			}

			if err != nil && tt.errContains != "" {
				gotContains, wantContains := strings.Contains(
					err.Error(), tt.errContains,
				), true
				if gotContains != wantContains {
					t.Errorf(
						"Apply() error = %v, want containing %q",
						err, tt.errContains,
					)
				}
			}

			if tt.verify != nil {
				tt.verify(t, ctx, db)
			}
		})
	}
}
