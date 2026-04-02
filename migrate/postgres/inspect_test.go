package postgres_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/scheme/migrate/postgres"
)

func TestInspect(t *testing.T) {
	tests := []struct {
		name    string
		db      *sql.DB
		wantErr string
	}{
		{
			name:    "nil db",
			db:      nil,
			wantErr: "database connection cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, err := postgres.Inspect(ctx, tt.db)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("Inspect() error = nil, wantErr %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("Inspect() error = %v, wantErr %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("Inspect() unexpected error = %v", err)
			}
		})
	}
}
