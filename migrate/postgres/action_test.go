package postgres_test

import (
	"testing"

	"github.com/uthereal/scheme/migrate/postgres"
)

func TestMigrationAction_String(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		defer func() {
			r := recover()
			gotPanic, wantPanic := r != nil, true
			if gotPanic != wantPanic {
				t.Errorf("String() on nil receiver did not panic")
			}
		}()
		var m *postgres.MigrationAction
		_ = m.String()
	})

	tests := []struct {
		name   string
		action postgres.MigrationAction
		want   string
	}{
		{
			name: "create table",
			action: postgres.MigrationAction{
				Type:          postgres.ActionTypeCreate,
				ObjectType:    postgres.ObjectTable,
				Schema:        "public",
				Name:          "users",
				SQL:           "CREATE TABLE public.users (id uuid);",
				IsDestructive: false,
			},
			want: "CREATE TABLE public.users -> " +
				"CREATE TABLE public.users (id uuid);",
		},
		{
			name: "drop column destructive",
			action: postgres.MigrationAction{
				Type:          postgres.ActionTypeDrop,
				ObjectType:    postgres.ObjectColumn,
				Schema:        "public",
				Name:          "users.email",
				SQL:           "ALTER TABLE public.users DROP COLUMN email;",
				IsDestructive: true,
			},
			want: "DROP COLUMN public.users.email [DESTRUCTIVE] -> " +
				"ALTER TABLE public.users DROP COLUMN email;",
		},
		{
			name: "rename enum",
			action: postgres.MigrationAction{
				Type:          postgres.ActionTypeRename,
				ObjectType:    postgres.ObjectEnum,
				Schema:        "auth",
				Name:          "status",
				SQL:           "ALTER TYPE auth.old_status RENAME TO status;",
				IsDestructive: false,
			},
			want: "RENAME ENUM auth.status -> " +
				"ALTER TYPE auth.old_status RENAME TO status;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, want := tt.action.String(), tt.want
			if got != want {
				t.Errorf("String() = %v, want %v", got, want)
			}
		})
	}
}
