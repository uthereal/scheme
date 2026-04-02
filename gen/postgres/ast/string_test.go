package ast_test

import (
	"testing"

	"github.com/uthereal/scheme/gen/postgres/ast"
)

func TestTableNameToStructName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"users", "User"},
		{"job_handlers", "JobHandler"},
		{"auth_users", "AuthUser"},
		{"person", "Person"},
		{"categories", "Category"},
		{"media_items", "MediaItem"},
		{"staff", "Staff"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, want := ast.TableNameToStructName(tt.in), tt.want
			if got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}

func TestColumnNameToFieldName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"id", "ID"},
		{"user_id", "UserID"},
		{"device_uuid", "DeviceUUID"},
		{"first_name", "FirstName"},
		{"is_active", "IsActive"},
		{"http_url", "HTTPURL"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, want := ast.ColumnNameToFieldName(tt.in), tt.want
			if got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}
