package ast_test

import (
	"testing"

	"github.com/scheme/gen/postgres/ast"
)

func TestModelData_Initialization(t *testing.T) {
	t.Run("verifies struct fields assignment", func(t *testing.T) {
		model := &ast.ModelData{
			StructNameExported: "User",
			StructNamePrivate:  "user",
			TableName:          "users",
			SchemaName:         "public",
			TableFullName:      `"public"."users"`,
			Fields:             []ast.ModelDataField{},
			Edges:              []ast.EdgeData{},
		}

		gotName, wantName := model.StructNameExported, "User"
		if gotName != wantName {
			t.Errorf("got %q, want %q", gotName, wantName)
		}

		gotFullName, wantFullName := model.TableFullName, `"public"."users"`
		if gotFullName != wantFullName {
			t.Errorf("got %q, want %q", gotFullName, wantFullName)
		}
	})
}
