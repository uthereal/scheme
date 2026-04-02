package ast_test

import (
	"testing"

	"github.com/scheme/gen/postgres/ast"
	"github.com/scheme/genproto/spec/postgres"
)

func TestNewSchemaGraph(t *testing.T) {
	t.Run("returns error on nil schema", func(t *testing.T) {
		_, err := ast.NewSchemaGraph(nil)

		gotErr, wantErr := err != nil, true
		if gotErr != wantErr {
			t.Error("expected error when schema is nil")
		}
	})

	t.Run("successfully parses basic schema", func(t *testing.T) {
		pgDb := &postgres.PostgresDatabase{
			Schemas: []*postgres.PostgresSchema{
				{
					Name: "public",
					Tables: []*postgres.Table{
						{
							Name: "users",
							Columns: []*postgres.Column{
								{
									Name: "id",
									Type: &postgres.DataType{
										Type: &postgres.DataType_UuidType{
											UuidType: &postgres.UuidType{},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		sg, err := ast.NewSchemaGraph(pgDb)

		gotErr, wantErr := err != nil, false
		if gotErr != wantErr {
			t.Fatalf("got unexpected error: %v", err)
		}

		gotModels, wantModels := len(sg.ModelList()), 1
		if gotModels != wantModels {
			t.Errorf("got %d models, want %d", gotModels, wantModels)
		}

		if len(sg.ModelList()) > 0 {
			modelName := sg.ModelList()[0].StructNameExported
			gotName, wantName := modelName, "User"
			if gotName != wantName {
				t.Errorf("got struct name %q, want %q", gotName, wantName)
			}
		}
	})
}
