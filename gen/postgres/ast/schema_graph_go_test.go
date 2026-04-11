package ast_test

import (
	"testing"

	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/ast"
	"github.com/uthereal/scheme/genproto/core"
	"github.com/uthereal/scheme/genproto/core/shared"
	"github.com/uthereal/scheme/genproto/postgres"
)

func TestNewSchemaGraphGo(t *testing.T) {
	t.Run("returns error on nil database", func(t *testing.T) {
		pgDb := &postgres.PostgresDatabase{
			Schemas: []*postgres.PostgresSchema{},
		}
		sg, _ := ast.NewSchemaGraph(pgDb)

		opts := gen.GenerateOptions{
			GoPackagePath: "github.com/test/pkg",
		}
		lang := gen.Language{
			Name:    "go",
			Options: opts,
			Validate: func(o gen.GenerateOptions) error {
				return nil
			},
		}
		_, err := ast.NewSchemaGraphGo(nil, sg, lang)

		if err == nil {
			t.Error("expected error when database is nil")
		}
	})

	t.Run("successfully wraps generic schema graph with all types",
		func(t *testing.T) {
			db := buildTestDatabase()

			pgDb := db.GetPostgres()
			sg, err := ast.NewSchemaGraph(pgDb)
			if err != nil {
				t.Fatalf("failed parsing generic graph -> %v", err)
			}

			opts := gen.GenerateOptions{
				GoPackagePath: "github.com/test/pkg",
			}
			lang := gen.Language{
				Name:    "go",
				Options: opts,
				Validate: func(o gen.GenerateOptions) error {
					return nil
				},
			}
			sgg, err := ast.NewSchemaGraphGo(db, sg, lang)
			if err != nil {
				t.Fatalf("failed wrapping go graph -> %v", err)
			}

			// Verify Models
			gotModels, wantModels := len(sgg.Models), 2
			if gotModels != wantModels {
				t.Errorf("got %d models, want %d", gotModels, wantModels)
			}

			// Verify Enums
			gotEnums, wantEnums := len(sgg.Enums), 1
			if gotEnums != wantEnums {
				t.Errorf("got %d enums, want %d", gotEnums, wantEnums)
			} else {
				got, want := sgg.Enums[0].Name, "UserRole"
				if got != want {
					t.Errorf("got enum %q, want %q", got, want)
				}
			}

			// Verify Domains
			gotDoms, wantDoms := len(sgg.Domains), 1
			if gotDoms != wantDoms {
				t.Errorf("got %d domains, want %d", gotDoms, wantDoms)
			} else {
				dom := sgg.Domains[0]
				got, want := dom.Name, "EmailAddress"
				if got != want {
					t.Errorf("got %q, want %q", got, want)
				}
				got, want = dom.BaseGoType, "string"
				if got != want {
					t.Errorf("got %q, want %q", got, want)
				}
			}

			// Verify Composites
			gotComps, wantComps := len(sgg.Composites), 1
			if gotComps != wantComps {
				t.Errorf("got %d composites, want %d", gotComps, wantComps)
			} else {
				got, want := sgg.Composites[0].Name, "Address"
				if got != want {
					t.Errorf("got %q, want %q", got, want)
				}
			}

			// Verify Field mapping in "Users" model
			usersModel := findModelGo(sgg.Models, "User")
			if usersModel == nil {
				t.Fatal("User model not found")
			}

			fieldMappings := map[string]struct {
				goType string
				isPtr  bool
			}{
				"id":        {"uuid.UUID", false},
				"role":      {"UserRole", false},
				"email":     {"EmailAddress", true},
				"home_addr": {"Address", true},
				"tags":      {"[]string", false},
			}

			for _, f := range usersModel.Fields {
				want, ok := fieldMappings[f.ColumnName]
				if !ok {
					continue
				}
				gotGoBaseType, wantGoBaseType := f.GoBaseType, want.goType
				if gotGoBaseType != wantGoBaseType {
					t.Errorf(
						"%s: got %q, want %q",
						f.ColumnName, gotGoBaseType, wantGoBaseType,
					)
				}
				gotIsPtr, wantIsPtr := f.IsPtr, want.isPtr
				if gotIsPtr != wantIsPtr {
					t.Errorf(
						"%s: got %v, want %v",
						f.ColumnName, gotIsPtr, wantIsPtr,
					)
				}
			}

			// Verify Edges
			if len(usersModel.Edges) != 1 {
				t.Errorf("got %d edges, want 1", len(usersModel.Edges))
			} else {
				edge := usersModel.Edges[0]
				got, want := edge.Name, "UserProfile"
				if got != want {
					t.Errorf("got %q, want %q", got, want)
				}
				got, want = edge.TargetModel, "Profile"
				if got != want {
					t.Errorf("got %q, want %q", got, want)
				}
			}

			// Verify Imports
			imports := sgg.ImportList()
			expectedImports := map[string]bool{
				"github.com/google/uuid": true,
			}
			for _, imp := range imports {
				delete(expectedImports, imp)
			}
			if len(expectedImports) > 0 {
				t.Errorf("missing imports: %v", expectedImports)
			}
		})

	t.Run("handles complex data types mapping", func(t *testing.T) {
		db := buildComplexDatabase()

		sg, _ := ast.NewSchemaGraph(db.GetPostgres())
		lang := gen.Language{
			Name: "go", Options: gen.GenerateOptions{},
			Validate: func(o gen.GenerateOptions) error { return nil },
		}
		sgg, _ := ast.NewSchemaGraphGo(db, sg, lang)

		model := findModelGo(sgg.Models, "ComplexType")
		if model == nil {
			t.Fatal("ComplexTypes model not found")
		}

		expected := map[string]string{
			"dec":       "decimal.Decimal",
			"ts":        "time.Time",
			"ip":        "netip.Addr",
			"json_data": "json.RawMessage",
			"int_range": "pgtype.Range[int32]",
		}

		for _, f := range model.Fields {
			want, ok := expected[f.ColumnName]
			if !ok {
				continue
			}
			if f.GoBaseType != want {
				t.Errorf("%s: got %q, want %q", f.ColumnName, f.GoBaseType, want)
			}
		}

		// Verify imports
		imports := sgg.ImportList()
		required := []string{
			"github.com/shopspring/decimal",
			"time",
			"net/netip",
			"encoding/json",
			"github.com/jackc/pgx/v5/pgtype",
		}
		for _, req := range required {
			found := false
			for _, imp := range imports {
				if imp == req {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("missing required import: %s", req)
			}
		}
	})
}

func buildTestDatabase() *core.Database {
	return &core.Database{
		Name: "my_app",
		Engine: &core.Database_Postgres{
			Postgres: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Enums: []*postgres.EnumDefinition{
							{
								Name:   "user_role",
								Values: []string{"admin", "user"},
							},
						},
						Domains: []*postgres.DomainDefinition{
							{
								Name: "email_address",
								BaseType: &postgres.DataType{
									Type: &postgres.DataType_TextType{
										TextType: &postgres.TextType{},
									},
								},
							},
						},
						Composites: []*postgres.CompositeDefinition{
							{
								Name: "address",
								Fields: []*postgres.CompositeField{
									{
										Name: "city",
										Type: &postgres.DataType{
											Type: &postgres.DataType_TextType{
												TextType: &postgres.TextType{},
											},
										},
									},
								},
							},
						},
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
										IsNullable: false,
									},
									{
										Name: "role",
										Type: &postgres.DataType{
											Type: &postgres.DataType_EnumType{
												EnumType: &postgres.EnumReference{
													Name: "user_role",
												},
											},
										},
										IsNullable: false,
									},
									{
										Name: "email",
										Type: &postgres.DataType{
											Type: &postgres.DataType_DomainType{
												DomainType: &postgres.DomainReference{
													Name: "email_address",
												},
											},
										},
										IsNullable: true,
									},
									{
										Name: "home_addr",
										Type: &postgres.DataType{
											Type: &postgres.DataType_CompositeType{
												CompositeType: &postgres.CompositeReference{
													Name: "address",
												},
											},
										},
										IsNullable: true,
									},
									{
										Name: "tags",
										Type: &postgres.DataType{
											Type: &postgres.DataType_ArrayType{
												ArrayType: &postgres.ArrayType{
													ElementType: &postgres.DataType{
														Type: &postgres.DataType_TextType{
															TextType: &postgres.TextType{},
														},
													},
												},
											},
										},
										IsNullable: true,
									},
								},
								Relations: []*shared.Relation{
									{
										Name:        "user_profile",
										TargetTable: "profiles",
										Type:        shared.RelationType_RELATION_TYPE_ONE_TO_ONE,
										Columns: []*shared.RelationColumnMapping{
											{
												SourceColumn: "id",
												TargetColumn: "user_id",
											},
										},
									},
								},
							},
							{
								Name: "profiles",
								Columns: []*postgres.Column{
									{
										Name: "user_id",
										Type: &postgres.DataType{
											Type: &postgres.DataType_UuidType{
												UuidType: &postgres.UuidType{},
											},
										},
										IsNullable: false,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func buildComplexDatabase() *core.Database {
	return &core.Database{
		Name: "complex_app",
		Engine: &core.Database_Postgres{
			Postgres: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "complex_types",
								Columns: []*postgres.Column{
									{
										Name: "dec",
										Type: &postgres.DataType{
											Type: &postgres.DataType_NumericType{
												NumericType: &postgres.NumericType{},
											},
										},
									},
									{
										Name: "ts",
										Type: &postgres.DataType{
											Type: &postgres.DataType_TimestamptzType{
												TimestamptzType: &postgres.TimestampTzType{},
											},
										},
									},
									{
										Name: "ip",
										Type: &postgres.DataType{
											Type: &postgres.DataType_InetType{
												InetType: &postgres.InetType{},
											},
										},
									},
									{
										Name: "json_data",
										Type: &postgres.DataType{
											Type: &postgres.DataType_JsonbType{
												JsonbType: &postgres.JsonbType{},
											},
										},
									},
									{
										Name: "int_range",
										Type: &postgres.DataType{
											Type: &postgres.DataType_Int4RangeType{
												Int4RangeType: &postgres.Int4RangeType{},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func findModelGo(models []*ast.ModelGo, name string) *ast.ModelGo {
	for _, m := range models {
		if m.Name == name {
			return m
		}
	}
	return nil
}
