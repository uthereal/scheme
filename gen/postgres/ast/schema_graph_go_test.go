package ast_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/gen/postgres/ast"
  "github.com/uthereal/scheme/genproto"
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

    goOpts := &genproto.GoLanguage{PackagePath: "github.com/test/pkg"}
    _, err := ast.NewSchemaGraphGo(nil, sg, goOpts)

    assert.Error(t, err, "expected error when database is nil")
  })

  t.Run("successfully wraps generic schema graph with all types",
    func(t *testing.T) {
      db := buildTestDatabase()

      pgDb := db.GetPostgres()
      sg, err := ast.NewSchemaGraph(pgDb)
      require.NoError(t, err, "failed parsing generic graph")

      goOpts := &genproto.GoLanguage{PackagePath: "github.com/test/pkg"}
      sgg, err := ast.NewSchemaGraphGo(db, sg, goOpts)
      require.NoError(t, err, "failed wrapping go graph")

      // Verify Models
      assert.Len(t, sgg.Models, 2)

      // Verify Enums
      if assert.Len(t, sgg.Enums, 1) {
        assert.Equal(t, "UserRole", sgg.Enums[0].Name)
        if assert.Len(t, sgg.Enums[0].Values, 2) {
          assert.Equal(t, "UserRoleAdmin", sgg.Enums[0].Values[0].Name)
          assert.Equal(t, "admin", sgg.Enums[0].Values[0].Value)
          assert.Equal(t, "UserRoleUser", sgg.Enums[0].Values[1].Name)
          assert.Equal(t, "user", sgg.Enums[0].Values[1].Value)
        }
      }

      // Verify Domains
      if assert.Len(t, sgg.Domains, 1) {
        dom := sgg.Domains[0]
        assert.Equal(t, "EmailAddress", dom.Name)
        assert.Equal(t, "string", dom.BaseGoType)
      }

      // Verify Composites
      if assert.Len(t, sgg.Composites, 1) {
        assert.Equal(t, "Address", sgg.Composites[0].Name)
      }

      // Verify Field mapping in "Users" model
      usersModel := findModelGo(sgg.Models, "User")
      require.NotNil(t, usersModel, "User model not found")

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
        assert.Equal(t, want.goType, f.GoBaseType, f.ColumnName)
        assert.Equal(t, want.isPtr, f.IsPtr, f.ColumnName)
      }

      // Verify Edges
      if assert.Len(t, usersModel.Edges, 1) {
        edge := usersModel.Edges[0]
        assert.Equal(t, "UserProfile", edge.Name)
        assert.Equal(t, "Profile", edge.TargetModel)
      }

      // Verify Imports
      imports := sgg.ImportList()
      assert.Contains(t, imports, "github.com/google/uuid")
    })

  t.Run("handles complex data types mapping", func(t *testing.T) {
    db := buildComplexDatabase()

    sg, _ := ast.NewSchemaGraph(db.GetPostgres())
    goOpts := &genproto.GoLanguage{PackagePath: "github.com/test/pkg"}
    sgg, _ := ast.NewSchemaGraphGo(db, sg, goOpts)

    model := findModelGo(sgg.Models, "ComplexType")
    require.NotNil(t, model, "ComplexTypes model not found")

    expected := map[string]string{
      "dec":       "pgtype.Numeric",
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
      assert.Equal(t, want, f.GoBaseType, f.ColumnName)
    }

    // Verify imports
    imports := sgg.ImportList()
    assert.Contains(t, imports, "time")
    assert.Contains(t, imports, "net/netip")
    assert.Contains(t, imports, "encoding/json")
    assert.Contains(t, imports, "github.com/jackc/pgx/v5/pgtype")
  })

  t.Run("correctly maps empty payload triggers", func(t *testing.T) {
    db := &core.Database{
      Name: "test_db",
      Engine: &core.Database_Postgres{
        Postgres: &postgres.PostgresDatabase{
          Channels: []*postgres.NotificationChannel{
            {
              Name: "my_empty_channel",
              Payload: &postgres.NotificationPayload{
                Payload: &postgres.NotificationPayload_Empty{
                  Empty: &postgres.EmptyPayload{},
                },
              },
            },
          },
          Schemas: []*postgres.PostgresSchema{
            {
              Name: "public",
              Tables: []*postgres.Table{
                {
                  Name: "my_table",
                  Columns: []*postgres.Column{
                    {Name: "id",
                      Type: &postgres.DataType{
                        Type: &postgres.DataType_IntegerType{},
                      },
                    },
                  },
                  NotifyTriggers: []*postgres.NotifyTrigger{
                    {
                      Name:    "my_trigger",
                      Channel: "my_empty_channel",
                    },
                  },
                },
              },
            },
          },
        },
      },
    }

    sg, _ := ast.NewSchemaGraph(db.GetPostgres())
    goOpts := &genproto.GoLanguage{PackagePath: "github.com/test/pkg"}
    sgg, err := ast.NewSchemaGraphGo(db, sg, goOpts)
    require.NoError(t, err)

    require.Len(t, sgg.Triggers, 1)
    assert.True(t, sgg.Triggers[0].IsEmptyPayload)
  })

  t.Run("packs structs by sorting fields by memory alignment", func(t *testing.T) {
    db := &core.Database{
      Name: "test_db",
      Engine: &core.Database_Postgres{
        Postgres: &postgres.PostgresDatabase{
          Schemas: []*postgres.PostgresSchema{
            {
              Name: "public",
              Tables: []*postgres.Table{
                {
                  Name: "packed_table",
                  Columns: []*postgres.Column{
                    {Name: "is_active", Type: &postgres.DataType{Type: &postgres.DataType_BooleanType{}}}, // 1 byte
                    {Name: "id", Type: &postgres.DataType{Type: &postgres.DataType_BigintType{}}},         // 8 bytes
                    {Name: "small_val", Type: &postgres.DataType{Type: &postgres.DataType_SmallintType{}}},// 2 bytes
                    {Name: "name", Type: &postgres.DataType{Type: &postgres.DataType_TextType{}}},         // 8 bytes (string)
                    {Name: "med_val", Type: &postgres.DataType{Type: &postgres.DataType_IntegerType{}}},   // 4 bytes
                  },
                },
              },
            },
          },
        },
      },
    }

    sg, _ := ast.NewSchemaGraph(db.GetPostgres())
    goOpts := &genproto.GoLanguage{PackagePath: "github.com/test/pkg"}
    sgg, err := ast.NewSchemaGraphGo(db, sg, goOpts)
    require.NoError(t, err)

    model := findModelGo(sgg.Models, "PackedTable")
    require.NotNil(t, model)

    // Expected order: 8-byte types, then 4-byte, 2-byte, 1-byte.
    // Within the same weight class, original authoring order is preserved (SortStableFunc).
    // Original authoring: is_active(1), id(8), small_val(2), name(8), med_val(4).
    // Expected sorted: id(8), name(8), med_val(4), small_val(2), is_active(1).
    require.Len(t, model.Fields, 5)
    assert.Equal(t, "id", model.Fields[0].ColumnName)
    assert.Equal(t, "name", model.Fields[1].ColumnName)
    assert.Equal(t, "med_val", model.Fields[2].ColumnName)
    assert.Equal(t, "small_val", model.Fields[3].ColumnName)
    assert.Equal(t, "is_active", model.Fields[4].ColumnName)
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
                    TargetTable: &shared.TableReference{Name: "profiles"},
                    Type:        shared.RelationType_RELATION_TYPE_ONE_TO_ONE,
                    Columns: []*shared.ColumnMapping{
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
