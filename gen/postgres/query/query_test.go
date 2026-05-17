package query_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/gen"
  "github.com/uthereal/scheme/gen/postgres/query"
  "github.com/uthereal/scheme/genproto"
  "github.com/uthereal/scheme/genproto/core"
  "github.com/uthereal/scheme/genproto/postgres"
)

func TestGenerateQueryBuilders(t *testing.T) {
  validLang := gen.LangGo
  validScheme := &genproto.Scheme{
    Go: &genproto.GoLanguage{PackagePath: "github.com/foo/bar"},
  }

  tests := []struct {
    name    string
    db      *core.Database
    lang    gen.Language
    scheme  *genproto.Scheme
    wantErr bool
  }{
    {
      name:    "returns error on nil database",
      db:      nil,
      lang:    validLang,
      scheme:  validScheme,
      wantErr: true,
    },
    {
      name: "returns error on nil postgres definition",
      db: &core.Database{
        Name: "mydb",
      },
      lang:    validLang,
      scheme:  validScheme,
      wantErr: true,
    },
    {
      name: "returns error on invalid language options",
      db: &core.Database{
        Name: "mydb",
        Engine: &core.Database_Postgres{
          Postgres: &postgres.PostgresDatabase{},
        },
      },
      lang:    validLang,
      scheme:  &genproto.Scheme{Go: &genproto.GoLanguage{PackagePath: ""}},
      wantErr: true,
    },
    {
      name: "successfully generates queries for valid schema",
      db: &core.Database{
        Name: "mydb",
        Engine: &core.Database_Postgres{
          Postgres: &postgres.PostgresDatabase{},
        },
      },
      lang:    validLang,
      scheme:  validScheme,
      wantErr: false,
    },
    {
      name: "returns error on unsupported language",
      db: &core.Database{
        Name: "mydb",
        Engine: &core.Database_Postgres{
          Postgres: &postgres.PostgresDatabase{},
        },
      },
      lang:    gen.Language{Name: "unsupported"},
      scheme:  validScheme,
      wantErr: true,
    },
    {
      name: "successfully generates schema-qualified function calls",
      db: &core.Database{
        Name: "mydb",
        Engine: &core.Database_Postgres{
          Postgres: &postgres.PostgresDatabase{
            Schemas: []*postgres.PostgresSchema{
              {
                Name: "myschema",
                Functions: []*postgres.FunctionDefinition{
                  {
                    Name: "myfunc",
                    ReturnType: &postgres.DataType{
                      Type: &postgres.DataType_IntegerType{
                        IntegerType: &postgres.IntegerType{},
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      lang:    validLang,
      scheme:  validScheme,
      wantErr: false,
    },
  }

  for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
      got, err := query.GenerateQueryBuilders(tt.db, tt.lang, tt.scheme)

      if tt.wantErr {
        assert.Error(t, err)
      } else {
        require.NoError(t, err)
        if tt.name == "successfully generates schema-qualified function calls" {
          assert.Contains(t, got, `query.WriteString("SELECT * FROM \"myschema\".\"myfunc\"(")`)
        }
      }
    })
  }
}
