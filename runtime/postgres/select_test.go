package postgres_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/runtime/postgres"
)

func TestBuildSelectString(t *testing.T) {
  tests := []struct {
    name         string
    cols         []postgres.Column
    wantStr      string
    wantRawNames []string
  }{
    {
      name:         "empty columns",
      cols:         []postgres.Column{},
      wantStr:      "",
      wantRawNames: nil,
    },
    {
      name: "simple columns",
      cols: []postgres.Column{
        postgres.BaseColumn("id"),
        postgres.BaseColumn("name"),
      },
      wantStr:      "id, name",
      wantRawNames: []string{"id", "name"},
    },
    {
      name: "table prefixes",
      cols: []postgres.Column{
        postgres.BaseColumn("users.id"),
        postgres.BaseColumn("public.posts.title"),
      },
      wantStr:      "users.id, public.posts.title",
      wantRawNames: []string{"id", "title"},
    },
    {
      name: "aliases with AS",
      cols: []postgres.Column{
        postgres.BaseColumn("id AS user_id"),
        postgres.BaseColumn("users.email AS email_addr"),
      },
      wantStr:      "id AS user_id, users.email AS email_addr",
      wantRawNames: []string{"id", "email"},
    },
    {
      name: "quoted columns",
      cols: []postgres.Column{
        postgres.BaseColumn(`"user_id"`),
        postgres.BaseColumn(`"schema"."table"."column"`),
      },
      wantStr:      `"user_id", "schema"."table"."column"`,
      wantRawNames: []string{"user_id", "column"},
    },
    {
      name: "complex combinations",
      cols: []postgres.Column{
        postgres.BaseColumn(`public.users."id" AS uid`),
        postgres.BaseColumn(`name`),
      },
      wantStr:      `public.users."id" AS uid, name`,
      wantRawNames: []string{"id", "name"},
    },
  }

  for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
      gotStr, gotNames, err := postgres.BuildSelectString(tt.cols)
      require.NoError(t, err)
      assert.Equal(t, tt.wantStr, gotStr)
      assert.Equal(t, tt.wantRawNames, gotNames)
    })
  }
}
