package postgres_test

import (
  "context"
  "testing"

  "github.com/google/go-cmp/cmp"
  "github.com/google/go-cmp/cmp/cmpopts"
  "github.com/gotidy/ptr"
  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/genproto/core/shared"
  genpg "github.com/uthereal/scheme/genproto/postgres"
  "github.com/uthereal/scheme/migrate/postgres"
)

func TestNewDatabaseStateFromProto(t *testing.T) {
  t.Run("nil proto", func(t *testing.T) {
    _, err := postgres.NewDatabaseStateFromProto(nil)
    assert.Error(t, err, "expected error on nil proto")
  })

  t.Run("foreign key unspecified and dot notation", func(t *testing.T) {
    state, err := postgres.NewDatabaseStateFromProto(&genpg.PostgresDatabase{
      Schemas: []*genpg.PostgresSchema{
        {
          Name: "public",
          Tables: []*genpg.Table{
            {
              Name: "posts",
              ForeignKeys: []*shared.ForeignKey{
                {
                  Name: "fk_author",
                  TargetTable: &shared.TableReference{
                    Schema: "auth",
                    Name:   "users",
                  },
                  Columns: []*shared.ColumnMapping{
                    {
                      SourceColumn: "author_id",
                      TargetColumn: "id",
                    },
                  },
                  OnUpdate: shared.ForeignKeyAction_FOREIGN_KEY_ACTION_UNSPECIFIED,
                  OnDelete: shared.ForeignKeyAction_FOREIGN_KEY_ACTION_UNSPECIFIED,
                },
              },
            },
          },
        },
      },
    })
    require.NoError(t, err)

    fk := state.Schemas["public"].Tables["posts"].ForeignKeys["fk_author"]
    assert.Equal(t, "NO ACTION", string(fk.OnUpdate))
    assert.Equal(t, "NO ACTION", string(fk.OnDelete))
    assert.Equal(t, "auth", fk.TargetSchema)
    assert.Equal(t, "users", fk.TargetTable)
  })

  t.Run("successful comprehensive mapping", func(t *testing.T) {
    proto := &genpg.PostgresDatabase{
      Schemas: []*genpg.PostgresSchema{
        {
          Name:         "public",
          NamePrevious: "old_public",
          Enums: []*genpg.EnumDefinition{
            {
              Name:         "status",
              NamePrevious: "old_status",
              Values:       []string{"active", "inactive"},
            },
          },
          Domains: []*genpg.DomainDefinition{
            {
              Name: "email",
              BaseType: &genpg.DataType{
                Type: &genpg.DataType_VarcharType{
                  VarcharType: &genpg.VarcharType{
                    Length: ptr.Int32(255),
                  },
                },
              },
            },
          },
          Composites: []*genpg.CompositeDefinition{
            {
              Name: "address",
              Fields: []*genpg.CompositeField{
                {
                  Name: "street",
                  Type: &genpg.DataType{
                    Type: &genpg.DataType_TextType{},
                  },
                },
              },
            },
          },
          Tables: []*genpg.Table{
            {
              Name:        "users",
              PrimaryKeys: []string{"id"},
              Columns: []*genpg.Column{
                {
                  Name: "id",
                  Type: &genpg.DataType{
                    Type: &genpg.DataType_UuidType{},
                  },
                },
                {
                  Name: "seq",
                  Type: &genpg.DataType{
                    Type: &genpg.DataType_BigserialType{},
                  },
                },
                {
                  Name: "role",
                  Type: &genpg.DataType{
                    Type: &genpg.DataType_TextType{},
                  },
                  DefaultValue: "'user'::text",
                },
              },
              Indexes: []*genpg.Index{
                {
                  Name:     "idx_users_id",
                  Columns:  []*genpg.IndexColumn{{Name: "id"}},
                  IsUnique: true,
                },
              },
              ForeignKeys: []*shared.ForeignKey{
                {
                  Name: "fk_profile",
                  TargetTable: &shared.TableReference{
                    Name: "profiles",
                  },
                  Columns: []*shared.ColumnMapping{
                    {
                      SourceColumn: "id",
                      TargetColumn: "user_id",
                    },
                  },
                },
              },
            },
          },
        },
      },
    }

    state, err := postgres.NewDatabaseStateFromProto(proto)
    require.NoError(t, err)
    require.NotNil(t, state)

    s, ok := state.Schemas["public"]
    require.True(t, ok, "expected public schema")
    assert.Equal(t, "old_public", s.NamePrevious)

    assert.Contains(t, s.Enums, "status")
    assert.Contains(t, s.Domains, "email")
    assert.Contains(t, s.Composites, "address")

    u, ok := s.Tables["users"]
    require.True(t, ok, "expected table users")

    require.NotNil(t, u.PrimaryKey)
    assert.Equal(t, []string{"id"}, u.PrimaryKey.Columns)

    assert.Contains(t, u.Columns, "id")
    seqCol, okSeq := u.Columns["seq"]
    assert.True(t, okSeq && seqCol.IsAutoIncrement)

    roleCol, okRole := u.Columns["role"]
    require.True(t, okRole)
    require.NotNil(t, roleCol.ColumnDefault)
    assert.Equal(t, "'user'", *roleCol.ColumnDefault)

    assert.Contains(t, u.Indexes, "idx_users_id")
    assert.Contains(t, u.ForeignKeys, "fk_profile")
  })

  tests := []struct {
    name  string
    proto *genpg.PostgresDatabase
  }{
    {
      name: "domain invalid datatype",
      proto: &genpg.PostgresDatabase{
        Schemas: []*genpg.PostgresSchema{
          {
            Name: "public",
            Domains: []*genpg.DomainDefinition{
              {
                Name:     "invalid",
                BaseType: nil,
              },
            },
          },
        },
      },
    },
    {
      name: "composite field invalid datatype",
      proto: &genpg.PostgresDatabase{
        Schemas: []*genpg.PostgresSchema{
          {
            Name: "public",
            Composites: []*genpg.CompositeDefinition{
              {
                Name: "address",
                Fields: []*genpg.CompositeField{
                  {Name: "street", Type: nil},
                },
              },
            },
          },
        },
      },
    },
    {
      name: "column invalid datatype",
      proto: &genpg.PostgresDatabase{
        Schemas: []*genpg.PostgresSchema{
          {
            Name: "public",
            Tables: []*genpg.Table{
              {
                Name: "users",
                Columns: []*genpg.Column{
                  {Name: "id", Type: nil},
                },
              },
            },
          },
        },
      },
    },
    {
      name: "nullable primary key",
      proto: &genpg.PostgresDatabase{
        Schemas: []*genpg.PostgresSchema{
          {
            Name: "public",
            Tables: []*genpg.Table{
              {
                Name:        "users",
                PrimaryKeys: []string{"id"},
                Columns: []*genpg.Column{
                  {
                    Name: "id",
                    Type: &genpg.DataType{
                      Type: &genpg.DataType_UuidType{},
                    },
                    IsNullable: true,
                  },
                },
              },
            },
          },
        },
      },
    },
  }

  for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
      _, err := postgres.NewDatabaseStateFromProto(tt.proto)
      assert.Error(t, err, "expected error for %q", tt.name)
    })
  }
}

func TestNewDatabaseStateFromDb(t *testing.T) {
  t.Run("nil db", func(t *testing.T) {
    ctx := context.Background()
    _, err := postgres.NewDatabaseStateFromDb(ctx, nil)
    assert.Error(t, err)
  })

  t.Run("exhaustive combinations", func(t *testing.T) {
    ctx := context.Background()

    db, cleanup, err := testContainer.CreateIsolatedDB(ctx, t)
    require.NoError(t, err, "failed to create isolated db")
    defer func() {
      _ = cleanup()
    }()

    setupSQL := []string{
      `CREATE SCHEMA custom_schema;`,
      `CREATE TYPE user_status AS ENUM ('ACTIVE', 'INACTIVE', 'BANNED');`,
      "CREATE TYPE address AS (street varchar(255), " +
        "city varchar(100), zip varchar(20));",
      `CREATE DOMAIN email_address AS varchar(255);`,
      `CREATE TABLE users (
					id uuid NOT NULL DEFAULT gen_random_uuid(),
					username varchar(50) NOT NULL,
					status user_status NOT NULL DEFAULT 'ACTIVE',
					billing_address address NULL,
					CONSTRAINT users_pkey PRIMARY KEY (id)
				 );`,
      `CREATE UNIQUE INDEX users_username_key ON users (username);`,
      `CREATE INDEX users_status_idx ON users (status, username);`,
      "CREATE TABLE custom_schema.posts (\n" +
        "  id bigserial NOT NULL,\n" +
        "  author_id uuid,\n" +
        "  CONSTRAINT posts_pkey PRIMARY KEY (id),\n" +
        "  CONSTRAINT fk_posts_author FOREIGN KEY (author_id) " +
        "REFERENCES public.users(id) ON DELETE SET NULL ON UPDATE CASCADE\n);",
      "CREATE TABLE custom_schema.post_tags (\n" +
        "  post_id bigint NOT NULL,\n" +
        "  tag_id varchar(50) NOT NULL,\n" +
        "  CONSTRAINT post_tags_pkey PRIMARY KEY (post_id, tag_id),\n" +
        "  CONSTRAINT fk_post_tags_post FOREIGN KEY (post_id) " +
        "REFERENCES custom_schema.posts(id) ON DELETE CASCADE\n);",
    }

    for _, stmt := range setupSQL {
      _, err = db.ExecContext(ctx, stmt)
      require.NoError(t, err, "failed to execute setup sql %q", stmt)
    }

    state, err := postgres.NewDatabaseStateFromDb(ctx, db)
    require.NoError(t, err)

    wantState := &postgres.DatabaseState{
      Schemas: map[string]*postgres.SchemaState{
        "custom_schema": {
          Name: "custom_schema",
          Tables: map[string]*postgres.TableState{
            "post_tags": {
              Name: "post_tags",
              Columns: map[string]*postgres.ColumnState{
                "post_id": {
                  Name:          "post_id",
                  DataType:      "bigint",
                  IsNullable:    false,
                  ColumnDefault: nil,
                },
                "tag_id": {
                  Name:          "tag_id",
                  DataType:      "character varying(50)",
                  IsNullable:    false,
                  ColumnDefault: nil,
                },
              },
              PrimaryKey: &postgres.PrimaryKeyState{
                Name:    "post_tags_pkey",
                Columns: []string{"post_id", "tag_id"},
              },
              Indexes: map[string]*postgres.IndexState{},
              ForeignKeys: map[string]*postgres.ForeignKeyState{
                "fk_post_tags_post": {
                  Name:         "fk_post_tags_post",
                  TargetTable:  "posts",
                  TargetSchema: "custom_schema",
                  ColsLocal:    []string{"post_id"},
                  ColsTarget:   []string{"id"},
                  OnUpdate:     "NO ACTION",
                  OnDelete:     "CASCADE",
                },
              },
            },
            "posts": {
              Name: "posts",
              Columns: map[string]*postgres.ColumnState{
                "author_id": {
                  Name:       "author_id",
                  DataType:   "uuid",
                  IsNullable: true,
                },
                "id": {
                  Name:            "id",
                  DataType:        "bigint",
                  IsNullable:      false,
                  IsAutoIncrement: true,
                  ColumnDefault: ptr.String(
                    "nextval('custom_schema.posts_id_seq'",
                  ),
                },
              },
              PrimaryKey: &postgres.PrimaryKeyState{
                Name:    "posts_pkey",
                Columns: []string{"id"},
              },
              Indexes: map[string]*postgres.IndexState{},
              ForeignKeys: map[string]*postgres.ForeignKeyState{
                "fk_posts_author": {
                  Name:         "fk_posts_author",
                  TargetTable:  "users",
                  TargetSchema: "public",
                  ColsLocal:    []string{"author_id"},
                  ColsTarget:   []string{"id"},
                  OnUpdate:     "CASCADE",
                  OnDelete:     "SET NULL",
                },
              },
            },
          },
          Enums:      map[string]*postgres.EnumState{},
          Composites: map[string]*postgres.CompositeState{},
          Domains:    map[string]*postgres.DomainState{},
          Functions:  map[string]*postgres.FunctionState{}},
        "public": {
          Name: "public",
          Tables: map[string]*postgres.TableState{
            "users": {
              Name: "users",
              Columns: map[string]*postgres.ColumnState{
                "billing_address": {
                  Name:          "billing_address",
                  DataType:      "public.address",
                  IsNullable:    true,
                  ColumnDefault: nil,
                },
                "id": {
                  Name:          "id",
                  DataType:      "uuid",
                  IsNullable:    false,
                  ColumnDefault: ptr.String("gen_random_uuid()"),
                },
                "status": {
                  Name:          "status",
                  DataType:      "public.user_status",
                  IsNullable:    false,
                  ColumnDefault: ptr.String("'ACTIVE'"),
                },
                "username": {
                  Name:          "username",
                  DataType:      "character varying(50)",
                  IsNullable:    false,
                  ColumnDefault: nil,
                },
              },
              PrimaryKey: &postgres.PrimaryKeyState{
                Name:    "users_pkey",
                Columns: []string{"id"},
              },
              Indexes: map[string]*postgres.IndexState{
                "users_status_idx": {
                  Name:     "users_status_idx",
                  Columns:  []string{"status", "username"},
                  IsUnique: false,
                },
                "users_username_key": {
                  Name:     "users_username_key",
                  Columns:  []string{"username"},
                  IsUnique: true,
                },
              },
              ForeignKeys: map[string]*postgres.ForeignKeyState{},
            },
          },
          Enums: map[string]*postgres.EnumState{
            "user_status": {
              Name:   "user_status",
              Values: []string{"ACTIVE", "INACTIVE", "BANNED"},
            },
          },
          Composites: map[string]*postgres.CompositeState{
            "address": {
              Name: "address",
              Fields: map[string]*postgres.CompositeFieldState{
                "city": {
                  Name: "city", DataType: "character varying(100)", Position: 2,
                },
                "street": {
                  Name:     "street",
                  DataType: "character varying(255)",
                  Position: 1,
                },
                "zip": {
                  Name: "zip", DataType: "character varying(20)", Position: 3,
                },
              },
            },
          },
          Domains: map[string]*postgres.DomainState{
            "email_address": {
              Name:     "email_address",
              DataType: "character varying(255)",
            },
          },
          Functions: map[string]*postgres.FunctionState{},
        },
      },
    }

    diff := cmp.Diff(wantState, state, cmpopts.EquateEmpty())
    assert.Empty(t, diff, "NewDatabaseStateFromDb() mismatch")
  })
}

func TestDatabaseState_Clone(t *testing.T) {
  t.Run("nil receiver", func(t *testing.T) {
    var l *postgres.DatabaseState
    clone := l.Clone()
    assert.Nil(t, clone)
  })

  t.Run("deep copy mutations", func(t *testing.T) {
    original := &postgres.DatabaseState{
      Schemas: map[string]*postgres.SchemaState{
        "public": {
          Name: "public",
          Tables: map[string]*postgres.TableState{
            "users": {
              Name: "users",
              Columns: map[string]*postgres.ColumnState{
                "id": {
                  Name:       "id",
                  DataType:   "uuid",
                  IsNullable: false,
                  ColumnDefault: ptr.String(
                    "gen_random_uuid()",
                  ),
                },
              },
              PrimaryKey: &postgres.PrimaryKeyState{
                Name:    "users_pkey",
                Columns: []string{"id"},
              },
              Indexes: map[string]*postgres.IndexState{
                "users_idx": {
                  Name:     "users_idx",
                  Columns:  []string{"id"},
                  IsUnique: true,
                },
              },
              ForeignKeys: map[string]*postgres.ForeignKeyState{
                "fk_users": {
                  Name:        "fk_users",
                  TargetTable: "roles",
                  ColsLocal:   []string{"id"},
                  ColsTarget:  []string{"other_id"},
                },
              },
            },
          },
          Enums: map[string]*postgres.EnumState{
            "status": {
              Name:   "status",
              Values: []string{"active"},
            },
          },
          Composites: map[string]*postgres.CompositeState{
            "address": {
              Name: "address",
              Fields: map[string]*postgres.CompositeFieldState{
                "city": {Name: "city", DataType: "text"},
              },
            },
          },
          Domains: map[string]*postgres.DomainState{
            "email": {Name: "email", DataType: "text"},
          },
        },
      },
    }

    clone := original.Clone()

    // Mutate original extensively
    original.Schemas["public"].Tables["users"].Columns["id"].
      DataType = "int"
    *original.Schemas["public"].Tables["users"].Columns["id"].
      ColumnDefault = "1"
    original.Schemas["public"].Tables["users"].PrimaryKey.
      Columns[0] = "mutated"
    original.Schemas["public"].Tables["users"].Indexes["users_idx"].
      Columns[0] = "mutated"
    original.Schemas["public"].Tables["users"].
      ForeignKeys["fk_users"].ColsLocal[0] = "mutated"
    original.Schemas["public"].Enums["status"].
      Values[0] = "mutated"
    original.Schemas["public"].Composites["address"].
      Fields["city"].DataType = "mutated"
    original.Schemas["public"].Domains["email"].
      DataType = "mutated"

    // Assert clone is completely untouched
    dt := clone.Schemas["public"].Tables["users"].Columns["id"].DataType
    assert.Equal(t, "uuid", string(dt))

    def := clone.Schemas["public"].Tables["users"].Columns["id"].
      ColumnDefault
    assert.Equal(t, "gen_random_uuid()", *def)

    pk := clone.Schemas["public"].Tables["users"].PrimaryKey
    assert.Equal(t, "id", pk.Columns[0])

    idx := clone.Schemas["public"].Tables["users"].Indexes["users_idx"]
    assert.Equal(t, "id", idx.Columns[0])

    fk := clone.Schemas["public"].Tables["users"].ForeignKeys["fk_users"]
    assert.Equal(t, "id", fk.ColsLocal[0])

    en := clone.Schemas["public"].Enums["status"]
    assert.Equal(t, "active", en.Values[0])

    city := clone.Schemas["public"].Composites["address"].Fields["city"]
    assert.Equal(t, "text", string(city.DataType))

    dom := clone.Schemas["public"].Domains["email"]
    assert.Equal(t, "text", string(dom.DataType))
  })
}
