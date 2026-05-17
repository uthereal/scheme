package ast_test

import (
  "testing"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/gen/postgres/ast"
  "github.com/uthereal/scheme/genproto/core/shared"
  "github.com/uthereal/scheme/genproto/postgres"
)

func TestNewSchemaGraph(t *testing.T) {
  t.Run("returns error on nil schema", func(t *testing.T) {
    _, err := ast.NewSchemaGraph(nil)
    assert.Error(t, err, "expected error when schema is nil")
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
    require.NoError(t, err)

    assert.Len(t, sg.Models, 1)

    model, ok := sg.Models["public.users"]
    require.True(t, ok)
    if ok {
      assert.Equal(t, "User", model.Name)
      assert.Equal(t, "\"public\".\"users\"", model.TableFullName)
    }
  })

  t.Run("extracts enums, composites, and domains", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Tables: []*postgres.Table{
            {
              Name: "profiles",
              Columns: []*postgres.Column{
                {
                  Name: "id",
                  Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{
                      UuidType: &postgres.UuidType{},
                    },
                  },
                },
                {
                  Name: "status",
                  Type: &postgres.DataType{
                    Type: &postgres.DataType_EnumType{
                      EnumType: &postgres.EnumReference{
                        Name:   "user_status",
                        Schema: "public",
                      },
                    },
                  },
                },
                {
                  Name: "contact",
                  Type: &postgres.DataType{
                    Type: &postgres.DataType_CompositeType{
                      CompositeType: &postgres.CompositeReference{
                        Name:   "contact_info",
                        Schema: "public",
                      },
                    },
                  },
                },
                {
                  Name: "email_address",
                  Type: &postgres.DataType{
                    Type: &postgres.DataType_DomainType{
                      DomainType: &postgres.DomainReference{
                        Name:   "email",
                        Schema: "public",
                      },
                    },
                  },
                },
              },
            },
          },
          Enums: []*postgres.EnumDefinition{
            {
              Name:   "user_status",
              Values: []string{"active", "inactive"},
            },
          },
          Composites: []*postgres.CompositeDefinition{
            {
              Name: "contact_info",
              Fields: []*postgres.CompositeField{
                {
                  Name: "email",
                  Type: &postgres.DataType{
                    Type: &postgres.DataType_TextType{
                      TextType: &postgres.TextType{},
                    },
                  },
                },
              },
            },
          },
          Domains: []*postgres.DomainDefinition{
            {
              Name: "email",
              BaseType: &postgres.DataType{
                Type: &postgres.DataType_TextType{
                  TextType: &postgres.TextType{},
                },
              },
            },
          },
        },
      },
    }

    sg, err := ast.NewSchemaGraph(pgDb)
    require.NoError(t, err)

    assert.NotEmpty(t, sg.Enums)
    assert.NotEmpty(t, sg.Composites)
    assert.NotEmpty(t, sg.Domains)

    assert.Contains(t, sg.Enums, "public.user_status")
    assert.Contains(t, sg.Composites, "public.contact_info")
    assert.Contains(t, sg.Domains, "public.email")
  })

  t.Run("resolves naming collision with schema prefix", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Tables: []*postgres.Table{
            {Name: "users"},
          },
        },
        {
          Name: "auth",
          Tables: []*postgres.Table{
            {Name: "users"},
          },
        },
      },
    }

    sg, err := ast.NewSchemaGraph(pgDb)
    require.NoError(t, err)

    assert.Equal(t, "PublicUser", sg.Models["public.users"].Name)
    assert.Equal(t, "AuthUser", sg.Models["auth.users"].Name)
  })

  t.Run("resolves naming collision for enums", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Enums: []*postgres.EnumDefinition{
            {Name: "status", Values: []string{"active"}},
          },
        },
        {
          Name: "auth",
          Enums: []*postgres.EnumDefinition{
            {Name: "status", Values: []string{"active"}},
          },
        },
      },
    }

    sg, err := ast.NewSchemaGraph(pgDb)
    require.NoError(t, err)

    assert.Equal(t, "PublicStatus", sg.Enums["public.status"].Name)
    assert.Equal(t, "AuthStatus", sg.Enums["auth.status"].Name)
  })

  t.Run("handles relationships and back-references", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Tables: []*postgres.Table{
            {
              Name: "users",
              Columns: []*postgres.Column{
                {Name: "id", Type: &postgres.DataType{
                  Type: &postgres.DataType_UuidType{},
                }},
              },
            },
            {
              Name: "posts",
              Columns: []*postgres.Column{
                {Name: "id", Type: &postgres.DataType{
                  Type: &postgres.DataType_UuidType{},
                }},
                {Name: "author_id", Type: &postgres.DataType{
                  Type: &postgres.DataType_UuidType{},
                }},
              },
              Relations: []*shared.Relation{
                {
                  Name:        "author",
                  TargetTable: &shared.TableReference{Name: "users"},
                  Type:        shared.RelationType_RELATION_TYPE_MANY_TO_ONE,
                  Columns: []*shared.ColumnMapping{
                    {
                      SourceColumn: "author_id",
                      TargetColumn: "id",
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
    require.NoError(t, err)

    userModel := sg.Models["public.users"]
    postModel := sg.Models["public.posts"]

    // Check Post -> User (Author)
    require.Len(t, postModel.Edges, 1)
    authorEdge := postModel.Edges[0]
    assert.Equal(t, "Author", authorEdge.Name)
    assert.Equal(t, "User", authorEdge.TargetModel)
    assert.False(t, authorEdge.IsSlice)

    // Check User -> Post (BackRef)
    require.Len(t, userModel.Edges, 1)
    postsEdge := userModel.Edges[0]
    assert.Equal(t, "AuthorPosts", postsEdge.Name)
    assert.Equal(t, "Post", postsEdge.TargetModel)
    assert.True(t, postsEdge.IsSlice)
  })

  t.Run(
    "handles ONE_TO_ONE back-reference naming correctly",
    func(t *testing.T) {
      pgDb := &postgres.PostgresDatabase{
        Schemas: []*postgres.PostgresSchema{
          {
            Name: "public",
            Tables: []*postgres.Table{
              {
                Name: "users",
                Columns: []*postgres.Column{
                  {Name: "id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                },
              },
              {
                Name: "user_profiles",
                Columns: []*postgres.Column{
                  {Name: "id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                  {Name: "user_id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                },
                Relations: []*shared.Relation{
                  {
                    Name:        "user",
                    TargetTable: &shared.TableReference{Name: "users"},
                    Type:        shared.RelationType_RELATION_TYPE_ONE_TO_ONE,
                    Columns: []*shared.ColumnMapping{
                      {
                        SourceColumn: "user_id",
                        TargetColumn: "id",
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
      require.NoError(t, err)

      userModel := sg.Models["public.users"]
      require.Len(t, userModel.Edges, 1)
      profileEdge := userModel.Edges[0]

      assert.False(t, profileEdge.IsSlice)
      assert.Condition(t, func() bool {
        return profileEdge.Name == "UserProfile" ||
          profileEdge.Name == "UserUserProfile"
      }, "backref name should be singular")
    })

  t.Run(
    "handles ONE_TO_MANY forward edge and back-reference correctly",
    func(t *testing.T) {
      pgDb := &postgres.PostgresDatabase{
        Schemas: []*postgres.PostgresSchema{
          {
            Name: "public",
            Tables: []*postgres.Table{
              {
                Name: "users",
                Columns: []*postgres.Column{
                  {Name: "id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                },
                Relations: []*shared.Relation{
                  {
                    Name:        "posts",
                    TargetTable: &shared.TableReference{Name: "posts"},
                    Type:        shared.RelationType_RELATION_TYPE_ONE_TO_MANY,
                    Columns: []*shared.ColumnMapping{
                      {
                        SourceColumn: "id",
                        TargetColumn: "author_id",
                      },
                    },
                  },
                },
              },
              {
                Name: "posts",
                Columns: []*postgres.Column{
                  {Name: "id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                  {Name: "author_id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                },
              },
            },
          },
        },
      }

      sg, err := ast.NewSchemaGraph(pgDb)
      require.NoError(t, err)

      userModel := sg.Models["public.users"]
      postModel := sg.Models["public.posts"]

      // Forward edge: User -> Posts (ONE_TO_MANY)
      require.Len(t, userModel.Edges, 1)
      postsEdge := userModel.Edges[0]
      assert.Equal(t, "Posts", postsEdge.Name)
      assert.True(t, postsEdge.IsSlice)

      // Back-reference: Post -> User (MANY_TO_ONE)
      require.Len(t, postModel.Edges, 1)
      userEdge := postModel.Edges[0]
      assert.Equal(t, "PostsUser", userEdge.Name)
      assert.False(t, userEdge.IsSlice)
    })

  t.Run("resolves cross-type naming collision", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Tables: []*postgres.Table{
            {Name: "status"}, // Table 'status' -> Status
          },
        },
        {
          Name: "auth",
          Enums: []*postgres.EnumDefinition{
            {
              Name:   "status",
              Values: []string{"active"},
            }, // Enum 'status' -> Status
          },
        },
      },
    }

    sg, err := ast.NewSchemaGraph(pgDb)
    require.NoError(t, err)

    assert.Equal(t, "PublicStatus", sg.Models["public.status"].Name)
    assert.Equal(t, "AuthStatus", sg.Enums["auth.status"].Name)
  })

  t.Run("returns error on unresolved naming collision", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Tables: []*postgres.Table{
            {Name: "users"}, // -> User
          },
        },
        {
          Name: "auth",
          Tables: []*postgres.Table{
            {Name: "user"}, // -> User
          },
        },
      },
    }

    _, err := ast.NewSchemaGraph(pgDb)
    assert.Error(t, err, "expected error on unresolved naming collision")
  })

  t.Run(
    "successfully handles non-colliding names across schemas",
    func(t *testing.T) {
      pgDb := &postgres.PostgresDatabase{
        Schemas: []*postgres.PostgresSchema{
          {
            Name: "public",
            Tables: []*postgres.Table{
              {Name: "users"}, // -> User
            },
          },
          {
            Name: "auth",
            Tables: []*postgres.Table{
              {Name: "accounts"}, // -> Account
            },
          },
        },
      }

      sg, err := ast.NewSchemaGraph(pgDb)
      require.NoError(t, err)

      assert.Equal(t, "User", sg.Models["public.users"].Name)
      assert.Equal(t, "Account", sg.Models["auth.accounts"].Name)
    })

  t.Run("handles MANY_TO_MANY relationships correctly", func(t *testing.T) {
    pgDb := &postgres.PostgresDatabase{
      Schemas: []*postgres.PostgresSchema{
        {
          Name: "public",
          Tables: []*postgres.Table{
            {
              Name: "users",
              Columns: []*postgres.Column{
                {Name: "id", Type: &postgres.DataType{
                  Type: &postgres.DataType_UuidType{},
                }},
              },
              Relations: []*shared.Relation{
                {
                  Name:        "roles",
                  TargetTable: &shared.TableReference{Name: "roles"},
                  Type:        shared.RelationType_RELATION_TYPE_MANY_TO_MANY,
                  Columns: []*shared.ColumnMapping{
                    {SourceColumn: "id", TargetColumn: "id"},
                  },
                },
              },
            },
            {
              Name: "roles",
              Columns: []*postgres.Column{
                {Name: "id", Type: &postgres.DataType{
                  Type: &postgres.DataType_UuidType{},
                }},
              },
            },
          },
        },
      },
    }

    sg, err := ast.NewSchemaGraph(pgDb)
    require.NoError(t, err)

    userModel := sg.Models["public.users"]
    roleModel := sg.Models["public.roles"]

    // Forward: User -> Roles (Slice)
    require.Len(t, userModel.Edges, 1)
    rolesEdge := userModel.Edges[0]
    assert.Equal(t, "Roles", rolesEdge.Name)
    assert.True(t, rolesEdge.IsSlice)

    // Backward: Role -> RolesUsers (Slice)
    require.Len(t, roleModel.Edges, 1)
    usersEdge := roleModel.Edges[0]
    assert.Equal(t, "RolesUsers", usersEdge.Name)
    assert.True(t, usersEdge.IsSlice)
  })

  t.Run(
    "handles multiple relations to the same target table",
    func(t *testing.T) {
      pgDb := &postgres.PostgresDatabase{
        Schemas: []*postgres.PostgresSchema{
          {
            Name: "public",
            Tables: []*postgres.Table{
              {
                Name: "users",
                Columns: []*postgres.Column{
                  {Name: "id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                },
              },
              {
                Name: "messages",
                Columns: []*postgres.Column{
                  {Name: "id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                  {Name: "sender_id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                  {Name: "receiver_id", Type: &postgres.DataType{
                    Type: &postgres.DataType_UuidType{},
                  }},
                },
                Relations: []*shared.Relation{
                  {
                    Name:        "sender",
                    TargetTable: &shared.TableReference{Name: "users"},
                    Type:        shared.RelationType_RELATION_TYPE_MANY_TO_ONE,
                    Columns: []*shared.ColumnMapping{{
                      SourceColumn: "sender_id", TargetColumn: "id",
                    }},
                  },
                  {
                    Name:        "receiver",
                    TargetTable: &shared.TableReference{Name: "users"},
                    Type:        shared.RelationType_RELATION_TYPE_MANY_TO_ONE,
                    Columns: []*shared.ColumnMapping{{
                      SourceColumn: "receiver_id", TargetColumn: "id",
                    }},
                  },
                },
              },
            },
          },
        },
      }

      sg, err := ast.NewSchemaGraph(pgDb)
      require.NoError(t, err)

      userModel := sg.Models["public.users"]
      require.Len(t, userModel.Edges, 2)

      // Names: "SenderMessages" and "ReceiverMessages"
      edgeNames := make(map[string]bool)
      for _, e := range userModel.Edges {
        edgeNames[e.Name] = true
      }

      assert.True(t, edgeNames["SenderMessages"])
      assert.True(t, edgeNames["ReceiverMessages"])
    })
}
