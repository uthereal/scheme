package ast_test

import (
	"testing"

	"github.com/uthereal/scheme/gen/postgres/ast"
	"github.com/uthereal/scheme/genproto/core/shared"
	"github.com/uthereal/scheme/genproto/postgres"
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

		gotModels, wantModels := len(sg.Models), 1
		if gotModels != wantModels {
			t.Errorf("got %d models, want %d", gotModels, wantModels)
		}

		if len(sg.Models) > 0 {
			model := sg.Models["public.users"]
			gotName, wantName := model.Name, "User"
			if gotName != wantName {
				t.Errorf("got struct name %q, want %q", gotName, wantName)
			}

			gotFull, wantFull := model.TableFullName, "\"public\".\"users\""
			if gotFull != wantFull {
				t.Errorf("got TableFullName %q, want %q", gotFull, wantFull)
			}
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
		if err != nil {
			t.Fatalf("NewSchemaGraph failed: %v", err)
		}

		if len(sg.Enums) == 0 {
			t.Error("expected Enums to be populated, but it was empty")
		}
		if len(sg.Composites) == 0 {
			t.Error("expected Composites to be populated, but it was empty")
		}
		if len(sg.Domains) == 0 {
			t.Error("expected Domains to be populated, but it was empty")
		}

		enumKey := "public.user_status"
		_, ok1 := sg.Enums[enumKey]
		if !ok1 {
			t.Errorf("expected enum %q not found", enumKey)
		}

		compKey := "public.contact_info"
		_, ok2 := sg.Composites[compKey]
		if !ok2 {
			t.Errorf("expected composite %q not found", compKey)
		}

		domainKey := "public.email"
		_, ok3 := sg.Domains[domainKey]
		if !ok3 {
			t.Errorf("expected domain %q not found", domainKey)
		}
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
		if err != nil {
			t.Fatalf(
				"expected no error on naming collision, got %v", err,
			)
		}

		got, want := sg.Models["public.users"].Name, "PublicUser"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
		got, want = sg.Models["auth.users"].Name, "AuthUser"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
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
		if err != nil {
			t.Fatalf("expected no error on enum naming collision, got %v", err)
		}

		got, want := sg.Enums["public.status"].Name, "PublicStatus"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
		got, want = sg.Enums["auth.status"].Name, "AuthStatus"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
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
									TargetTable: "users",
									Type:        shared.RelationType_RELATION_TYPE_MANY_TO_ONE,
									Columns: []*shared.RelationColumnMapping{
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
		if err != nil {
			t.Fatalf("NewSchemaGraph failed: %v", err)
		}

		userModel := sg.Models["public.users"]
		postModel := sg.Models["public.posts"]

		// Check Post -> User (Author)
		if len(postModel.Edges) != 1 {
			t.Fatalf("expected 1 edge in post model, got %d", len(postModel.Edges))
		}
		authorEdge := postModel.Edges[0]
		got, want := authorEdge.Name, "Author"
		if got != want {
			t.Errorf("got edge name %q, want %q", got, want)
		}
		got, want = authorEdge.TargetModel, "User"
		if got != want {
			t.Errorf("got target model %q, want %q", got, want)
		}
		if authorEdge.IsSlice != false {
			t.Error("expected IsSlice to be false for MANY_TO_ONE")
		}

		// Check User -> Post (BackRef)
		if len(userModel.Edges) != 1 {
			t.Fatalf("expected 1 edge in user model, got %d", len(userModel.Edges))
		}
		postsEdge := userModel.Edges[0]
		// BackRef name = Pascal(rel.Name) + resTableNames[modelKey]
		// resTableNames[modelKey] = "Post"
		// name = "Author" + "Post" = "AuthorPost" -> plural "AuthorPosts"
		got, want = postsEdge.Name, "AuthorPosts"
		if got != want {
			t.Errorf("got backref name %q, want %q", got, want)
		}
		got, want = postsEdge.TargetModel, "Post"
		if got != want {
			t.Errorf("got target model %q, want %q", got, want)
		}
		if postsEdge.IsSlice != true {
			t.Error("expected IsSlice to be true for backref of MANY_TO_ONE")
		}
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
										TargetTable: "users",
										Type:        shared.RelationType_RELATION_TYPE_ONE_TO_ONE,
										Columns: []*shared.RelationColumnMapping{
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
			if err != nil {
				t.Fatalf("NewSchemaGraph failed: %v", err)
			}

			userModel := sg.Models["public.users"]
			// Forward edge is Profile -> User. Back-ref is User -> Profile.
			// Profile table name is "user_profiles".
			// resTableNames["public.user_profiles"] = "UserProfiles"
			// If bug exists, backRefName = "UserProfile" + "UserProfiles"
			// = "UserUserProfiles" (if name is "user")
			// Wait, rel.GetName() is "user". resTableNames["public.user_profiles"]
			// is "UserProfiles".
			// backRefName = "User" + "UserProfiles" = "UserUserProfiles".
			// Pluralized = "UserUserProfiles".
			// Still plural! Should be "UserProfile" or "UserUserProfile".

			if len(userModel.Edges) != 1 {
				t.Fatalf("expected 1 edge in user model, got %d", len(userModel.Edges))
			}
			profileEdge := userModel.Edges[0]

			// If rel.GetName() is "user", and target is "users" (User).
			// Forward edge (Profile -> User) is named "User".
			// Back-ref (User -> Profile) name should ideally be "UserProfile" or
			// something.
			// Currently it is "UserUserProfiles".

			gotName := profileEdge.Name
			// Ideally we want "UserProfile" or "UserProfiles" (if it's not one-to-one).
			// But for one-to-one it MUST be singular.

			if profileEdge.IsSlice {
				t.Error("expected IsSlice to be false for ONE_TO_ONE back-reference")
			}

			// Let's check if it is singular.
			if gotName != "UserProfile" && gotName != "UserUserProfile" {
				t.Errorf(
					"got backref name %q, want singular (e.g., %q or %q)",
					gotName, "UserProfile", "UserUserProfile",
				)
			}
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
										TargetTable: "posts",
										Type:        shared.RelationType_RELATION_TYPE_ONE_TO_MANY,
										Columns: []*shared.RelationColumnMapping{
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
			if err != nil {
				t.Fatalf("NewSchemaGraph failed: %v", err)
			}

			userModel := sg.Models["public.users"]
			postModel := sg.Models["public.posts"]

			// Forward edge: User -> Posts (ONE_TO_MANY)
			if len(userModel.Edges) != 1 {
				t.Fatalf("expected 1 edge in user model, got %d", len(userModel.Edges))
			}
			postsEdge := userModel.Edges[0]
			got, want := postsEdge.Name, "Posts"
			if got != want {
				t.Errorf("got forward edge name %q, want %q", got, want)
			}
			if !postsEdge.IsSlice {
				t.Error("expected IsSlice to be true for ONE_TO_MANY forward edge")
			}

			// Back-reference: Post -> User (MANY_TO_ONE)
			if len(postModel.Edges) != 1 {
				t.Fatalf("expected 1 edge in post model, got %d", len(postModel.Edges))
			}
			userEdge := postModel.Edges[0]
			// resTableNames["public.users"] = "Users"
			// Singularized = "User"
			got, want = userEdge.Name, "PostsUser"
			if got != want {
				t.Errorf(
					"got backref name %q, want %q",
					got, want,
				)
			}
			if userEdge.IsSlice {
				t.Error("expected IsSlice to be false for MANY_TO_ONE back-reference")
			}
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
						{Name: "status", Values: []string{"active"}}, // Enum 'status' -> Status
					},
				},
			},
		}

		sg, err := ast.NewSchemaGraph(pgDb)
		if err != nil {
			t.Fatalf(
				"expected no error on cross-type naming collision, got %v",
				err,
			)
		}

		got, want := sg.Models["public.status"].Name, "PublicStatus"
		if got != want {
			t.Errorf(
				"got %q, want %q",
				got, want,
			)
		}
		got, want = sg.Enums["auth.status"].Name, "AuthStatus"
		if got != want {
			t.Errorf(
				"got %q, want %q",
				got, want,
			)
		}
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
		if err == nil {
			t.Fatal("expected error on unresolved naming collision, got nil")
		}
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
			if err != nil {
				t.Fatalf("got unexpected error: %v", err)
			}

			got, want := sg.Models["public.users"].Name, "User"
			if got != want {
				t.Errorf(
					"got %q, want %q",
					got, want,
				)
			}
			got, want = sg.Models["auth.accounts"].Name, "Account"
			if got != want {
				t.Errorf(
					"got %q, want %q",
					got, want,
				)
			}
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
									TargetTable: "roles",
									Type:        shared.RelationType_RELATION_TYPE_MANY_TO_MANY,
									Columns: []*shared.RelationColumnMapping{
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
		if err != nil {
			t.Fatalf("NewSchemaGraph failed: %v", err)
		}

		userModel := sg.Models["public.users"]
		roleModel := sg.Models["public.roles"]

		// Forward: User -> Roles (Slice)
		if len(userModel.Edges) != 1 {
			t.Fatal("expected 1 edge in user model")
		}
		rolesEdge := userModel.Edges[0]
		if rolesEdge.Name != "Roles" || !rolesEdge.IsSlice {
			t.Errorf(
				"got name %q, isSlice %v; want Roles, true",
				rolesEdge.Name, rolesEdge.IsSlice,
			)
		}

		// Backward: Role -> RolesUsers (Slice)
		// name = "Roles" + "Users" = "RolesUsers"
		if len(roleModel.Edges) != 1 {
			t.Fatal("expected 1 edge in role model")
		}
		usersEdge := roleModel.Edges[0]
		if usersEdge.Name != "RolesUsers" || !usersEdge.IsSlice {
			t.Errorf(
				"got name %q, isSlice %v; want RolesUsers, true",
				usersEdge.Name, usersEdge.IsSlice,
			)
		}
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
										TargetTable: "users",
										Type:        shared.RelationType_RELATION_TYPE_MANY_TO_ONE,
										Columns: []*shared.RelationColumnMapping{{
											SourceColumn: "sender_id", TargetColumn: "id",
										}},
									},
									{
										Name:        "receiver",
										TargetTable: "users",
										Type:        shared.RelationType_RELATION_TYPE_MANY_TO_ONE,
										Columns: []*shared.RelationColumnMapping{{
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
			if err != nil {
				t.Fatalf("NewSchemaGraph failed: %v", err)
			}

			userModel := sg.Models["public.users"]
			if len(userModel.Edges) != 2 {
				t.Fatalf("expected 2 edges in user model, got %d", len(userModel.Edges))
			}

			// Names: "SenderMessages" and "ReceiverMessages"
			edgeNames := make(map[string]bool)
			for _, e := range userModel.Edges {
				edgeNames[e.Name] = true
			}

			hasSender := edgeNames["SenderMessages"]
			hasReceiver := edgeNames["ReceiverMessages"]
			if !hasSender || !hasReceiver {
				t.Errorf("missing expected back-references. Got: %v", edgeNames)
			}
		})
}
