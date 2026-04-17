package postgres_test

import (
  "testing"

  "context"

  "github.com/google/go-cmp/cmp"
  "github.com/gotidy/ptr"
  "github.com/uthereal/scheme/genproto/core/shared"
  genpg "github.com/uthereal/scheme/genproto/postgres"
  "github.com/uthereal/scheme/migrate/postgres"
)

func TestNewDatabaseStateFromProto(t *testing.T) {
	t.Run("nil proto", func(t *testing.T) {
		_, err := postgres.NewDatabaseStateFromProto(nil)
		if err == nil {
			t.Fatal("expected error on nil proto")
		}
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
									Name:        "fk_author",
									TargetTable: "auth.users",
									Columns: []*shared.ForeignKeyColumnMapping{
										{SourceColumn: "author_id", TargetColumn: "id"},
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

		gotErr, wantErr := err != nil, false
		if gotErr != wantErr {
			t.Fatalf("NewDatabaseStateFromProto() error = %v, wantErr %v", err, wantErr)
		}

		fk := state.Schemas["public"].Tables["posts"].ForeignKeys["fk_author"]
		gotUp, wantUp := string(fk.OnUpdate), "NO ACTION"
		if gotUp != wantUp {
			t.Errorf("expected NO ACTION for unspecified fks, got %q", gotUp)
		}

		gotDel, wantDel := string(fk.OnDelete), "NO ACTION"
		if gotDel != wantDel {
			t.Errorf("expected NO ACTION for unspecified fks, got %q", gotDel)
		}

		gotSchema, wantSchema := fk.TargetSchema, "auth"
		if gotSchema != wantSchema {
			t.Errorf("expected target auth, got %q", gotSchema)
		}

		gotTable, wantTable := fk.TargetTable, "users"
		if gotTable != wantTable {
			t.Errorf("expected target users, got %q", gotTable)
		}
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
									VarcharType: &genpg.VarcharType{Length: ptr.Int32(255)},
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
									Type: &genpg.DataType{Type: &genpg.DataType_UuidType{}},
								},
								{
									Name: "seq",
									Type: &genpg.DataType{Type: &genpg.DataType_BigserialType{}},
								},
								{
									Name:         "role",
									Type:         &genpg.DataType{Type: &genpg.DataType_TextType{}},
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
									Name:        "fk_profile",
									TargetTable: "profiles",
									Columns: []*shared.ForeignKeyColumnMapping{
										{SourceColumn: "id", TargetColumn: "user_id"},
									},
								},
							},
						},
					},
				},
			},
		}

		state, err := postgres.NewDatabaseStateFromProto(proto)
		gotErr, wantErr := err != nil, false
		if gotErr != wantErr {
			t.Fatalf("failed to convert proto to state: %v", err)
		}

		gotState, wantState := state != nil, true
		if gotState != wantState {
			t.Fatal("expected state, got nil")
		}

		s, ok := state.Schemas["public"]
		if !ok {
			t.Fatal("expected public schema")
		}

		gotPrev, wantPrev := s.NamePrevious, "old_public"
		if gotPrev != wantPrev {
			t.Errorf("expected previous name %q, got %q", wantPrev, gotPrev)
		}

		_, okEnums := s.Enums["status"]
		if !okEnums {
			t.Error("expected enum status")
		}

		_, okDomains := s.Domains["email"]
		if !okDomains {
			t.Error("expected domain email")
		}

		_, okComposites := s.Composites["address"]
		if !okComposites {
			t.Error("expected composite address")
		}

		u, ok := s.Tables["users"]
		if !ok {
			t.Fatal("expected table users")
		}

		gotPK, wantPK := u.PrimaryKey != nil &&
			len(u.PrimaryKey.Columns) == 1 &&
			u.PrimaryKey.Columns[0] == "id", true
		if gotPK != wantPK {
			t.Error("expected primary key on id")
		}

		_, okCol := u.Columns["id"]
		if !okCol {
			t.Error("expected column id")
		}

		seqCol, okSeq := u.Columns["seq"]
		if !okSeq || !seqCol.IsAutoIncrement {
			t.Error("expected auto increment column seq")
		}

		roleCol, okRole := u.Columns["role"]
		if !okRole || roleCol.ColumnDefault == nil || *roleCol.ColumnDefault != "'user'" {
			t.Error("expected column role with default 'user'")
		}

		_, okIdx := u.Indexes["idx_users_id"]
		if !okIdx {
			t.Error("expected index idx_users_id")
		}

		_, okFk := u.ForeignKeys["fk_profile"]
		if !okFk {
			t.Error("expected foreign key fk_profile")
		}
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
										Name:       "id",
										Type:       &genpg.DataType{Type: &genpg.DataType_UuidType{}},
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

			gotErr, wantErr := err != nil, true
			if gotErr != wantErr {
				t.Fatalf("expected error for %q", tt.name)
			}
		})
	}
}

func TestNewDatabaseStateFromDb(t *testing.T) {
	t.Run("nil db", func(t *testing.T) {
		ctx := context.Background()
		_, err := postgres.NewDatabaseStateFromDb(ctx, nil)

		gotErr, wantErr := err != nil, true
		if gotErr != wantErr {
			t.Errorf(
				"NewDatabaseStateFromDb(nil) error = %v, want %v",
				gotErr, wantErr,
			)
		}
	})

	t.Run("exhaustive combinations", func(t *testing.T) {
		ctx := context.Background()

		db, cleanup, err := testContainer.CreateIsolatedDB(ctx, t)
		if err != nil {
			t.Fatalf("failed to create isolated db: %v", err)
		}
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
			if err != nil {
				t.Fatalf("failed to execute setup sql %q: %v", stmt, err)
			}
		}

		state, err := postgres.NewDatabaseStateFromDb(ctx, db)

		gotErr, wantErr := err != nil, false
		if gotErr != wantErr {
			t.Fatalf("NewDatabaseStateFromDb() error = %v, wantErr %v", err, wantErr)
		}

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
									ColumnDefault:   ptr.String("nextval('custom_schema.posts_id_seq'"),
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
				},
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
							Name: "email_address", DataType: "character varying(255)",
						},
					},
				},
			},
		}

		diff := cmp.Diff(wantState, state)
		gotDiff, wantDiff := diff != "", false
		if gotDiff != wantDiff {
			t.Errorf("NewDatabaseStateFromDb() mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestDatabaseState_Clone(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var l *postgres.DatabaseState
		clone := l.Clone()

		gotClone, wantClone := clone != nil, false
		if gotClone != wantClone {
			t.Errorf(
				"Clone() on nil receiver = %v, want %v",
				gotClone, wantClone,
			)
		}
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
									TargetTable: "other",
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
		gotDT, wantDT := string(dt), "uuid"
		if gotDT != wantDT {
			t.Errorf("Column DataType mutated = %q, want %q", gotDT, wantDT)
		}

		def := clone.Schemas["public"].Tables["users"].Columns["id"].
			ColumnDefault
		gotDef, wantDef := *def, "gen_random_uuid()"
		if gotDef != wantDef {
			t.Errorf("Column Default mutated = %q, want %q", gotDef, wantDef)
		}

		pk := clone.Schemas["public"].Tables["users"].PrimaryKey
		gotPK, wantPK := pk.Columns[0], "id"
		if gotPK != wantPK {
			t.Errorf("PrimaryKey mutated = %q, want %q", gotPK, wantPK)
		}

		idx := clone.Schemas["public"].Tables["users"].Indexes["users_idx"]
		gotIdx, wantIdx := idx.Columns[0], "id"
		if gotIdx != wantIdx {
			t.Errorf("Index mutated = %q, want %q", gotIdx, wantIdx)
		}

		fk := clone.Schemas["public"].Tables["users"].ForeignKeys["fk_users"]
		gotFK, wantFK := fk.ColsLocal[0], "id"
		if gotFK != wantFK {
			t.Errorf("ForeignKey mutated = %q, want %q", gotFK, wantFK)
		}

		en := clone.Schemas["public"].Enums["status"]
		gotEn, wantEn := en.Values[0], "active"
		if gotEn != wantEn {
			t.Errorf("Enum mutated = %q, want %q", gotEn, wantEn)
		}

		city := clone.Schemas["public"].Composites["address"].Fields["city"]
		gotCity, wantCity := string(city.DataType), "text"
		if gotCity != wantCity {
			t.Errorf("Composite mutated = %q, want %q", gotCity, wantCity)
		}

		dom := clone.Schemas["public"].Domains["email"]
		gotDom, wantDom := string(dom.DataType), "text"
		if gotDom != wantDom {
			t.Errorf("Domain mutated = %q, want %q", gotDom, wantDom)
		}
	})
}
