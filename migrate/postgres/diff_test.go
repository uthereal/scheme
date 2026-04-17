package postgres

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/gotidy/ptr"
	"github.com/uthereal/scheme/genproto/core/shared"
	"github.com/uthereal/scheme/genproto/postgres"
)

func TestDiffer_DeterministicPlan(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "determinate drop tables"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{Name: "public"},
				},
			}

			var firstActions []MigrationAction

			for i := 0; i < 10; i++ {
				liveClone := &DatabaseState{
					Schemas: map[string]*SchemaState{
						"public": {
							Name: "public",
							Tables: map[string]*TableState{
								"table_a": {Name: "table_a"},
								"table_b": {Name: "table_b"},
								"table_c": {Name: "table_c"},
								"table_d": {Name: "table_d"},
								"table_e": {Name: "table_e"},
							},
							Enums:      make(map[string]*EnumState),
							Composites: make(map[string]*CompositeState),
							Domains:    make(map[string]*DomainState),
						},
					},
				}

				targetState, err := NewDatabaseStateFromProto(target)
				if err != nil {
					t.Fatalf("NewDatabaseStateFromProto failed -> %v", err)
				}
				actions, err := ComputeDiff(liveClone, targetState)
				if err != nil {
					t.Fatalf("ComputeDiff failed -> %v", err)
				}

				if i == 0 {
					firstActions = actions
				} else {
					if !reflect.DeepEqual(firstActions, actions) {
						t.Fatalf("run %d produced non-deterministic plan", i)
					}
				}
			}
		})
	}
}

func TestDiffer_Plan(t *testing.T) {
	tests := []struct {
		name        string
		live        *DatabaseState
		target      *postgres.PostgresDatabase
		wantActions []MigrationAction
		wantErr     bool
	}{
		{
			name: "create schemas",
			live: &DatabaseState{Schemas: make(map[string]*SchemaState)},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{Name: "public"},
					{Name: "auth"},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectSchema,
					Name:       "auth",
					SQL:        `CREATE SCHEMA IF NOT EXISTS "auth";`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectSchema,
					Name:       "public",
					SQL:        `CREATE SCHEMA IF NOT EXISTS "public";`,
				},
			},
		},
		{
			name: "rename schema",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"old_auth": {Name: "old_auth"},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{Name: "new_auth", NamePrevious: "old_auth"},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectSchema,
					Name:       "scheme_tmp_schema_old_auth",
					SQL: `ALTER SCHEMA "old_auth" RENAME TO ` +
						`"scheme_tmp_schema_old_auth";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectSchema,
					Name:       "new_auth",
					SQL: `ALTER SCHEMA "scheme_tmp_schema_old_auth" RENAME TO ` +
						`"new_auth";`,
				},
			},
		},
		{
			name: "create enum",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {Name: "public", Enums: make(map[string]*EnumState)},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Enums: []*postgres.EnumDefinition{
							{
								Name:   "status",
								Values: []string{"active", "inactive"},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectEnum,
					Schema:     "public",
					Name:       "status",
					SQL: "CREATE TYPE \"public\".\"status\" " +
						"AS ENUM ('active', 'inactive');",
				},
			},
		},
		{
			name: "rename enum and add value",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Enums: map[string]*EnumState{
							"old_status": {
								Name:   "old_status",
								Values: []string{"active"},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Enums: []*postgres.EnumDefinition{
							{
								Name:         "new_status",
								NamePrevious: "old_status",
								Values:       []string{"active", "banned"},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectEnum,
					Schema:     "public",
					Name:       "scheme_tmp_enum_old_status",
					SQL: `ALTER TYPE "public"."old_status" RENAME TO ` +
						`"scheme_tmp_enum_old_status";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectEnum,
					Schema:     "public",
					Name:       "new_status",
					SQL: `ALTER TYPE "public"."scheme_tmp_enum_old_status" RENAME TO ` +
						`"new_status";`,
				},
				{
					Type:       ActionTypeAlter,
					ObjectType: ObjectEnum,
					Schema:     "public",
					Name:       "new_status",
					SQL:        `ALTER TYPE "public"."new_status" ADD VALUE 'banned';`,
				},
			},
		},
		{
			name: "drop enum",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Enums: map[string]*EnumState{
							"to_drop": {Name: "to_drop", Values: []string{"v"}},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{Name: "public"},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectEnum,
					Schema:        "public",
					Name:          "to_drop",
					IsDestructive: true,
					SQL:           `DROP TYPE "public"."to_drop" CASCADE;`,
				},
			},
		},
		{
			name: "create composite",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name:       "public",
						Composites: make(map[string]*CompositeState),
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Composites: []*postgres.CompositeDefinition{
							{
								Name: "address",
								Fields: []*postgres.CompositeField{
									{
										Name: "street",
										Type: &postgres.DataType{
											Type: &postgres.DataType_VarcharType{
												VarcharType: &postgres.VarcharType{Length: ptr.Int32(255)},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectComposite,
					Schema:     "public",
					Name:       "address",
					SQL: "CREATE TYPE \"public\".\"address\" " +
						"AS (\"street\" character varying(255));",
				},
			},
		},
		{
			name: "create domain",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {Name: "public", Domains: make(map[string]*DomainState)},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Domains: []*postgres.DomainDefinition{
							{
								Name: "email",
								BaseType: &postgres.DataType{
									Type: &postgres.DataType_VarcharType{
										VarcharType: &postgres.VarcharType{Length: ptr.Int32(255)},
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectDomain,
					Schema:     "public",
					Name:       "email",
					SQL:        `CREATE DOMAIN "public"."email" AS character varying(255);`,
				},
			},
		},
		{
			name: "create table",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {Name: "public", Tables: make(map[string]*TableState)},
				},
			},
			target: &postgres.PostgresDatabase{
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
										IsNullable:   false,
										DefaultValue: "gen_random_uuid()",
									},
								},
								PrimaryKeys: []string{"id"},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectTable,
					Schema:     "public",
					Name:       "users",
					SQL:        `CREATE TABLE "public"."users" ();`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectColumn,
					Schema:     "public",
					Name:       "users.id",
					SQL: "ALTER TABLE \"public\".\"users\" " +
						"ADD COLUMN \"id\" uuid NOT NULL DEFAULT " +
						"gen_random_uuid();",
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectPrimaryKey,
					Schema:     "public",
					Name:       "users_pkey",
					SQL: "ALTER TABLE \"public\".\"users\" " +
						"ADD CONSTRAINT \"users_pkey\" " +
						"PRIMARY KEY (\"id\");",
				},
			},
		},
		{
			name: "alter column type and nullability",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"users": {
								Name: "users",
								Columns: map[string]*ColumnState{
									"age": {
										Name:       "age",
										DataType:   "integer",
										IsNullable: true,
									},
								},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "users",
								Columns: []*postgres.Column{
									{
										Name: "age",
										Type: &postgres.DataType{
											Type: &postgres.DataType_BigintType{
												BigintType: &postgres.BigIntType{},
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
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeAlter,
					ObjectType: ObjectColumn,
					Schema:     "public",
					Name:       "users.age",
					SQL:        `ALTER TABLE "public"."users" ALTER COLUMN "age" TYPE bigint;`,
				},
				{
					Type:       ActionTypeAlter,
					ObjectType: ObjectColumn,
					Schema:     "public",
					Name:       "users.age",
					SQL: "ALTER TABLE \"public\".\"users\" " +
						"ALTER COLUMN \"age\" SET NOT NULL;",
				},
			},
		},
		{
			name: "create foreign key",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"posts": {Name: "posts", ForeignKeys: make(map[string]*ForeignKeyState)},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "posts",
								ForeignKeys: []*shared.ForeignKey{
									{
										Name:        "fk_author",
										TargetTable: "users",
										Columns: []*shared.ForeignKeyColumnMapping{
											{SourceColumn: "author_id", TargetColumn: "id"},
										},
										OnDelete: shared.ForeignKeyAction_FOREIGN_KEY_ACTION_CASCADE,
										OnUpdate: shared.ForeignKeyAction_FOREIGN_KEY_ACTION_NO_ACTION,
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectForeignKey,
					Schema:     "public",
					Name:       "fk_author",
					SQL: "ALTER TABLE \"public\".\"posts\" " +
						"ADD CONSTRAINT \"fk_author\" " +
						"FOREIGN KEY (\"author_id\") " +
						"REFERENCES \"public\".\"users\" (\"id\")" +
						" ON DELETE CASCADE;",
				},
			},
		},
		{
			name: "create index",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"users": {Name: "users", Indexes: make(map[string]*IndexState)},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "users",
								Indexes: []*postgres.Index{
									{
										Name:     "idx_users_email",
										IsUnique: true,
										Columns:  []*postgres.IndexColumn{{Name: "email"}},
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectIndex,
					Schema:     "public",
					Name:       "idx_users_email",
					SQL: "CREATE UNIQUE INDEX \"idx_users_email\" " +
						"ON \"public\".\"users\" (\"email\");",
				},
			},
		},
		{
			name: "column default modifications and identity creation",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"users": {
								Name: "users",
								Columns: map[string]*ColumnState{
									"status": {
										Name:          "status",
										DataType:      "character varying(255)",
										IsNullable:    true,
										ColumnDefault: ptr.String("'active'"),
									},
								},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "users",
								Columns: []*postgres.Column{
									{
										Name: "status",
										Type: &postgres.DataType{
											Type: &postgres.DataType_VarcharType{
												VarcharType: &postgres.VarcharType{Length: ptr.Int32(255)},
											},
										},
										IsNullable: true,
									},
									{
										Name: "id",
										Type: &postgres.DataType{
											Type: &postgres.DataType_BigserialType{
												BigserialType: &postgres.BigSerialType{},
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
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeAlter,
					ObjectType: ObjectColumn,
					Schema:     "public",
					Name:       "users.status",
					SQL:        `ALTER TABLE "public"."users" ALTER COLUMN "status" DROP DEFAULT;`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectColumn,
					Schema:     "public",
					Name:       "users.id",
					SQL:        `ALTER TABLE "public"."users" ADD COLUMN "id" bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY;`,
				},
			},
		},
		{
			name: "modify index structures",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"users": {
								Name: "users",
								Indexes: map[string]*IndexState{
									"idx_email": {
										Name:     "idx_email",
										IsUnique: false,
										Columns:  []string{"email"},
									},
									"idx_name": {
										Name:     "idx_name",
										IsUnique: true,
										Columns:  []string{"first_name", "last_name"},
									},
								},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "users",
								Indexes: []*postgres.Index{
									{
										Name:     "idx_email",
										IsUnique: true,
										Columns:  []*postgres.IndexColumn{{Name: "email"}},
									},
									{
										Name:     "idx_name",
										IsUnique: true,
										Columns:  []*postgres.IndexColumn{{Name: "last_name"}, {Name: "first_name"}},
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeDrop,
					ObjectType: ObjectIndex,
					Schema:     "public",
					Name:       "idx_email",
					SQL:        `DROP INDEX "public"."idx_email";`,
				},
				{
					Type:       ActionTypeDrop,
					ObjectType: ObjectIndex,
					Schema:     "public",
					Name:       "idx_name",
					SQL:        `DROP INDEX "public"."idx_name";`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectIndex,
					Schema:     "public",
					Name:       "idx_email",
					SQL:        `CREATE UNIQUE INDEX "idx_email" ON "public"."users" ("email");`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectIndex,
					Schema:     "public",
					Name:       "idx_name",
					SQL:        `CREATE UNIQUE INDEX "idx_name" ON "public"."users" ("last_name", "first_name");`,
				},
			},
		},
		{
			name: "primary key modifications",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"users": {
								Name: "users",
								PrimaryKey: &PrimaryKeyState{
									Name:    "users_pkey",
									Columns: []string{"id", "tenant_id"},
								},
							},
							"posts": {
								Name: "posts",
								PrimaryKey: &PrimaryKeyState{
									Name:    "posts_pkey",
									Columns: []string{"id"},
								},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "posts",
							},
							{
								Name:        "users",
								PrimaryKeys: []string{"id"},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeDrop,
					ObjectType: ObjectPrimaryKey,
					Schema:     "public",
					Name:       "posts_pkey",
					SQL:        `ALTER TABLE "public"."posts" DROP CONSTRAINT "posts_pkey";`,
				},
				{
					Type:       ActionTypeDrop,
					ObjectType: ObjectPrimaryKey,
					Schema:     "public",
					Name:       "users_pkey",
					SQL:        `ALTER TABLE "public"."users" DROP CONSTRAINT "users_pkey";`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectPrimaryKey,
					Schema:     "public",
					Name:       "users_pkey",
					SQL:        `ALTER TABLE "public"."users" ADD CONSTRAINT "users_pkey" PRIMARY KEY ("id");`,
				},
			},
		},
		{
			name: "alter foreign key rules",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Tables: map[string]*TableState{
							"posts": {
								Name: "posts",
								ForeignKeys: map[string]*ForeignKeyState{
									"fk_author": {
										Name:     "fk_author",
										OnDelete: "CASCADE",
										OnUpdate: "NO ACTION",
									},
								},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Tables: []*postgres.Table{
							{
								Name: "posts",
								ForeignKeys: []*shared.ForeignKey{
									{
										Name:        "fk_author",
										TargetTable: "users",
										Columns: []*shared.ForeignKeyColumnMapping{
											{SourceColumn: "author_id", TargetColumn: "id"},
										},
										OnDelete: shared.ForeignKeyAction_FOREIGN_KEY_ACTION_SET_NULL,
										OnUpdate: shared.ForeignKeyAction_FOREIGN_KEY_ACTION_CASCADE,
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeDrop,
					ObjectType: ObjectForeignKey,
					Schema:     "public",
					Name:       "fk_author",
					SQL:        `ALTER TABLE "public"."posts" DROP CONSTRAINT "fk_author";`,
				},
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectForeignKey,
					Schema:     "public",
					Name:       "fk_author",
					SQL:        `ALTER TABLE "public"."posts" ADD CONSTRAINT "fk_author" FOREIGN KEY ("author_id") REFERENCES "public"."users" ("id") ON UPDATE CASCADE ON DELETE SET NULL;`,
				},
			},
		},
		{
			name: "cross schema migration and composite attribute changes",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Enums: map[string]*EnumState{
							"role": {Name: "role", Values: []string{"admin", "user"}},
						},
						Composites: map[string]*CompositeState{
							"address": {
								Name: "address",
								Fields: map[string]*CompositeFieldState{
									"street": {Name: "street", DataType: "character varying(255)"},
									"city":   {Name: "city", DataType: "character varying(255)"},
								},
							},
						},
					},
					"auth": {
						Name:       "auth",
						Enums:      make(map[string]*EnumState),
						Composites: make(map[string]*CompositeState),
						Domains:    make(map[string]*DomainState),
						Tables:     make(map[string]*TableState),
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "auth",
						Enums: []*postgres.EnumDefinition{
							{
								Name:   "role",
								Values: []string{"admin", "user"},
							},
						},
					},
					{
						Name: "public",
						Composites: []*postgres.CompositeDefinition{
							{
								Name: "address",
								Fields: []*postgres.CompositeField{
									{
										Name: "street",
										Type: &postgres.DataType{
											Type: &postgres.DataType_VarcharType{
												VarcharType: &postgres.VarcharType{Length: ptr.Int32(255)},
											},
										},
									},
									{
										Name: "zipcode",
										Type: &postgres.DataType{
											Type: &postgres.DataType_VarcharType{
												VarcharType: &postgres.VarcharType{Length: ptr.Int32(20)},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeCreate,
					ObjectType: ObjectEnum,
					Schema:     "auth",
					Name:       "role",
					SQL:        `CREATE TYPE "auth"."role" AS ENUM ('admin', 'user');`,
				},
				{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectEnum,
					Schema:        "public",
					Name:          "role",
					IsDestructive: true,
					SQL:           `DROP TYPE "public"."role" CASCADE;`,
				},
				{
					Type:       ActionTypeAlter,
					ObjectType: ObjectComposite,
					Schema:     "public",
					Name:       "address",
					SQL:        `ALTER TYPE "public"."address" ADD ATTRIBUTE "zipcode" character varying(20);`,
				},
				{
					Type:          ActionTypeDrop,
					ObjectType:    ObjectComposite,
					Schema:        "public",
					Name:          "address.city",
					IsDestructive: true,
					SQL:           `ALTER TYPE "public"."address" DROP ATTRIBUTE "city" CASCADE;`,
				},
			},
		},
		{
			name: "explicit object renames",
			live: &DatabaseState{
				Schemas: map[string]*SchemaState{
					"public": {
						Name: "public",
						Enums: map[string]*EnumState{
							"old_enum": {Name: "old_enum", Values: []string{"v"}},
						},
						Domains: map[string]*DomainState{
							"old_domain": {Name: "old_domain", DataType: "integer"},
						},
						Composites: map[string]*CompositeState{
							"old_comp": {
								Name: "old_comp",
								Fields: map[string]*CompositeFieldState{
									"f": {Name: "f", DataType: "integer"},
								},
							},
						},
						Tables: map[string]*TableState{
							"posts": {
								Name: "posts",
								ForeignKeys: map[string]*ForeignKeyState{
									"fk_old": {
										Name:         "fk_old",
										ColsLocal:    []string{"id"},
										ColsTarget:   []string{"id"},
										TargetSchema: "public",
										TargetTable:  "users",
										OnDelete:     "NO ACTION",
										OnUpdate:     "NO ACTION",
									},
								},
							},
						},
					},
				},
			},
			target: &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{
						Name: "public",
						Enums: []*postgres.EnumDefinition{
							{Name: "new_enum", NamePrevious: "old_enum", Values: []string{"v"}},
						},
						Domains: []*postgres.DomainDefinition{
							{
								Name: "new_domain", NamePrevious: "old_domain",
								BaseType: &postgres.DataType{Type: &postgres.DataType_IntegerType{IntegerType: &postgres.IntegerType{}}},
							},
						},
						Composites: []*postgres.CompositeDefinition{
							{
								Name: "new_comp", NamePrevious: "old_comp",
								Fields: []*postgres.CompositeField{
									{
										Name: "f",
										Type: &postgres.DataType{Type: &postgres.DataType_IntegerType{IntegerType: &postgres.IntegerType{}}},
									},
								},
							},
						},
						Tables: []*postgres.Table{
							{
								Name: "posts",
								ForeignKeys: []*shared.ForeignKey{
									{
										Name:         "new_fk",
										NamePrevious: "fk_old",
										TargetTable:  "users",
										Columns: []*shared.ForeignKeyColumnMapping{
											{SourceColumn: "id", TargetColumn: "id"},
										},
									},
								},
							},
						},
					},
				},
			},
			wantActions: []MigrationAction{
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectEnum,
					Schema:     "public",
					Name:       "scheme_tmp_enum_old_enum",
					SQL:        `ALTER TYPE "public"."old_enum" RENAME TO "scheme_tmp_enum_old_enum";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectEnum,
					Schema:     "public",
					Name:       "new_enum",
					SQL:        `ALTER TYPE "public"."scheme_tmp_enum_old_enum" RENAME TO "new_enum";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectComposite,
					Schema:     "public",
					Name:       "scheme_tmp_comp_old_comp",
					SQL:        `ALTER TYPE "public"."old_comp" RENAME TO "scheme_tmp_comp_old_comp";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectComposite,
					Schema:     "public",
					Name:       "new_comp",
					SQL:        `ALTER TYPE "public"."scheme_tmp_comp_old_comp" RENAME TO "new_comp";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectDomain,
					Schema:     "public",
					Name:       "scheme_tmp_dom_old_domain",
					SQL:        `ALTER DOMAIN "public"."old_domain" RENAME TO "scheme_tmp_dom_old_domain";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectDomain,
					Schema:     "public",
					Name:       "new_domain",
					SQL:        `ALTER DOMAIN "public"."scheme_tmp_dom_old_domain" RENAME TO "new_domain";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectForeignKey,
					Schema:     "public",
					Name:       "scheme_tmp_fk_fk_old",
					SQL:        `ALTER TABLE "public"."posts" RENAME CONSTRAINT "fk_old" TO "scheme_tmp_fk_fk_old";`,
				},
				{
					Type:       ActionTypeRename,
					ObjectType: ObjectForeignKey,
					Schema:     "public",
					Name:       "new_fk",
					SQL:        `ALTER TABLE "public"."posts" RENAME CONSTRAINT "scheme_tmp_fk_fk_old" TO "new_fk";`,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targetState, err := NewDatabaseStateFromProto(tt.target)
			if err != nil {
				t.Fatalf("NewDatabaseStateFromProto failed -> %v", err)
			}
			actions, err := ComputeDiff(tt.live, targetState)

			gotErr, wantErr := err != nil, tt.wantErr
			if gotErr != wantErr {
				t.Fatalf("ComputeDiff() error exists = %v, want %v", gotErr, wantErr)
			}

			if !tt.wantErr {
				diff := cmp.Diff(tt.wantActions, actions)
				if diff != "" {
					t.Errorf("ComputeDiff() actions mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
