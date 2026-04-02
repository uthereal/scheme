package query_test

import (
	"testing"

	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/query"
	"github.com/uthereal/scheme/genproto/core"
	"github.com/uthereal/scheme/genproto/postgres"
)

func TestGenerateQueryBuilders(t *testing.T) {
	validLang := gen.LangGo
	validLang.Options.GoPackagePath = "github.com/foo/bar"

	tests := []struct {
		name    string
		db      *core.Database
		lang    gen.Language
		wantErr bool
	}{
		{
			name: "returns error on nil postgres definition",
			db: &core.Database{
				Name: "mydb",
			},
			lang:    validLang,
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
			lang:    gen.LangGo, // missing options.GoPackagePath
			wantErr: true,
		},
		{
			name: "returns error on empty database name",
			db: &core.Database{
				Name: "",
				Engine: &core.Database_Postgres{
					Postgres: &postgres.PostgresDatabase{},
				},
			},
			lang:    validLang,
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
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := query.GenerateQueryBuilders(tt.db, tt.lang)

			gotErr, wantErr := err != nil, tt.wantErr
			if gotErr != wantErr {
				t.Fatalf("got error %v, wantErr %v", err, wantErr)
			}
		})
	}
}
