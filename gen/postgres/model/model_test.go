package model_test

import (
	"testing"

	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/model"
	"github.com/uthereal/scheme/genproto/spec/core"
	"github.com/uthereal/scheme/genproto/spec/postgres"
)

func TestGenerateModels(t *testing.T) {
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
			name: "successfully generates models for valid schema",
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
			_, err := model.GenerateModels(tt.db, tt.lang)

			gotErr, wantErr := err != nil, tt.wantErr
			if gotErr != wantErr {
				t.Fatalf("got error %v, wantErr %v", err, wantErr)
			}
		})
	}
}
