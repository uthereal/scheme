package gen_test

import (
	"testing"

	"github.com/scheme/gen"
)

func TestLanguage_Validate(t *testing.T) {
	tests := []struct {
		name    string
		lang    gen.Language
		wantErr bool
	}{
		{
			name: "successfully validates correctly configured go language target",
			lang: gen.Language{
				Name:      "Go",
				Extension: "go",
				Options: gen.GenerateOptions{
					GoPackagePath: "github.com/foo/bar",
				},
				Validate: gen.LangGo.Validate,
			},
			wantErr: false,
		},
		{
			name: "returns error when go package path is empty",
			lang: gen.Language{
				Name:      "Go",
				Extension: "go",
				Options: gen.GenerateOptions{
					GoPackagePath: "",
				},
				Validate: gen.LangGo.Validate,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.lang.Validate(tt.lang.Options)

			gotErr, wantErr := err != nil, tt.wantErr
			if gotErr != wantErr {
				t.Fatalf("got error %v, wantErr %v", err, wantErr)
			}
		})
	}
}
