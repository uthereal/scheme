package gen_test

import (
	"testing"
	"text/template"

	"github.com/uthereal/scheme/gen"
)

func TestRenderSource(t *testing.T) {
	tmplText := "package {{ . }}"
	validTmpl := template.Must(template.New("test").Parse(tmplText))
	badTmpl := template.Must(template.New("bad").Parse("{{ .Bad }}"))

	tests := []struct {
		name    string
		lang    gen.Language
		tmpl    *template.Template
		data    any
		want    string
		wantErr bool
	}{
		{
			name:    "returns error on nil template",
			lang:    gen.LangGo,
			tmpl:    nil,
			data:    nil,
			want:    "",
			wantErr: true,
		},
		{
			name:    "returns error on execution failure",
			lang:    gen.LangGo,
			tmpl:    badTmpl,
			data:    nil,
			want:    "",
			wantErr: true,
		},
		{
			name:    "successfully formats go source",
			lang:    gen.LangGo,
			tmpl:    validTmpl,
			data:    "main",
			want:    "package main\n",
			wantErr: false,
		},
		{
			name: "returns unformatted source for other languages",
			lang: gen.Language{
				Name:      "Other",
				Extension: "txt",
			},
			tmpl:    validTmpl,
			data:    "main",
			want:    "package main",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := gen.RenderSource(tt.lang, tt.tmpl, tt.data)

			gotErr, wantErr := err != nil, tt.wantErr
			if gotErr != wantErr {
				t.Fatalf("got error %v, wantErr %v", err, wantErr)
			}

			gotStr, wantStr := got, tt.want
			if gotStr != wantStr {
				t.Errorf("got %q, want %q", gotStr, wantStr)
			}
		})
	}
}
