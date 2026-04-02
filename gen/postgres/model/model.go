package model

import (
	_ "embed"
	"errors"
	"fmt"
	"text/template"

	"github.com/ettle/strcase"
	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/ast"
	"github.com/uthereal/scheme/genproto/core"
)

// modelTemplateData holds all required data to generate go code from the
// `model.go.tmpl` template.
type modelTemplateData struct {
	PkgName    string
	GoPkgPath  string
	Imports    []string
	Models     []*ast.ModelData
	Enums      []*ast.EnumData
	Composites []*ast.ModelData
	Domains    []*ast.DomainData
}

//go:embed model.go.tmpl
var modelGoTmpl string

var tmpls = map[string]*template.Template{
	gen.LangGo.Name: template.Must(
		template.New("modelGoData").Parse(modelGoTmpl),
	),
}

// GenerateModels returns the generated code for the models defined in the
// provided schema, specifically targeting the requested language.
func GenerateModels(
	db *core.Database,
	lang gen.Language,
) (string, error) {
	pgDatabase := db.GetPostgres()
	if pgDatabase == nil {
		return "", errors.New(
			"database does not contain a postgres database definition",
		)
	}

	if lang.Validate == nil {
		panic("language validation function cannot be nil")
	}
	err := lang.Validate(lang.Options)
	if err != nil {
		return "", err
	}

	pkgName := db.GetName()
	if pkgName == "" {
		return "", errors.New("database name cannot be empty")
	}
	pkgName = strcase.ToSnake(pkgName)

	tmpl, ok := tmpls[lang.Name]
	if !ok {
		return "", fmt.Errorf("no template mapped for language -> %s", lang.Name)
	}

	g, err := ast.NewSchemaGraph(pgDatabase)
	if err != nil {
		return "", err
	}

	goPkgPath := ""
	if lang.Options.GoPackagePath != "" {
		goPkgPath = lang.Options.GoPackagePath + "/postgres/" + pkgName
	}

	tmplData := modelTemplateData{
		PkgName:    pkgName,
		GoPkgPath:  goPkgPath,
		Imports:    g.ImportList(),
		Models:     g.ModelList(),
		Enums:      g.EnumList(),
		Composites: g.CompositeList(),
		Domains:    g.DomainList(),
	}

	return gen.RenderSource(lang, tmpl, tmplData)
}
