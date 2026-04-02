package query

import (
	"embed"
	"errors"
	"fmt"
	"text/template"

	"github.com/ettle/strcase"
	"github.com/scheme/gen"
	"github.com/scheme/gen/postgres/ast"
	"github.com/scheme/genproto/spec/core"
)

type queryTemplateData struct {
	PkgName    string
	GoPkgPath  string
	Imports    []string
	Models     []*ast.ModelData
	Composites []*ast.ModelData
}

//go:embed tmpl/go/*.go.tmpl
var goTmplFS embed.FS

var tmpls = map[string]*template.Template{
	gen.LangGo.Name: template.Must(
		template.New("go").ParseFS(goTmplFS, "tmpl/go/*.go.tmpl"),
	),
}

// GenerateQueryBuilders returns the generated code for the query builders
// defined in the provided schema, specifically targeting the requested
// language.
func GenerateQueryBuilders(
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

	tmplData := queryTemplateData{
		PkgName:    pkgName,
		GoPkgPath:  goPkgPath,
		Imports:    g.ImportList(),
		Models:     g.ModelList(),
		Composites: g.CompositeList(),
	}

	mainTmpl := tmpl.Lookup("main.go.tmpl")
	if mainTmpl == nil {
		panic("no template mapped for language -> main.go.tmpl")
	}

	return gen.RenderSource(lang, mainTmpl, tmplData)
}
