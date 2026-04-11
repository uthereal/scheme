package query

import (
	"embed"
	"errors"
	"fmt"
	"text/template"

	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/ast"
	"github.com/uthereal/scheme/genproto/core"
)

type queryTemplateData struct {
	PkgName    string
	GoPkgPath  string
	Imports    []string
	Models     []*ast.ModelGo
	Composites []*ast.CompositeGo
	Active     map[string]bool
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
	switch lang.Name {
	case gen.LangGo.Name:
		return generateQueryBuildersGo(db, lang)
	default:
		return "", fmt.Errorf("unsupported language -> %s", lang.Name)
	}
}

func generateQueryBuildersGo(
	db *core.Database,
	lang gen.Language,
) (string, error) {
	if db == nil {
		return "", errors.New("db cannot be nil")
	}

	pgSchema := db.GetPostgres()
	g, err := ast.NewSchemaGraph(pgSchema)
	if err != nil {
		return "", err
	}

	goGraph, err := ast.NewSchemaGraphGo(db, g, lang)
	if err != nil {
		return "", err
	}

	tmpl, ok := tmpls[lang.Name]
	if !ok {
		return "", fmt.Errorf(
			"no template mapped for language -> %s", lang.Name,
		)
	}

	tmplData := queryTemplateData{
		PkgName:    goGraph.GoPkgName,
		GoPkgPath:  goGraph.GoPkgPath,
		Imports:    goGraph.ImportList(),
		Models:     goGraph.Models,
		Composites: goGraph.Composites,
		Active:     goGraph.ActiveCategories,
	}

	mainTmpl := tmpl.Lookup("main.go.tmpl")
	if mainTmpl == nil {
		panic("no template mapped for language -> main.go.tmpl")
	}

	return gen.RenderSource(lang, mainTmpl, tmplData)
}
