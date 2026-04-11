package model

import (
	_ "embed"
	"errors"
	"fmt"
	"text/template"

	"github.com/uthereal/scheme/gen"
	"github.com/uthereal/scheme/gen/postgres/ast"
	"github.com/uthereal/scheme/genproto/core"
)

// modelTemplateData holds all required data to generate Go code from the
// `model.go.tmpl` template.
type modelTemplateData struct {
	GoPkgName string
	GoPkgPath string
	GoImports []string

	Models     []*ast.ModelGo
	Enums      []*ast.EnumGo
	Composites []*ast.CompositeGo
	Domains    []*ast.DomainGo
}

//go:embed model.go.tmpl
var modelGoTmpl string

// GenerateModels returns the generated code for the models defined in the
// provided schema, specifically targeting the requested language.
func GenerateModels(
	db *core.Database,
	lang gen.Language,
) (string, error) {
	if db == nil {
		return "", errors.New("db cannot be nil")
	}

	pgDatabase := db.GetPostgres()
	if pgDatabase == nil {
		return "", errors.New(
			"database does not contain a postgres database definition",
		)
	}

	g, err := ast.NewSchemaGraph(pgDatabase)
	if err != nil {
		return "", fmt.Errorf("failed to build schema graph -> %w", err)
	}

	switch lang.Name {
	case gen.LangGo.Name:
		return generateModelsGo(db, g, lang)
	default:
		return "", fmt.Errorf(
			"unsupported language for model generation -> %s", lang.Name,
		)
	}
}

// generateModelsGo handles Go-specific model generation.
func generateModelsGo(
	db *core.Database,
	g *ast.SchemaGraph,
	lang gen.Language,
) (string, error) {
	goGraph, err := ast.NewSchemaGraphGo(db, g, lang)
	if err != nil {
		return "", fmt.Errorf("failed to build go schema graph -> %w", err)
	}

	tmpl, err := template.New("modelGo").Parse(modelGoTmpl)
	if err != nil {
		return "", fmt.Errorf("failed to parse go model template -> %w", err)
	}

	tmplData := modelTemplateData{
		GoPkgName: goGraph.GoPkgName,
		GoPkgPath: goGraph.GoPkgPath,
		GoImports: goGraph.ImportList(),

		Models:     goGraph.Models,
		Enums:      goGraph.Enums,
		Composites: goGraph.Composites,
		Domains:    goGraph.Domains,
	}

	return gen.RenderSource(lang, tmpl, tmplData)
}
