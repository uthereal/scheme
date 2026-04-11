package gen

import (
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"text/template"
)

// RenderSource executes a template, optionally formats it if the target
// language is Go, and returns it as a string.
func RenderSource(
	lang Language,
	tmpl *template.Template,
	data any,
) (string, error) {
	if tmpl == nil {
		return "", fmt.Errorf(
			"template is nil -> %w", errors.New("no template"),
		)
	}

	var out bytes.Buffer
	err := tmpl.Execute(&out, data)
	if err != nil {
		return "", fmt.Errorf("failed to execute template -> %w", err)
	}

	switch lang.Name {
	case LangGo.Name:
		formatted, err := format.Source(out.Bytes())
		if err != nil {
			return "", fmt.Errorf(
				"failed to format generated Go source -> %w", err,
			)
		}
		return string(formatted), nil
	default:
		return out.String(), nil
	}
}
