package gen

import "errors"

// GenerateOptions contains language-specific configuration values required
// during code generation.
type GenerateOptions struct {
	// GoPackagePath defines the root Go module path (e.g., github.com/org/repo).
	GoPackagePath string
}

// Language represents the target programming language for the generator.
type Language struct {
	Name      string
	Extension string
	Options   GenerateOptions
	Validate  func(opts GenerateOptions) error
}

var (
	LangGo = Language{
		Name:      "Go",
		Extension: "go",
		Validate: func(opts GenerateOptions) error {
			if opts.GoPackagePath == "" {
				return errors.New("go package path cannot be empty for Go generation")
			}
			return nil
		},
	}
)
