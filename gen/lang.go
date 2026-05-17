package gen

// Language represents the target programming language for the generator.
type Language struct {
  Name      string
  Extension string
}

var (
  LangGo = Language{
    Name:      "Go",
    Extension: "go",
  }
)
