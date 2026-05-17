package gen_test

import (
  "bytes"
  "context"
  _ "embed"
  "os"
  "os/exec"
  "path/filepath"
  "testing"
  "text/template"

  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/require"
  "github.com/uthereal/scheme/test/testutil"
)

//go:embed test_app_main.go.tmpl
var testAppMain []byte

//go:embed test_app_mod.go.tmpl
var testAppModTmpl []byte

var (
  cliPathParts    = []string{"cmd", "scheme", "main.go"}
  schemaPathParts = []string{"test", "testdata", "kitchen_sink.textproto"}
  outDirParts     = []string{"gen", "kitchensink"}
)

func TestPostgresGeneratedCode(t *testing.T) {
  ctx := context.Background()

  // Resolving the root directory anchors the subprocess command execution,
  // allowing the generator CLI to be invoked exactly as if the user ran it
  // manually from the base of the repository.
  rootDir, err := filepath.Abs("../..")
  require.NoError(t, err, "failed to get root dir")

  // isolated clone filesystem for the compiled code output, ensuring
  // automatic cleanup upon test completion and preventing artifacts
  // from mutating the host VCS tree.
  distDir := t.TempDir()

  cliPath := filepath.Join(append([]string{rootDir}, cliPathParts...)...)
  schema := filepath.Join(append([]string{rootDir}, schemaPathParts...)...)
  outDir := filepath.Join(append([]string{distDir}, outDirParts...)...)

  cmd := exec.Command("go", "run", cliPath, "gen",
    "-in", schema,
    "-langs", "go",
    "-out-dir", outDir,
  )
  cmd.Dir = rootDir
  out, err := cmd.CombinedOutput()
  require.NoError(t, err, "scheme cli failed: %s", string(out))

  err = os.WriteFile(filepath.Join(distDir, "main.go"), testAppMain, 0600)
  require.NoError(t, err, "failed to write main.go")

  var testAppMod bytes.Buffer
  modTmpl := template.Must(template.New("mod").Parse(string(testAppModTmpl)))
  err = modTmpl.Execute(&testAppMod, struct {
    RootDir string
  }{RootDir: rootDir})
  require.NoError(t, err, "failed to execute test_app_mod template")

  err = os.WriteFile(filepath.Join(distDir, "go.mod"), testAppMod.Bytes(), 0600)
  require.NoError(t, err, "failed to write go.mod")

  tidyCmd := exec.Command("go", "mod", "tidy")
  tidyCmd.Dir = distDir
  out, err = tidyCmd.CombinedOutput()
  require.NoError(t, err, "go mod tidy failed: %s", string(out))

  pgContainer, err := testutil.StartPostgresContainer(ctx)
  require.NoError(t, err, "failed to start postgres container")
  defer func() {
    _ = testutil.StopPostgresContainer(pgContainer)
  }()

  err = pgContainer.SetupTemplateDBWithSchema(
    ctx, "kitchen_sink_template", schema,
  )
  require.NoError(t, err, "failed to setup template db")

  db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
  require.NoError(t, err, "failed to create isolated db")
  defer func() {
    _ = cleanup()
  }()

  // The isolated database connection string is built dynamically because
  // test containers allocate highly randomized port mappings per runtime.
  var actualDBName string
  err = db.QueryRow("SELECT current_database()").Scan(&actualDBName)
  require.NoError(t, err, "failed to get db name")

  connStr, err := pgContainer.ConnectionStringForDB(ctx, actualDBName)
  require.NoError(t, err, "failed to build conn str")

  runCmd := exec.Command("go", "run", "main.go")
  runCmd.Dir = distDir
  runCmd.Env = append(
    os.Environ(),
    "DATABASE_URL="+connStr,
  )

  out, err = runCmd.CombinedOutput()
  require.NoError(t, err, "generated code execution failed: %s", string(out))

  assert.Contains(t, string(out), "SUCCESS", "expected success stdout")
}
