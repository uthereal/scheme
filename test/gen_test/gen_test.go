package gen_test

import (
	"context"
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/uthereal/scheme/test/testutil"
)

//go:embed test_app_main.go.tmpl
var testAppMain []byte

//go:embed test_app_mod.go.tmpl
var testAppMod []byte

var (
	cliPathParts    = []string{"cmd", "scheme", "main.go"}
	schemaPathParts = []string{"test", "testdata", "kitchen_sink.textproto"}
	outDirParts     = []string{"gen"}
)

func TestPostgresGeneratedCode(t *testing.T) {
	ctx := context.Background()

	// Resolving the root directory anchors the subprocess command execution,
	// allowing the generator CLI to be invoked exactly as if the user ran it
	// manually from the base of the repository.
	rootDir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("failed to get root dir -> %v", err)
	}

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
		"-go-pkg-prefix", "integration/gen",
	)
	cmd.Dir = rootDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scheme cli failed -> %s\n%v", string(out), err)
	}

	err = os.WriteFile(filepath.Join(distDir, "main.go"), testAppMain, 0600)
	if err != nil {
		t.Fatalf("failed to write main.go -> %v", err)
	}

	err = os.WriteFile(filepath.Join(distDir, "go.mod"), testAppMod, 0600)
	if err != nil {
		t.Fatalf("failed to write go.mod -> %v", err)
	}

	tidyCmd := exec.Command("go", "mod", "tidy")
	tidyCmd.Dir = distDir
	out, err = tidyCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go mod tidy failed -> %s\n%v", string(out), err)
	}

	pgContainer, err := testutil.StartPostgresContainer(ctx)
	if err != nil {
		t.Fatalf("failed to start postgres container -> %v", err)
	}
	defer func() {
		_ = testutil.StopPostgresContainer(pgContainer)
	}()

	err = pgContainer.SetupTemplateDBWithSchema(
		ctx, "kitchen_sink_template", schema,
	)
	if err != nil {
		t.Fatalf("failed to setup template db -> %v", err)
	}

	db, cleanup, err := pgContainer.CreateIsolatedDB(ctx, t)
	if err != nil {
		t.Fatalf("failed to create isolated db -> %v", err)
	}
	defer func() {
		_ = cleanup()
	}()

	// The isolated database connection string is built dynamically because
	// test containers allocate highly randomized port mappings per runtime.
	var actualDBName string
	err = db.QueryRow("SELECT current_database()").Scan(&actualDBName)
	if err != nil {
		t.Fatalf("failed to get db name -> %v", err)
	}

	connStr, err := pgContainer.ConnectionStringForDB(ctx, actualDBName)
	if err != nil {
		t.Fatalf("failed to build conn str -> %v", err)
	}

	runCmd := exec.Command("go", "run", "main.go")
	runCmd.Dir = distDir
	runCmd.Env = append(
		os.Environ(),
		"DATABASE_URL="+connStr,
	)

	out, err = runCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("generated code execution failed -> %s\n%v", string(out), err)
	}

	if !strings.Contains(string(out), "SUCCESS") {
		t.Fatalf("expected success stdout, got -> %s", string(out))
	}
}
