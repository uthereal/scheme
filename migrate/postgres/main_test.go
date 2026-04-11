package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/uthereal/scheme/test/testutil"
)

var testContainer *testutil.PostgresContainer

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := testutil.StartPostgresContainer(ctx)
	if err != nil {
		panic(err)
	}

	err = container.SetupTemplateDBWithManualSQL(ctx, "template_empty", nil)
	if err != nil {
		panic(err)
	}

	testContainer = container
	code := m.Run()

	_ = testutil.StopPostgresContainer(container)
	os.Exit(code)
}
