package postgres

import (
	"reflect"
	"testing"

	"github.com/scheme/genproto/spec/postgres"
)

func TestDiffer_DeterministicPlan(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "determinate drop tables"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &postgres.PostgresDatabase{
				Schemas: []*postgres.PostgresSchema{
					{Name: "public"},
				},
			}

			var firstActions []MigrationAction

			for i := 0; i < 10; i++ {
				liveClone := &LiveState{
					Schemas: map[string]*LiveSchema{
						"public": {
							Name: "public",
							Tables: map[string]*LiveTable{
								"table_a": {Name: "table_a"},
								"table_b": {Name: "table_b"},
								"table_c": {Name: "table_c"},
								"table_d": {Name: "table_d"},
								"table_e": {Name: "table_e"},
							},
							Enums:      make(map[string]*LiveEnum),
							Composites: make(map[string]*LiveComposite),
							Domains:    make(map[string]*LiveDomain),
						},
					},
				}

				differ, err := NewDiffer(liveClone, target)
				if err != nil {
					t.Fatalf("failed to init differ -> %v", err)
				}

				err = differ.Plan()
				if err != nil {
					t.Fatalf("failed to plan -> %v", err)
				}

				if i == 0 {
					firstActions = differ.Actions
				} else {
					if !reflect.DeepEqual(firstActions, differ.Actions) {
						t.Fatalf("run %d produced non-deterministic plan", i)
					}
				}
			}
		})
	}
}
