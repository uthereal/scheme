package ast

import (
	"testing"
)

func TestColumnCategory(t *testing.T) {
	tests := []struct {
		name       string
		cat        ColumnCategory
		wantString string
		wantType   string
		wantEq     bool
		wantMem    bool
		wantRel    bool
	}{
		{
			name:       "number column evaluates mapped outputs correctly",
			cat:        ColumnCategory{Name: colTypeNumber, Type: "int32"},
			wantString: "NumberColumn[int32]",
			wantType:   "int32",
			wantEq:     true,
			wantMem:    true,
			wantRel:    true,
		},
		{
			name:       "boolean column evaluates mapped outputs correctly",
			cat:        ColumnCategory{Name: colTypeBoolean, Type: "bool"},
			wantString: "BooleanColumn[bool]",
			wantType:   "bool",
			wantEq:     true,
			wantMem:    true,
			wantRel:    false,
		},
		{
			name:       "unsupported column falls back to generic interface",
			cat:        ColumnCategory{Name: colTypeUnsupported},
			wantString: "any",
			wantType:   "any",
			wantEq:     true,
			wantMem:    false,
			wantRel:    false,
		},
		{
			name: "composite column renders bare structural reference",
			cat: ColumnCategory{
				Name: "CompositeAddressColumn",
				Type: "CompositeAddress",
			},
			wantString: "CompositeAddressColumn",
			wantType:   "CompositeAddress",
			wantEq:     true,
			wantMem:    false,
			wantRel:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStr, wantStr := tt.cat.String(), tt.wantString
			if gotStr != wantStr {
				t.Errorf("String(): got %q, want %q", gotStr, wantStr)
			}

			gotType, wantType := tt.cat.GetType(), tt.wantType
			if gotType != wantType {
				t.Errorf("GetType(): got %q, want %q", gotType, wantType)
			}

			gotEq, wantEq := tt.cat.SupportOperatorEquality(), tt.wantEq
			if gotEq != wantEq {
				t.Errorf("SupportOperatorEquality(): got %v, want %v", gotEq, wantEq)
			}

			gotMem, wantMem := tt.cat.SupportOperatorMembership(), tt.wantMem
			if gotMem != wantMem {
				t.Errorf("SupportOperatorMembership(): got %v, want %v", gotMem, wantMem)
			}

			gotRel, wantRel := tt.cat.SupportOperatorRelational(), tt.wantRel
			if gotRel != wantRel {
				t.Errorf("SupportOperatorRelational(): got %v, want %v", gotRel, wantRel)
			}
		})
	}
}
