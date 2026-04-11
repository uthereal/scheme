package ast

import (
	"testing"
)

func TestSupportOperatorEquality(t *testing.T) {
	tests := []struct {
		name string
		cat  ColumnCategory
		want bool
	}{
		{
			name: "number column evaluates mapped outputs correctly",
			cat:  ColumnCategory{Name: ColTypeNumber, Type: "int32"},
			want: true,
		},
		{
			name: "boolean column evaluates mapped outputs correctly",
			cat:  ColumnCategory{Name: ColTypeBoolean, Type: "bool"},
			want: true,
		},
		{
			name: "unsupported column falls back to generic interface",
			cat:  ColumnCategory{Name: ColTypeUnsupported},
			want: true,
		},
		{
			name: "composite column renders bare structural reference",
			cat: ColumnCategory{
				Name: "CompositeAddressColumn",
				Type: "CompositeAddress",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cat.SupportOperatorEquality()
			if got != tt.want {
				t.Errorf("SupportOperatorEquality(): got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSupportOperatorMembership(t *testing.T) {
	tests := []struct {
		name string
		cat  ColumnCategory
		want bool
	}{
		{
			name: "number column evaluates mapped outputs correctly",
			cat:  ColumnCategory{Name: ColTypeNumber, Type: "int32"},
			want: true,
		},
		{
			name: "boolean column evaluates mapped outputs correctly",
			cat:  ColumnCategory{Name: ColTypeBoolean, Type: "bool"},
			want: true,
		},
		{
			name: "unsupported column falls back to generic interface",
			cat:  ColumnCategory{Name: ColTypeUnsupported},
			want: false,
		},
		{
			name: "composite column renders bare structural reference",
			cat: ColumnCategory{
				Name: "CompositeAddressColumn",
				Type: "CompositeAddress",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cat.SupportOperatorMembership()
			if got != tt.want {
				t.Errorf("SupportOperatorMembership(): got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSupportOperatorRelational(t *testing.T) {
	tests := []struct {
		name string
		cat  ColumnCategory
		want bool
	}{
		{
			name: "number column evaluates mapped outputs correctly",
			cat:  ColumnCategory{Name: ColTypeNumber, Type: "int32"},
			want: true,
		},
		{
			name: "boolean column evaluates mapped outputs correctly",
			cat:  ColumnCategory{Name: ColTypeBoolean, Type: "bool"},
			want: false,
		},
		{
			name: "unsupported column falls back to generic interface",
			cat:  ColumnCategory{Name: ColTypeUnsupported},
			want: false,
		},
		{
			name: "composite column renders bare structural reference",
			cat: ColumnCategory{
				Name: "CompositeAddressColumn",
				Type: "CompositeAddress",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cat.SupportOperatorRelational()
			if got != tt.want {
				t.Errorf("SupportOperatorRelational(): got %v, want %v", got, tt.want)
			}
		})
	}
}
