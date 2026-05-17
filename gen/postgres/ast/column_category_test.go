package ast

import (
  "testing"

  "github.com/stretchr/testify/assert"
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
      assert.Equal(t, tt.want, got, "SupportOperatorEquality() mismatch")
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
      assert.Equal(t, tt.want, got, "SupportOperatorMembership() mismatch")
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
      assert.Equal(t, tt.want, got, "SupportOperatorRelational() mismatch")
    })
  }
}
