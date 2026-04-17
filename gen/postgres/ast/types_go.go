package ast

import "strings"

// SchemaGraphGo wraps SchemaGraph for Go specific generation.
type SchemaGraphGo struct {
	Graph *SchemaGraph

	GoPkgName        string
	GoPkgPath        string
	Imports          map[string]struct{}
	Models           []*ModelGo
	Enums            []*EnumGo
	Domains          []*DomainGo
	Composites       []*CompositeGo
	Functions        []*FunctionGo
	ActiveCategories map[string]bool
}

// EnumGo wraps Enum for Go specific generation.
type EnumGo struct {
	*Enum
}

// DomainGo wraps Domain for Go specific generation.
// FunctionGo wraps FunctionDefinition for Go specific generation.
type FunctionGo struct {
	NameExported string
	*Function
	ArgumentsGo  []FunctionArgumentGo
	ReturnTypeGo string
}

type FunctionArgumentGo struct {
	Index  int
	Name   string
	GoType string
}

type DomainGo struct {
	*Domain
	BaseGoType string
}

// CompositeGo alias for ModelGo.
type CompositeGo ModelGo

// ModelGo wraps Model for Go specific generation.
type ModelGo struct {
	*Model
	StructNamePrivate string
	Fields            []*FieldGo
	Edges             []*EdgeGo
}

// FieldGo wraps Field for Go specific generation.
type FieldGo struct {
	*Field
	GoBaseType string
	IsPtr      bool
	Category   ColumnCategory
}

// EdgeGo wraps Edge for Go specific generation.
type EdgeGo struct {
	*Edge
	FieldTypes       []string
	LocalIsNullable  []bool
	TargetIsNullable []bool
}

// String returns the formatted go structural type representing the column
// category.
//
// Examples:
//
//	{Name: "NumberColumn", Type: "int32"} => "NumberColumn[int32]"
//	{Name: "CompositeAddressColumn", Type: "..."} => "CompositeAddressColumn"
//	{Name: "UnsupportedColumn", Type: "any"} => "any"
func (f *FieldGo) String() string {
	nameStr := f.Category.Name.String()
	typeStr := f.GoBaseType
	if typeStr == "" {
		typeStr = "any"
	}

	if f.Category.Name == ColTypeUnsupported {
		return typeStr
	}
	if strings.HasPrefix(nameStr, string(ColTypeComposite)) {
		return nameStr
	}
	return "rootpkg." + nameStr + "[" + typeStr + "]"
}

// GoType returns the GoBaseType prefixed with a pointer (*) if IsPtr is true.
func (f *FieldGo) GoType() string {
	if f.IsPtr {
		return "*" + f.GoBaseType
	}
	return f.GoBaseType
}
