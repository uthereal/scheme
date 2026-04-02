package ast

import (
	"strings"
)

// columnCategoryName represents a strongly typed internal identifier for a
// column's structural behavior.
type columnCategoryName string

// ColumnCategory represents the core generic constraints and types used for
// column generation.
type ColumnCategory struct {
	Name columnCategoryName
	Type string
}

// Column category constants for type safety and to prevent magic string typos.
const (
	colTypeNumber         columnCategoryName = "NumberColumn"
	colTypeString         columnCategoryName = "StringColumn"
	colTypeBoolean        columnCategoryName = "BooleanColumn"
	colTypeTime           columnCategoryName = "TimeColumn"
	colTypeByte           columnCategoryName = "ByteColumn"
	colTypeEnum           columnCategoryName = "EnumColumn"
	colTypeUUID           columnCategoryName = "UUIDColumn"
	colTypeJSON           columnCategoryName = "JSONColumn"
	colTypeArray          columnCategoryName = "ArrayColumn"
	colTypeNetworkAddress columnCategoryName = "NetworkAddressColumn"
	colTypeGeometric      columnCategoryName = "GeometricColumn"
	colTypeRange          columnCategoryName = "RangeColumn"
	colTypeBitString      columnCategoryName = "BitStringColumn"
	colTypeUnsupported    columnCategoryName = "UnsupportedColumn"
	colTypeComposite      columnCategoryName = "Composite"
)

// String returns the string representation of the column category name.
func (c columnCategoryName) String() string {
	return string(c)
}

// String returns the formatted go structural type representing the column.
//
// Examples:
//
//	{Name: "NumberColumn", Type: "int32"} => "NumberColumn[int32]"
//	{Name: "CompositeAddressColumn", Type: "..."} => "CompositeAddressColumn"
//	{Name: "UnsupportedColumn", Type: "any"} => "any"
func (c ColumnCategory) String() string {
	nameStr := c.Name.String()
	typeStr := c.GetType()

	if c.Name == colTypeUnsupported {
		return typeStr
	}
	if strings.HasPrefix(nameStr, string(colTypeComposite)) {
		return nameStr
	}
	return nameStr + "[" + typeStr + "]"
}

// GetType safely returns the inner generic type, defaulting to "any".
func (c ColumnCategory) GetType() string {
	if c.Type == "" {
		return "any"
	}
	return c.Type
}

// SupportOperatorEquality returns true if the column supports basic equality
// constraints.
func (c ColumnCategory) SupportOperatorEquality() bool {
	return true
}

// SupportOperatorMembership returns true if the column supports IN / NOT IN
// array constraints.
func (c ColumnCategory) SupportOperatorMembership() bool {
	switch c.Name {
	case colTypeArray,
		colTypeBoolean,
		colTypeByte,
		colTypeEnum,
		colTypeNetworkAddress,
		colTypeNumber,
		colTypeRange,
		colTypeString,
		colTypeTime,
		colTypeUUID:
		return true
	default:
		return false
	}
}

// SupportOperatorRelational returns true if the column supports >, <, >=, <=
// constraints.
func (c ColumnCategory) SupportOperatorRelational() bool {
	switch c.Name {
	case colTypeNumber,
		colTypeTime,
		colTypeString:
		return true
	default:
		return false
	}
}
