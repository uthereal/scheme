package ast

// ColumnCategory represents the core generic constraints and types used for
// column generation.
type ColumnCategory struct {
	Name columnCategoryName
	Type string
}

// columnCategoryName represents a strongly typed internal identifier for a
// column's structural behavior.
type columnCategoryName string

// Column category constants for type safety and to prevent magic string typos.
const (
	ColTypeArray          columnCategoryName = "ArrayColumn"
	ColTypeBitString      columnCategoryName = "BitStringColumn"
	ColTypeBoolean        columnCategoryName = "BooleanColumn"
	ColTypeByte           columnCategoryName = "ByteColumn"
	ColTypeComposite      columnCategoryName = "Composite"
	ColTypeEnum           columnCategoryName = "EnumColumn"
	ColTypeGeometric      columnCategoryName = "GeometricColumn"
	ColTypeJSON           columnCategoryName = "JSONColumn"
	ColTypeNetworkAddress columnCategoryName = "NetworkAddressColumn"
	ColTypeNumber         columnCategoryName = "NumberColumn"
	ColTypeRange          columnCategoryName = "RangeColumn"
	ColTypeString         columnCategoryName = "StringColumn"
	ColTypeTime           columnCategoryName = "TimeColumn"
	ColTypeUnsupported    columnCategoryName = "UnsupportedColumn"
	ColTypeUUID           columnCategoryName = "UUIDColumn"
)

// String returns the string representation of the column category name.
func (c columnCategoryName) String() string {
	return string(c)
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
	case ColTypeArray,
		ColTypeBoolean,
		ColTypeByte,
		ColTypeEnum,
		ColTypeNetworkAddress,
		ColTypeNumber,
		ColTypeRange,
		ColTypeString,
		ColTypeTime,
		ColTypeUUID:
		return true
	default:
		return false
	}
}

// SupportOperatorRelational returns true if the column supports >, <, >=, <=
// constraints.
func (c ColumnCategory) SupportOperatorRelational() bool {
	switch c.Name {
	case ColTypeNumber,
		ColTypeTime,
		ColTypeString:
		return true
	default:
		return false
	}
}
