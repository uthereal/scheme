package ast

// DomainData represents a PostgreSQL domain type mapped to Go.
// @link https://www.postgresql.org/docs/current/domains.html
type DomainData struct {
	Name     string
	BaseType string
}

// EnumData represents a PostgreSQL ENUM type mapped to Go.
// @link https://www.postgresql.org/docs/current/datatype-enum.html
type EnumData struct {
	Name   string
	Values []EnumDataValue
}

// EnumDataValue represents a single value within a PostgreSQL ENUM type.
// @link https://www.postgresql.org/docs/current/datatype-enum.html
type EnumDataValue struct {
	Name  string
	Value string
}

// ModelData represents a database table mapped to a Go struct.
type ModelData struct {
	StructNameExported string
	StructNamePrivate  string
	TableName          string
	SchemaName         string
	TableFullName      string
	Fields             []ModelDataField
	Edges              []EdgeData
}

// EdgeData represents a relationship between two models, tracking the
// structural and metadata elements required to map the relationship.
type EdgeData struct {
	Name          string
	NameLower     string
	TargetModel   string
	IsSlice       bool
	LocalColumns  []string
	TargetColumns []string
	LocalFields   []string
	TargetFields  []string
	IsBackRef     bool
}

// ModelDataField represents a single column in a database table mapped to a Go
// struct field.
type ModelDataField struct {
	Name       string
	ColumnName string
	Type       string
	ColumnType ColumnCategory
}
