package ast

import "github.com/uthereal/scheme/genproto/postgres"

// SchemaGraph handles parsing PostgreSQL schema into generic graph types.
type SchemaGraph struct {
	Enums      map[string]*Enum
	Composites map[string]*Composite
	Domains    map[string]*Domain
	Functions  map[string]*Function
	Models     map[string]*Model
}

// Domain represents a PostgreSQL domain type.
// @link https://www.postgresql.org/docs/current/domains.html
// Function represents a PostgreSQL function or stored procedure.
type Function struct {
	Name         string
	NamePrevious string
	Arguments    []FunctionArgument
	ReturnType   *postgres.DataType
	Language     string
	Body         string
}

type FunctionArgument struct {
	Name string
	Type *postgres.DataType
}

type Domain struct {
	Name     string
	BaseType *postgres.DataType
}

// Enum represents a PostgreSQL ENUM type.
// @link https://www.postgresql.org/docs/current/datatype-enum.html
type Enum struct {
	Name   string
	Values []EnumValue
}

// EnumValue represents a single value within a PostgreSQL ENUM type.
// @link https://www.postgresql.org/docs/current/datatype-enum.html
type EnumValue struct {
	Name  string
	Value string
}

// Composite represents a PostgreSQL composite type.
// @link https://www.postgresql.org/docs/current/rowtypes.html
type Composite Model

// Model represents a database table mapped to an entity.
type Model struct {
	Name          string
	TableName     string
	SchemaName    string
	TableFullName string
	Fields        []*Field
	Edges         []*Edge
}

// Field represents a single column in a database table.
type Field struct {
	Name       string
	ColumnName string
	Type       *postgres.DataType
	IsNullable bool
}

// Edge represents a relationship between two models.
type Edge struct {
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
