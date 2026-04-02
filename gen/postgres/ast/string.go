package ast

import (
	"github.com/ettle/strcase"
	"github.com/jinzhu/inflection"
)

// TableNameToStructName converts a PostgreSQL table name to a Go struct name.
//
// Example:
//
//	"users" => "User"
//	"job_handlers" => "JobHandler"
func TableNameToStructName(tableName string) string {
	return inflection.Singular(strcase.ToGoPascal(tableName))
}

// ColumnNameToFieldName converts a PostgreSQL column name to a Go struct field
// name.
//
// Example:
//
//	"user_id" -> "UserID"
//	"user_identity" -> "UserIdentity"
//	"device_uuid" -> "DeviceUUID"
//	"user_id_number" -> "UserIDNumber"
func ColumnNameToFieldName(colName string) string {
	return strcase.ToGoPascal(colName)
}
