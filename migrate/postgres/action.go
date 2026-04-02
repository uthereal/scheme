package postgres

import (
	"fmt"
)

// ActionType defines the classification of a DDL migration operation.
type ActionType string

// ObjectType defines the PostgreSQL structural target of a migration operation.
type ObjectType string

// MigrationAction represents a single, atomic DDL execution operation.
type MigrationAction struct {
	// The type of action (CREATE, ALTER, DROP, RENAME).
	Type ActionType

	// The architectural object being modified (TABLE, COLUMN, SCHEMA).
	ObjectType ObjectType

	// The schema namespace where the object resides.
	Schema string

	// The fully qualified identifier of the object being modified
	// (e.g. "users", or "users.email").
	Name string

	// The raw, executable PostgreSQL DDL statement.
	SQL string

	// Indicates if the action drops data or performs a strict type coercion.
	IsDestructive bool
}

const (
	ActionTypeCreate ActionType = "CREATE"
	ActionTypeAlter  ActionType = "ALTER"
	ActionTypeDrop   ActionType = "DROP"
	ActionTypeRename ActionType = "RENAME"
)

const (
	ObjectSchema     ObjectType = "SCHEMA"
	ObjectTable      ObjectType = "TABLE"
	ObjectColumn     ObjectType = "COLUMN"
	ObjectEnum       ObjectType = "ENUM"
	ObjectComposite  ObjectType = "COMPOSITE"
	ObjectDomain     ObjectType = "DOMAIN"
	ObjectPrimaryKey ObjectType = "PRIMARY_KEY"
	ObjectForeignKey ObjectType = "FOREIGN_KEY"
	ObjectIndex      ObjectType = "INDEX"
)

// String returns a readable representation of the migration action.
func (m MigrationAction) String() string {
	dest := ""
	if m.IsDestructive {
		dest = " [DESTRUCTIVE]"
	}
	return fmt.Sprintf("%s %s %s.%s%s -> %s",
		m.Type, m.ObjectType, m.Schema, m.Name, dest, m.SQL,
	)
}
