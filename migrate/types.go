package migrate

// DatabaseDataType represents a formatted database data type string.
type DatabaseDataType string

// String returns the underlying string representation.
func (p DatabaseDataType) String() string {
	return string(p)
}

// DatabaseForeignKeyAction represents a foreign key referential action.
type DatabaseForeignKeyAction string

// String returns the underlying string representation.
func (a DatabaseForeignKeyAction) String() string {
	return string(a)
}
