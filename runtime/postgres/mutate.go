package postgres

// MutateOption defines a strongly-typed functional argument for
// providing column values during insert, update, or upsert operations.
type MutateOption func(map[Column]any)
