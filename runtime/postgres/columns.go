package postgres

import (
  "fmt"
  "strings"
)

// BaseColumn provides the fundamental SQL representation for a database column.
type BaseColumn string

// ComparableColumn provides standard comparison operators (eq, neq, gt, etc.).
type ComparableColumn[T any] string

// ArrayableColumn provides array-specific operators (overlaps, contains, etc.).
type ArrayableColumn[T any] string

// OrderableColumn is a marker type for columns that can be used in ORDER BY.
type OrderableColumn[T any] string

// DynamicColumn allows for free-form field selection, primarily for
// JSON and Composite types.
type DynamicColumn struct {
  BaseColumn
  ComparableColumn[any]
  ArrayableColumn[any]
  OrderableColumn[any]
}

// ArrayColumn represents a PostgreSQL array column.
type ArrayColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// BitStringColumn represents a PostgreSQL bit or bit varying column.
type BitStringColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// BooleanColumn represents a PostgreSQL boolean column.
type BooleanColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// ByteColumn represents a PostgreSQL bytea column.
type ByteColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// EnumColumn represents a PostgreSQL user-defined enum type.
type EnumColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// GeometricColumn represents PostgreSQL geometric types (point, line, etc.).
type GeometricColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
}

// JSONColumn represents PostgreSQL json or jsonb columns.
type JSONColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
}

// NetworkAddressColumn represents PostgreSQL network types (inet, cidr,
// macaddr).
type NetworkAddressColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// NumberColumn represents PostgreSQL numeric types (integer, decimal, etc.).
type NumberColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// RangeColumn represents PostgreSQL range types (int4range, daterange, etc.).
type RangeColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// StringColumn represents PostgreSQL character types (text, varchar, etc.).
type StringColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// TimeColumn represents PostgreSQL date and time types.
type TimeColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// UUIDColumn represents a PostgreSQL uuid column.
type UUIDColumn[T any] struct {
  BaseColumn
  ComparableColumn[T]
  ArrayableColumn[T]
  OrderableColumn[T]
}

// UnsupportedColumn is a fallback for PostgreSQL types not explicitly
// supported by the generator.
type UnsupportedColumn[T any] struct {
  BaseColumn
}

// Asc returns an ascending order condition for this column.
func (c BaseColumn) Asc() OrderCondition {
  return orderExpr{col: string(c), dir: OrderAsc}
}

// Desc returns a descending order condition for this column.
func (c BaseColumn) Desc() OrderCondition {
  return orderExpr{col: string(c), dir: OrderDesc}
}

// AscNullsFirst returns an ascending order condition with NULLS FIRST.
func (c BaseColumn) AscNullsFirst() OrderCondition {
  return orderExpr{col: string(c), dir: OrderAscNullsFirst}
}

// DescNullsFirst returns a descending order condition with NULLS FIRST.
func (c BaseColumn) DescNullsFirst() OrderCondition {
  return orderExpr{col: string(c), dir: OrderDescNullsFirst}
}

// AscNullsLast returns an ascending order condition with NULLS LAST.
func (c BaseColumn) AscNullsLast() OrderCondition {
  return orderExpr{col: string(c), dir: OrderAscNullsLast}
}

// DescNullsLast returns a descending order condition with NULLS LAST.
func (c BaseColumn) DescNullsLast() OrderCondition {
  return orderExpr{col: string(c), dir: OrderDescNullsLast}
}

// String returns the raw SQL representation of the column.
func (c BaseColumn) String() string {
  return string(c)
}

// Eq returns a WhereCondition for 'column = value'.
func (c ComparableColumn[T]) Eq(val T) WhereCondition {
  return whereExpr{sql: string(c) + " = ?", args: []any{val}}
}

// EqPtr returns a WhereCondition for 'column = value' or 'column IS NULL'
// if the pointer is nil.
func (c ComparableColumn[T]) EqPtr(val *T) WhereCondition {
  if val == nil {
    return c.IsNull()
  }
  return c.Eq(*val)
}

// EqCol returns a WhereCondition for 'column = other_column'.
func (c ComparableColumn[T]) EqCol(other Column) WhereCondition {
  return whereExpr{sql: string(c) + " = " + other.String()}
}

// Neq returns a WhereCondition for 'column != value'.
func (c ComparableColumn[T]) Neq(val T) WhereCondition {
  return whereExpr{sql: string(c) + " != ?", args: []any{val}}
}

// Gt returns a WhereCondition for 'column > value'.
func (c ComparableColumn[T]) Gt(val T) WhereCondition {
  return whereExpr{sql: string(c) + " > ?", args: []any{val}}
}

// Gte returns a WhereCondition for 'column >= value'.
func (c ComparableColumn[T]) Gte(val T) WhereCondition {
  return whereExpr{sql: string(c) + " >= ?", args: []any{val}}
}

// Lt returns a WhereCondition for 'column < value'.
func (c ComparableColumn[T]) Lt(val T) WhereCondition {
  return whereExpr{sql: string(c) + " < ?", args: []any{val}}
}

// Lte returns a WhereCondition for 'column <= value'.
func (c ComparableColumn[T]) Lte(val T) WhereCondition {
  return whereExpr{sql: string(c) + " <= ?", args: []any{val}}
}

// In returns a WhereCondition for 'column IN (value1, value2, ...)'.
func (c ComparableColumn[T]) In(vals ...T) WhereCondition {
  if len(vals) == 0 {
    return nil
  }
  placeholders := make([]string, len(vals))
  args := make([]any, len(vals))
  for i, v := range vals {
    placeholders[i] = "?"
    args[i] = v
  }
  sql := fmt.Sprintf("%s IN (%s)", string(c), strings.Join(placeholders, ", "))
  return whereExpr{sql: sql, args: args}
}

// InQuery returns a WhereCondition for 'column IN (subquery)'.
func (c ComparableColumn[T]) InSubQuery(q SubQuery) WhereCondition {
  return whereSubquery{sql: string(c) + " IN", query: q}
}

// IsNull returns a WhereCondition for 'column IS NULL'.
func (c ComparableColumn[T]) IsNull() WhereCondition {
  return whereExpr{sql: string(c) + " IS NULL"}
}

// IsNotNull returns a WhereCondition for 'column IS NOT NULL'.
func (c ComparableColumn[T]) IsNotNull() WhereCondition {
  return whereExpr{sql: string(c) + " IS NOT NULL"}
}

// Like returns a WhereCondition for 'column LIKE value'.
func (c ComparableColumn[T]) Like(val string) WhereCondition {
  return whereExpr{sql: string(c) + " LIKE ?", args: []any{val}}
}

// ILike returns a WhereCondition for 'column ILIKE value' (case-insensitive).
func (c ComparableColumn[T]) ILike(val string) WhereCondition {
  return whereExpr{sql: string(c) + " ILIKE ?", args: []any{val}}
}

// Contains returns a WhereCondition for 'column LIKE %value%'.
func (c ComparableColumn[T]) Contains(val string) WhereCondition {
  return whereExpr{sql: string(c) + " LIKE ?", args: []any{"%" + val + "%"}}
}

// ContainsFold returns a WhereCondition for 'column ILIKE %value%'.
func (c ComparableColumn[T]) ContainsFold(val string) WhereCondition {
  return whereExpr{sql: string(c) + " ILIKE ?", args: []any{"%" + val + "%"}}
}

// HasPrefix returns a WhereCondition for 'column LIKE value%'.
func (c ComparableColumn[T]) HasPrefix(val string) WhereCondition {
  return whereExpr{sql: string(c) + " LIKE ?", args: []any{val + "%"}}
}

// HasSuffix returns a WhereCondition for 'column LIKE %value'.
func (c ComparableColumn[T]) HasSuffix(val string) WhereCondition {
  return whereExpr{sql: string(c) + " LIKE ?", args: []any{"%" + val}}
}

// Overlaps returns a WhereCondition for 'column && value' (array overlap).
func (c ArrayableColumn[T]) Overlaps(vals []T) WhereCondition {
  return whereExpr{sql: string(c) + " && ?", args: []any{vals}}
}

// ArrayContains returns a WhereCondition for 'column @> value' (array
// contains).
func (c ArrayableColumn[T]) ArrayContains(vals []T) WhereCondition {
  return whereExpr{sql: string(c) + " @> ?", args: []any{vals}}
}

// ContainedBy returns a WhereCondition for 'column <@ value' (array contained
// by).
func (c ArrayableColumn[T]) ContainedBy(vals []T) WhereCondition {
  return whereExpr{sql: string(c) + " <@ ?", args: []any{vals}}
}

// InQuery returns a WhereCondition for '? IN (subquery)'.
func (c ArrayableColumn[T]) InQuery(val T, q SubQuery) WhereCondition {
  return whereSubquery{sql: "? IN", args: []any{val}, query: q}
}

// NotInQuery returns a WhereCondition for '? NOT IN (subquery)'.
func (c ArrayableColumn[T]) NotInQuery(val T, q SubQuery) WhereCondition {
  return whereSubquery{sql: "? NOT IN", args: []any{val}, query: q}
}
