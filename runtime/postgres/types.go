package postgres

import (
  "context"

  "github.com/jackc/pgx/v5"
  "github.com/jackc/pgx/v5/pgconn"
)

// DBQuerier represents a generic database interface for executing SQL queries.
// It is satisfied by standard pgx types including *pgxpool.Pool, *pgx.Conn,
// and pgx.Tx, enabling consistent query execution across connection types.
type DBQuerier interface {
  // Exec executes a query without returning any rows.
  Exec(
    ctx context.Context, query string, args ...any,
  ) (pgconn.CommandTag, error)

  // Query executes a query that returns multiple rows.
  Query(
    ctx context.Context, query string, args ...any,
  ) (pgx.Rows, error)

  // QueryRow executes a query that is expected to return at most one row.
  QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

// SubQuery represents an abstract SQL query builder that can be nested
// within other queries, such as in WHERE EXISTS or IN clauses.
type SubQuery interface {
  // ToSelectSQL generates the SQL string and arguments for the subquery.
  ToSelectSQL(startParamIdx int, withCount bool) (string, []any, error)
}

// Column represents a strongly typed database column. It provides methods
// for generating ORDER BY conditions with various sort directions and
// null handling behaviors.
type Column interface {
  // Asc returns an ascending order condition for this column.
  Asc() OrderCondition

  // Desc returns a descending order condition for this column.
  Desc() OrderCondition

  // AscNullsFirst returns an ascending order condition with NULLS FIRST.
  AscNullsFirst() OrderCondition

  // DescNullsFirst returns a descending order condition with NULLS FIRST.
  DescNullsFirst() OrderCondition

  // AscNullsLast returns an ascending order condition with NULLS LAST.
  AscNullsLast() OrderCondition

  // DescNullsLast returns a descending order condition with NULLS LAST.
  DescNullsLast() OrderCondition

  // String returns the raw SQL representation of the column (e.g. "users.id").
  String() string
}

// WhereCondition represents a boolean expression in a SQL WHERE clause.
type WhereCondition interface {
  // BuildWhereSQL generates the SQL fragment and arguments for the condition.
  BuildWhereSQL(paramIdx *int) (string, []any, error)
}

// OrderCondition represents a sorting constraint in a SQL ORDER BY clause.
type OrderCondition interface {
  // BuildOrderSQL generates the SQL fragment and arguments for the constraint.
  BuildOrderSQL(paramIdx *int) (string, []any, error)
}

// OrderDirection defines the sort direction and null handling for an
// ORDER BY clause.
type OrderDirection string

const (
  // OrderAsc represents a standard ascending sort.
  OrderAsc OrderDirection = "ASC"

  // OrderDesc represents a standard descending sort.
  OrderDesc OrderDirection = "DESC"

  // OrderAscNullsFirst represents an ascending sort where NULLs appear first.
  OrderAscNullsFirst OrderDirection = "ASC NULLS FIRST"

  // OrderAscNullsLast represents an ascending sort where NULLs appear last.
  OrderAscNullsLast OrderDirection = "ASC NULLS LAST"

  // OrderDescNullsFirst represents a descending sort where NULLs appear first.
  OrderDescNullsFirst OrderDirection = "DESC NULLS FIRST"

  // OrderDescNullsLast represents a descending sort where NULLs appear last.
  OrderDescNullsLast OrderDirection = "DESC NULLS LAST"
)
