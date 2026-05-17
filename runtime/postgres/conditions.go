package postgres

import (
  "strconv"
  "strings"
)

// whereExpr implements WhereCondition for a raw SQL expression with
// placeholders.
type whereExpr struct {
  sql  string
  args []any
}

// whereSubquery implements WhereCondition for subquery-based operations
// such as EXISTS or IN.
type whereSubquery struct {
  sql   string
  args  []any
  query SubQuery
}

// whereGroup implements WhereCondition for logical groupings (AND, OR, NOT)
// of multiple other conditions.
type whereGroup struct {
  conds []WhereCondition
  isOr  bool
  isNot bool
}

// orderExpr implements OrderCondition for a simple column and direction sort.
type orderExpr struct {
  col string
  dir OrderDirection
}

// orderSubquery implements OrderCondition for a subquery sort direction.
type orderSubquery struct {
  query SubQuery
  dir   OrderDirection
}

// BuildWhereSQL generates the SQL fragment and arguments for a raw expression.
func (w whereExpr) BuildWhereSQL(paramIdx *int) (string, []any, error) {
  var query strings.Builder
  var args []any
  var numBuf [20]byte

  parts := strings.Split(w.sql, "?")
  for j, part := range parts {
    query.WriteString(part)
    if j < len(parts)-1 && j < len(w.args) {
      query.WriteString("$")
      query.Write(strconv.AppendInt(numBuf[:0], int64(*paramIdx), 10))
      args = append(args, w.args[j])
      *paramIdx++
    }
  }
  return query.String(), args, nil
}

// BuildWhereSQL generates the SQL fragment and arguments for a subquery
// condition.
func (w whereSubquery) BuildWhereSQL(paramIdx *int) (string, []any, error) {
  var query strings.Builder
  var args []any
  var numBuf [20]byte

  parts := strings.Split(w.sql, "?")
  for j, part := range parts {
    query.WriteString(part)
    if j < len(parts)-1 && j < len(w.args) {
      query.WriteString("$")
      query.Write(strconv.AppendInt(numBuf[:0], int64(*paramIdx), 10))
      args = append(args, w.args[j])
      *paramIdx++
    }
  }
  query.WriteString(" (")
  subSQL, subArgs, err := w.query.ToSelectSQL(*paramIdx, false)
  if err != nil {
    return "", nil, err
  }
  query.WriteString(subSQL)
  query.WriteString(")")
  args = append(args, subArgs...)
  *paramIdx += len(subArgs)

  return query.String(), args, nil
}

// BuildWhereSQL generates the SQL fragment and arguments for a logical group.
func (w whereGroup) BuildWhereSQL(paramIdx *int) (string, []any, error) {
  var query strings.Builder
  var args []any

  validConds := make([]WhereCondition, 0, len(w.conds))
  for _, c := range w.conds {
    if c != nil {
      validConds = append(validConds, c)
    }
  }

  if len(validConds) == 0 {
    return "", nil, nil
  }

  if w.isNot {
    query.WriteString("NOT ")
  }

  query.WriteString("(")
  for i, cond := range validConds {
    if i > 0 {
      if w.isOr {
        query.WriteString(" OR ")
      } else {
        query.WriteString(" AND ")
      }
    }

    cSQL, cArgs, err := cond.BuildWhereSQL(paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(cSQL)
    args = append(args, cArgs...)
  }
  query.WriteString(")")

  return query.String(), args, nil
}

// And returns a WhereCondition that joins multiple conditions with AND.
func And(conds ...WhereCondition) WhereCondition {
  return whereGroup{conds: conds, isOr: false}
}

// Or returns a WhereCondition that joins multiple conditions with OR.
func Or(conds ...WhereCondition) WhereCondition {
  return whereGroup{conds: conds, isOr: true}
}

// Not returns a WhereCondition that negates the given conditions.
func Not(conds ...WhereCondition) WhereCondition {
  return whereGroup{conds: conds, isNot: true}
}

// Exists returns a WhereCondition for 'EXISTS (subquery)'.
func Exists(q SubQuery) WhereCondition {
  return whereSubquery{sql: "EXISTS", query: q}
}

// NotExists returns a WhereCondition for 'NOT EXISTS (subquery)'.
func NotExists(q SubQuery) WhereCondition {
  return whereSubquery{sql: "NOT EXISTS", query: q}
}

// BuildConditions assembles multiple WhereConditions into a single AND-joined
// SQL fragment.
func BuildConditions(
  conds []WhereCondition, paramIdx *int,
) (string, []any, error) {
  if len(conds) == 0 {
    return "", nil, nil
  }
  var query strings.Builder
  var args []any

  validConds := make([]WhereCondition, 0, len(conds))
  for _, c := range conds {
    if c != nil {
      validConds = append(validConds, c)
    }
  }

  if len(validConds) == 0 {
    return "", nil, nil
  }

  for i, cond := range validConds {
    if i > 0 {
      query.WriteString(" AND ")
    }
    cSQL, cArgs, err := cond.BuildWhereSQL(paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(cSQL)
    args = append(args, cArgs...)
  }
  return query.String(), args, nil
}

// BuildOrderSQL generates the SQL fragment for a simple column sort.
func (o orderExpr) BuildOrderSQL(paramIdx *int) (string, []any, error) {
  return o.col + " " + string(o.dir), nil, nil
}

// BuildOrderSQL generates the SQL fragment and arguments for a subquery sort.
func (o orderSubquery) BuildOrderSQL(paramIdx *int) (string, []any, error) {
  var query strings.Builder
  query.WriteString("(")
  subSQL, subArgs, err := o.query.ToSelectSQL(*paramIdx, false)
  if err != nil {
    return "", nil, err
  }
  query.WriteString(subSQL)
  query.WriteString(") ")
  query.WriteString(string(o.dir))

  *paramIdx += len(subArgs)
  return query.String(), subArgs, nil
}

// OrderSubQuery returns an OrderCondition for a subquery sort direction.
func OrderSubQuery(q SubQuery, dir OrderDirection) OrderCondition {
  return orderSubquery{query: q, dir: dir}
}

// Expr returns a WhereCondition from a raw SQL string and arguments.
func Expr(sql string, args ...any) WhereCondition {
  return whereExpr{sql: sql, args: args}
}
