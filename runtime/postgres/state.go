package postgres

// QueryState encapsulates the shared state of a SQL query builder, including
// selections, constraints, and pagination parameters.
type QueryState struct {
  // Alias represents the optional table alias used in the query.
  Alias string
  // Selects contains the columns to be retrieved in a SELECT clause.
  Selects []Column
  // Returning contains the columns to be retrieved in a RETURNING clause.
  Returning []Column
  // Wheres contains the conditions to be applied in a WHERE clause.
  Wheres []WhereCondition
  // GroupBys contains the columns to be used in a GROUP BY clause.
  GroupBys []Column
  // Havings contains the conditions to be applied in a HAVING clause.
  Havings []WhereCondition
  // OrderBys contains the sorting constraints for an ORDER BY clause.
  OrderBys []OrderCondition
  // Limit specifies the maximum number of rows to return.
  Limit *int
  // Offset specifies the number of rows to skip before returning results.
  Offset *int
}

// WithAlias returns a new QueryState with the table alias updated.
func (s QueryState) WithAlias(alias string) QueryState {
  s.Alias = alias
  return s
}

// WithSelects returns a new QueryState with additional columns added to
// the SELECT clause.
func (s QueryState) WithSelects(cols ...Column) QueryState {
  if len(cols) == 0 {
    return s
  }
  newSelects := make([]Column, len(s.Selects), len(s.Selects)+len(cols))
  copy(newSelects, s.Selects)
  s.Selects = append(newSelects, cols...)
  return s
}

// WithReturning returns a new QueryState with additional columns added to
// the RETURNING clause.
func (s QueryState) WithReturning(cols ...Column) QueryState {
  if len(cols) == 0 {
    return s
  }
  newRet := make([]Column, len(s.Returning), len(s.Returning)+len(cols))
  copy(newRet, s.Returning)
  s.Returning = append(newRet, cols...)
  return s
}

// WithWheres returns a new QueryState with additional conditions added to
// the WHERE clause. Nil conditions are automatically ignored.
func (s QueryState) WithWheres(conds ...WhereCondition) QueryState {
  if len(conds) == 0 {
    return s
  }
  newWheres := make(
    []WhereCondition, len(s.Wheres), len(s.Wheres)+len(conds),
  )
  copy(newWheres, s.Wheres)
  for _, c := range conds {
    if c != nil {
      newWheres = append(newWheres, c)
    }
  }
  s.Wheres = newWheres
  return s
}

// WithGroupBys returns a new QueryState with additional columns added to
// the GROUP BY clause.
func (s QueryState) WithGroupBys(cols ...Column) QueryState {
  if len(cols) == 0 {
    return s
  }
  newGB := make([]Column, len(s.GroupBys), len(s.GroupBys)+len(cols))
  copy(newGB, s.GroupBys)
  s.GroupBys = append(newGB, cols...)
  return s
}

// WithHavings returns a new QueryState with additional conditions added to
// the HAVING clause. Nil conditions are automatically ignored.
func (s QueryState) WithHavings(conds ...WhereCondition) QueryState {
  if len(conds) == 0 {
    return s
  }
  newHavings := make(
    []WhereCondition, len(s.Havings), len(s.Havings)+len(conds),
  )
  copy(newHavings, s.Havings)
  for _, c := range conds {
    if c != nil {
      newHavings = append(newHavings, c)
    }
  }
  s.Havings = newHavings
  return s
}

// WithOrderBys returns a new QueryState with additional sorting constraints
// added to the ORDER BY clause. Nil constraints are automatically ignored.
func (s QueryState) WithOrderBys(conds ...OrderCondition) QueryState {
  if len(conds) == 0 {
    return s
  }
  newOB := make(
    []OrderCondition, len(s.OrderBys), len(s.OrderBys)+len(conds),
  )
  copy(newOB, s.OrderBys)
  for _, c := range conds {
    if c != nil {
      newOB = append(newOB, c)
    }
  }
  s.OrderBys = newOB
  return s
}

// WithLimit returns a new QueryState with the LIMIT parameter updated.
func (s QueryState) WithLimit(limit int) QueryState {
  s.Limit = &limit
  return s
}

// WithOffset returns a new QueryState with the OFFSET parameter updated.
func (s QueryState) WithOffset(offset int) QueryState {
  s.Offset = &offset
  return s
}
