package postgres

import (
  "sort"
  "strconv"
  "strings"
)

// BuildSelectSQL assembles a complete parameterized SELECT SQL statement and
// its arguments from a given QueryState.
func BuildSelectSQL(
  state QueryState,
  selectStr string,
  tableName string,
  startParamIdx int,
  withCount bool,
) (string, []any, error) {
  var query strings.Builder
  var numBuf [20]byte
  query.Grow(256)
  query.WriteString("SELECT ")
  query.WriteString(selectStr)

  if withCount {
    query.WriteString(", COUNT(*) OVER()")
  }
  query.WriteString(" FROM ")
  query.WriteString(tableName)

  if state.Alias != "" {
    query.WriteString(" AS ")
    query.WriteString(state.Alias)
  }

  var args []any
  paramIdx := startParamIdx

  if len(state.Wheres) > 0 {
    query.WriteString(" WHERE ")
    wSQL, wArgs, err := BuildConditions(state.Wheres, &paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(wSQL)
    args = append(args, wArgs...)
  }

  if len(state.GroupBys) > 0 {
    query.WriteString(" GROUP BY ")
    for i, gb := range state.GroupBys {
      if i > 0 {
        query.WriteString(", ")
      }
      query.WriteString(gb.String())
    }
  }

  if len(state.Havings) > 0 {
    query.WriteString(" HAVING ")
    hSQL, hArgs, err := BuildConditions(state.Havings, &paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(hSQL)
    args = append(args, hArgs...)
  }

  if len(state.OrderBys) > 0 {
    query.WriteString(" ORDER BY ")
    for i, ob := range state.OrderBys {
      if i > 0 {
        query.WriteString(", ")
      }
      obSQL, obArgs, err := ob.BuildOrderSQL(&paramIdx)
      if err != nil {
        return "", nil, err
      }
      query.WriteString(obSQL)
      args = append(args, obArgs...)
    }
  }

  if state.Limit != nil {
    query.WriteString(" LIMIT $")
    query.Write(strconv.AppendInt(numBuf[:0], int64(paramIdx), 10))
    args = append(args, *state.Limit)
    paramIdx++
  }

  if state.Offset != nil {
    query.WriteString(" OFFSET $")
    query.Write(strconv.AppendInt(numBuf[:0], int64(paramIdx), 10))
    args = append(args, *state.Offset)
    paramIdx++
  }

  return query.String(), args, nil
}

// BuildExistsSQL assembles an optimized SELECT EXISTS(...) SQL statement.
func BuildExistsSQL(
  state QueryState,
  tableName string,
) (string, []any, error) {
  var query strings.Builder
  query.Grow(256)
  query.WriteString("SELECT EXISTS(SELECT 1 FROM ")
  query.WriteString(tableName)

  if state.Alias != "" {
    query.WriteString(" AS ")
    query.WriteString(state.Alias)
  }

  var args []any
  paramIdx := 1

  if len(state.Wheres) > 0 {
    query.WriteString(" WHERE ")
    wSQL, wArgs, err := BuildConditions(state.Wheres, &paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(wSQL)
    args = append(args, wArgs...)
  }

  if len(state.GroupBys) > 0 {
    query.WriteString(" GROUP BY ")
    for i, gb := range state.GroupBys {
      if i > 0 {
        query.WriteString(", ")
      }
      query.WriteString(gb.String())
    }
  }

  if len(state.Havings) > 0 {
    query.WriteString(" HAVING ")
    hSQL, hArgs, err := BuildConditions(state.Havings, &paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(hSQL)
    args = append(args, hArgs...)
  }

  query.WriteString(")")
  return query.String(), args, nil
}

// BuildDeleteSQL assembles a DELETE SQL statement.
func BuildDeleteSQL(
  state QueryState,
  tableName string,
) (string, []any, error) {
  var query strings.Builder
  query.Grow(128)
  query.WriteString("DELETE FROM ")
  query.WriteString(tableName)

  if state.Alias != "" {
    query.WriteString(" AS ")
    query.WriteString(state.Alias)
  }

  var args []any
  paramIdx := 1

  if len(state.Wheres) > 0 {
    query.WriteString(" WHERE ")
    wSQL, wArgs, err := BuildConditions(state.Wheres, &paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(wSQL)
    args = append(args, wArgs...)
  }

  return query.String(), args, nil
}

// BuildUpdateSQL assembles a deterministic UPDATE SQL statement. It sorts the
// mutation columns alphabetically to ensure consistent SQL generation.
func BuildUpdateSQL(
  state QueryState,
  tableName string,
  m map[Column]any,
) (string, []any, error) {
  var query strings.Builder
  var numBuf [20]byte
  query.Grow(256)
  query.WriteString("UPDATE ")
  query.WriteString(tableName)

  if state.Alias != "" {
    query.WriteString(" AS ")
    query.WriteString(state.Alias)
  }
  query.WriteString(" SET ")

  // Sort columns for deterministic SQL generation
  cols := make([]Column, 0, len(m))
  for col := range m {
    cols = append(cols, col)
  }
  sort.Slice(cols, func(i, j int) bool {
    return cols[i].String() < cols[j].String()
  })

  var args []any
  paramIdx := 1

  for i, col := range cols {
    if i > 0 {
      query.WriteString(", ")
    }
    query.WriteString(col.String())
    val := m[col]
    if val == nil {
      query.WriteString(" = NULL")
    } else {
      query.WriteString(" = $")
      query.Write(strconv.AppendInt(numBuf[:0], int64(paramIdx), 10))
      args = append(args, val)
      paramIdx++
    }
  }

  if len(state.Wheres) > 0 {
    query.WriteString(" WHERE ")
    wSQL, wArgs, err := BuildConditions(state.Wheres, &paramIdx)
    if err != nil {
      return "", nil, err
    }
    query.WriteString(wSQL)
    args = append(args, wArgs...)
  }

  return query.String(), args, nil
}
