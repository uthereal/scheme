package postgres

import (
  "strings"
)

// BuildSelectString constructs the comma-separated SELECT clause string and
// extracts the raw, unquoted column names from a slice of Columns.
// It handles stripping table prefixes and "AS" aliases to ensure consistent
// mapping back to struct fields.
func BuildSelectString(cols []Column) (string, []string, error) {
  if len(cols) == 0 {
    return "", nil, nil
  }

  var colStr strings.Builder
  colStr.Grow(len(cols) * 32)
  rawNames := make([]string, len(cols))

  for i, col := range cols {
    if i > 0 {
      colStr.WriteString(", ")
    }
    cName := col.String()
    colStr.WriteString(cName)

    // Strip aliases: "table.col AS alias" -> "table.col"
    asIdx := strings.Index(strings.ToUpper(cName), " AS ")
    if asIdx >= 0 {
      cName = cName[:asIdx]
    }

    // Strip table prefixes: "table.col" -> "col"
    dotIdx := strings.LastIndexByte(cName, '.')
    if dotIdx >= 0 {
      cName = cName[dotIdx+1:]
    }

    // Strip quotes: "\"col\"" -> "col"
    cName = strings.Trim(cName, `"`)
    rawNames[i] = cName
  }

  return colStr.String(), rawNames, nil
}
