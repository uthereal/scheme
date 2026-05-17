package postgres_test

type mockSubQuery struct {
  sql  string
  args []any
}

func (m *mockSubQuery) ToSelectSQL(
  startParamIdx int, withCount bool,
) (string, []any, error) {
  return m.sql, m.args, nil
}
