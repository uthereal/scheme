package postgres

// CountPagination holds a paginated result set of items of type T
// alongside the total count of available records matching the query.
type CountPagination[T any] struct {
  // Items contains the slice of records for the current page.
  Items []T
  // TotalCount is the total number of records matching the query constraints.
  TotalCount int
}

// SimplePagination holds a paginated result set of items of type T
// and a boolean indicating if more records exist beyond the requested page.
type SimplePagination[T any] struct {
  // Items contains the slice of records for the current page.
  Items []T
  // HasMore is true if at least one more record exists beyond this page.
  HasMore bool
}
