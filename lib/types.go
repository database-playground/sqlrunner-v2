package sqlrunner

// QueryResult is a struct that holds the result of a query
type QueryResult struct {
	// Columns is a slice of column names
	Columns []string `json:"columns"`
	// Rows is a slice of rows, each row is a slice of strings
	Rows [][]string `json:"rows"`
}
