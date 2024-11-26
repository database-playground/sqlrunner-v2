package sqlrunner

// SchemaError is returned when the schema registeration failed.
type SchemaError struct {
	Parent error
}

// QueryError is returned when a query fails.
type QueryError struct {
	Parent error
}

func NewSchemaError(err error) error {
	return SchemaError{Parent: err}
}

func NewQueryError(err error) error {
	return QueryError{Parent: err}
}

func (e SchemaError) Error() string {
	return "invalid schema: " + e.Parent.Error()
}

func (e QueryError) Error() string {
	return "query error: " + e.Parent.Error()
}
