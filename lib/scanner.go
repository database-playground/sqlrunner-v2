package sqlrunner

import (
	"database/sql"
	"fmt"
)

type StringScanner struct {
	value string
}

func (s *StringScanner) Scan(value any) error {
	s.value = fmt.Sprintf("%v", value)
	return nil
}

func (s *StringScanner) Value() string {
	return s.value
}

var _ sql.Scanner = &StringScanner{}
