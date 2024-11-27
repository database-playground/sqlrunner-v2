package sqlrunner

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"
)

type StringScanner struct {
	value string
}

func (s *StringScanner) Scan(value any) error {
	switch v := value.(type) {
	case int64:
		s.value = strconv.FormatInt(v, 10)
	case float64:
		s.value = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			s.value = "1"
		} else {
			s.value = "0"
		}
	case []byte:
		s.value = hex.EncodeToString(v)
	case string:
		s.value = v
	case time.Time:
		s.value = v.Format("2006-01-02 15:04:05")
	case nil:
		s.value = "NULL"
	default:
		s.value = fmt.Sprintf("%v", value)
	}

	return nil
}

func (s *StringScanner) Value() string {
	return s.value
}

var _ sql.Scanner = &StringScanner{}
