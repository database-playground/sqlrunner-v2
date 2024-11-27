// Package sqlrunner provides a wrapper of SQLite that implements the
// cache, timed out, and MySQL-compatible functions.
package sqlrunner

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"
	"modernc.org/sqlite"
	_ "modernc.org/sqlite"
)

var sf = &singleflight.Group{}

func init() {
	// MySQL-compatible functions
	sqlite.MustRegisterFunction("YEAR", &sqlite.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		Scalar: func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			d, err := parseSqliteDate(args[0])
			if err != nil {
				return nil, fmt.Errorf("parse date: %w", err)
			}

			return int64(d.Year()), nil
		},
	})

	sqlite.MustRegisterFunction("MONTH", &sqlite.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		Scalar: func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			d, err := parseSqliteDate(args[0])
			if err != nil {
				return nil, fmt.Errorf("parse date: %w", err)
			}

			return int64(d.Month()), nil
		},
	})

	sqlite.MustRegisterFunction("DAY", &sqlite.FunctionImpl{
		NArgs:         1,
		Deterministic: true,
		Scalar: func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			d, err := parseSqliteDate(args[0])
			if err != nil {
				return nil, fmt.Errorf("parse date: %w", err)
			}

			return int64(d.Day()), nil
		},
	})

	sqlite.MustRegisterFunction("LEFT", &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		Scalar: func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			str, ok := args[0].(string)
			if !ok {
				return nil, fmt.Errorf("invalid argument type: %T", args[0])
			}

			length, ok := args[1].(int64)
			if !ok {
				return nil, fmt.Errorf("invalid argument type: %T", args[1])
			}

			if length < 0 {
				return nil, fmt.Errorf("negative length: %d", length)
			}

			if int(length) > len(str) {
				return str, nil
			}

			return str[:length], nil
		},
	})

	sqlite.MustRegisterFunction("IF", &sqlite.FunctionImpl{
		NArgs:         3,
		Deterministic: true,
		Scalar: func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			condition, ok := args[0].(bool)
			if !ok {
				conditionInt64, ok := args[0].(int64)
				if !ok {
					return nil, fmt.Errorf("invalid argument type: %T", args[0])
				}

				condition = conditionInt64 != 0
			}

			if condition {
				return args[1], nil
			}

			return args[2], nil
		},
	})
}

const tmpDir = "/tmp/sqlrunner"

type SQLRunner struct {
	schema string

	cache *lru.Cache[string, *QueryResult]
}

func NewSQLRunner(schema string) (*SQLRunner, error) {
	_ = os.MkdirAll(tmpDir, 0o755)

	cache, err := lru.New[string, *QueryResult](100)
	if err != nil {
		return nil, fmt.Errorf("create lru cache: %w", err)
	}

	runner := &SQLRunner{
		schema: schema,
		cache:  cache,
	}

	// Initialize the SQLite instance early to
	// make sure the schema is valid.
	_, err = runner.getSqliteInstance()
	if err != nil {
		return nil, fmt.Errorf("initialize sqlite: %w", err)
	}

	return runner, nil
}

// Query executes a query and returns the result.
func (r *SQLRunner) Query(ctx context.Context, query string) (*QueryResult, error) {
	// Check the cache first
	if result, ok := r.cache.Get(query); ok {
		return result, nil
	}

	db, err := r.getSqliteInstance()
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.WarnContext(ctx, "close schema database", slog.Any("error", err))
		}
	}()

	result, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, NewQueryError(err)
	}
	defer func() {
		if err := result.Close(); err != nil {
			slog.WarnContext(ctx, "close result", slog.Any("error", err))
		}
	}()

	cols, err := result.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	rows := [][]string{}
	for result.Next() {
		rawCells := make([]any, 0, len(cols))
		for range cols {
			rawCells = append(rawCells, &StringScanner{})
		}

		if err := result.Scan(rawCells...); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		row := make([]string, 0, len(cols))
		for _, cell := range rawCells {
			row = append(row, cell.(*StringScanner).Value())
		}

		rows = append(rows, row)
	}

	queryResult := &QueryResult{
		Columns: cols,
		Rows:    rows,
	}

	// Add the result to the cache
	r.cache.Add(query, queryResult)

	return queryResult, nil
}

// getSqliteInstance gets the initialized SQLite instance.
//
// You should close the database after using it.
func (r *SQLRunner) getSqliteInstance() (*sql.DB, error) {
	filename, err := initializeThreadSafe(r.schema)
	if err != nil {
		return nil, NewSchemaError(err)
	}

	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", filename))
	if err != nil {
		return nil, fmt.Errorf("open schema database (r/o): %w", err)
	}

	return db, nil
}

// initializeThreadSafe creates a new SQLite database and sets up the schema.
// It is thread safe which ensures that the schema is only initialized once.
func initializeThreadSafe(schema string) (filename string, err error) {
	filenameAny, err, _ := sf.Do(schema, func() (interface{}, error) {
		return initialize(schema)
	})
	if err != nil {
		return "", err
	}

	return filenameAny.(string), nil
}

// initialize creates a new SQLite database and sets up the schema.
func initialize(schema string) (filename string, err error) {
	schemaHash := sha1.Sum([]byte(schema))
	schemaHashStr := hex.EncodeToString(schemaHash[:])
	schemaFilename := filepath.Join(tmpDir, schemaHashStr+".db")

	// If the file already exists, return it
	if _, err := os.Stat(schemaFilename); err == nil {
		return schemaFilename, nil
	}

	drv, err := sql.Open("sqlite", schemaFilename+".tmp")
	if err != nil {
		return "", fmt.Errorf("open sqlite: %w", err)
	}

	if _, err := drv.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		return "", fmt.Errorf("enable foreign keys: %w", err)
	}

	if _, err := drv.Exec(schema); err != nil {
		return "", fmt.Errorf("create schema: %w", err)
	}

	if err := drv.Close(); err != nil {
		return "", fmt.Errorf("close sqlite: %w", err)
	}

	// Rename the file to the final name
	if err := os.Rename(schemaFilename+".tmp", schemaFilename); err != nil {
		return "", fmt.Errorf("persistent schema: %w", err)
	}

	return schemaFilename, nil
}

func parseSqliteDate(d any) (*time.Time, error) {
	if date, ok := d.(*time.Time); ok {
		return date, nil
	}

	dateStr, ok := d.(string)
	if !ok {
		return nil, fmt.Errorf("invalid date type: %T", d)
	}

	t, err := time.Parse("2006-01-02 15:04:05", dateStr)
	if err == nil {
		return &t, nil
	}

	t, err = time.Parse("2006-01-02", dateStr)
	if err == nil {
		return &t, nil
	}

	return &t, nil
}
