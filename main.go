package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	sqlrunner "github.com/database-playground/sqlrunner/lib"
	"golang.org/x/sync/singleflight"
)

func main() {
	addr := ":8080"
	if os.Getenv("PORT") != "" {
		addr = ":" + os.Getenv("PORT")
	}

	service := &SqlQueryService{}
	http.Handle("POST /query", service)

	slog.Info("Listening", slog.String("addr", addr))
	if err := http.ListenAndServe(addr, nil); err != nil {
		slog.Error("ListenAndServe failed", slog.Any("error", err))
		os.Exit(1)
	}
}

type SqlQueryService struct {
	// fixme: lru
	runners sync.Map
	sfgroup singleflight.Group
}

func (s *SqlQueryService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var req QueryRequest
	if err := decoder.Decode(&req); err != nil {
		respond(w, http.StatusUnprocessableEntity, NewFailedResponse(BadPayloadError{Parent: err}))
		return
	}

	if req.Schema == "" {
		respond(w, http.StatusUnprocessableEntity, NewFailedResponse(NewBadPayloadError("Schema is required")))
		return
	}

	if req.Query == "" {
		respond(w, http.StatusUnprocessableEntity, NewFailedResponse(NewBadPayloadError("Query is required")))
		return
	}

	runner, err := s.findRunner(req.Schema)
	if err != nil {
		respond(w, http.StatusInternalServerError, NewFailedResponse(err))
		return
	}

	queryCtx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()

	result, err := runner.Query(queryCtx, req.Query)
	if err != nil {
		respond(w, http.StatusBadRequest, NewFailedResponse(err))
		return
	}

	respond(w, http.StatusOK, NewSuccessResponse(result))
}

func (s *SqlQueryService) findRunner(schema string) (*sqlrunner.SQLRunner, error) {
	// If we have already prepared a runner for this schema, return it.
	runner, ok := s.runners.Load(schema)
	if ok {
		return runner.(*sqlrunner.SQLRunner), nil
	}

	result, err, _ := s.sfgroup.Do(schema, func() (any, error) {
		newRunner, err := sqlrunner.NewSQLRunner(schema)
		if err != nil {
			return nil, fmt.Errorf("create SQLRunner: %w", err)
		}

		s.runners.Store(schema, newRunner)
		return newRunner, nil
	})
	if err != nil {
		return nil, err
	}

	typedResult := result.(*sqlrunner.SQLRunner)

	s.runners.Store(schema, typedResult)
	return typedResult, err
}

type QueryRequest struct {
	Schema string `json:"schema"`
	Query  string `json:"query"`
}

type QueryResponse struct {
	Success bool `json:"success"`

	Data    *sqlrunner.QueryResult `json:"data,omitempty"`    // success = true
	Message *string                `json:"message,omitempty"` // success = false
	Code    *string                `json:"code,omitempty"`    // success = false
}

type BadPayloadError struct {
	Parent error
}

func NewSuccessResponse(data *sqlrunner.QueryResult) QueryResponse {
	return QueryResponse{
		Success: true,
		Data:    data,
	}
}

func NewFailedResponse(err error) QueryResponse {
	var badPayloadError BadPayloadError
	var schemaError sqlrunner.SchemaError
	var queryError sqlrunner.QueryError

	var code string
	var message string

	if errors.As(err, &badPayloadError) {
		code = "BAD_PAYLOAD"
		message = badPayloadError.Parent.Error()
	} else if errors.As(err, &schemaError) {
		code = "SCHEMA_ERROR"
		message = schemaError.Parent.Error()
	} else if errors.As(err, &queryError) {
		code = "QUERY_ERROR"
		message = queryError.Parent.Error()
	} else {
		code = "INTERNAL_ERROR"
		message = err.Error()
	}

	return QueryResponse{
		Success: false,
		Message: &message,
		Code:    &code,
	}
}

func NewBadPayloadError(message string) BadPayloadError {
	return BadPayloadError{Parent: errors.New(message)}
}

func (e BadPayloadError) Error() string {
	return "bad payload: " + e.Parent.Error()
}

func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
