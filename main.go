package main

import (
	"context"
	"encoding/json"
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
		respond(w, http.StatusBadRequest, NewFailedResponse(err.Error()))
		return
	}

	if req.Schema == "" {
		respond(w, http.StatusUnprocessableEntity, NewFailedResponse("Schema is required"))
		return
	}

	if req.Query == "" {
		respond(w, http.StatusUnprocessableEntity, NewFailedResponse("Query is required"))
		return
	}

	runner, err := s.findRunner(req.Schema)
	if err != nil {
		respond(w, http.StatusInternalServerError, NewFailedResponse(err.Error()))
		return
	}

	queryCtx, cancel := context.WithTimeout(r.Context(), time.Minute)
	defer cancel()

	result, err := runner.Query(queryCtx, req.Query)
	if err != nil {
		respond(w, http.StatusBadRequest, NewFailedResponse(err.Error()))
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
}

func NewSuccessResponse(data *sqlrunner.QueryResult) QueryResponse {
	return QueryResponse{
		Success: true,
		Data:    data,
	}
}

func NewFailedResponse(message string) QueryResponse {
	return QueryResponse{
		Success: false,
		Message: &message,
	}
}

func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
