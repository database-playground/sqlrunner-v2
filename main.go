package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Depado/ginprom"
	sqlrunner "github.com/database-playground/sqlrunner/lib"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"golang.org/x/sync/singleflight"
)

var tracer = otel.Tracer("sqlrunner")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	addr := ":8080"
	if os.Getenv("PORT") != "" {
		addr = ":" + os.Getenv("PORT")
	}

	shutdown, err := setupOTelSDK(ctx)
	if err != nil {
		slog.Error("Failed to setup OpenTelemetry", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			slog.Error("Failed to shutdown OpenTelemetry", slog.Any("error", err))
		}
	}()

	r := gin.Default()
	p := ginprom.New(
		ginprom.Engine(r),
		ginprom.Path("/metrics"),
	)
	r.Use(p.Instrument())
	r.Use(otelgin.Middleware("sqlrunner"))

	p.AddCustomHistogram("query_requests_total", "The total number of SQL query requests.", []string{"code"})
	p.AddCustomHistogram("query_requests_duration_seconds", "The duration of each SQL query request.", []string{"code"})

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	service := &SqlQueryService{
		p:       p,
		sfgroup: singleflight.Group{},
	}
	r.POST("/query", service.Serve)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("ListenAndServe failed", slog.Any("error", err))
			panic(err)
		}
	}()

	<-ctx.Done()
	slog.Info("Received signal to shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Shutdown failed", slog.Any("error", err))
	}
}

type SqlQueryService struct {
	p       *ginprom.Prometheus
	sfgroup singleflight.Group
}

func (s *SqlQueryService) Serve(c *gin.Context) {
	now := time.Now()

	ctx, span := tracer.Start(c.Request.Context(), "SqlQueryService.Serve")
	defer span.End()

	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		span.SetStatus(codes.Error, "bad payload")
		span.RecordError(err)

		s.p.IncrementCounterValue("query_requests_total", []string{"422"})
		s.p.AddCustomHistogramValue("query_requests_duration_seconds", []string{"422"}, time.Since(now).Seconds())
		c.JSON(http.StatusUnprocessableEntity, NewFailedResponse(BadPayloadError{Parent: err}))
		return
	}

	if req.Schema == "" || req.Query == "" {
		span.SetStatus(codes.Error, "bad payload")
		span.RecordError(errors.New("schema and query are required"))

		s.p.IncrementCounterValue("query_requests_total", []string{"422"})
		s.p.AddCustomHistogramValue("query_requests_duration_seconds", []string{"422"}, time.Since(now).Seconds())
		c.JSON(http.StatusUnprocessableEntity, NewFailedResponse(NewBadPayloadError("Schema and Query are required")))
		return
	}

	span.AddEvent("runner.find")
	runner, err := s.findRunner(req.Schema)
	if err != nil {
		span.SetStatus(codes.Error, "runner find error")
		span.RecordError(err)

		s.p.IncrementCounterValue("query_requests_total", []string{"500"})
		s.p.AddCustomHistogramValue("query_requests_duration_seconds", []string{"500"}, time.Since(now).Seconds())
		c.JSON(http.StatusInternalServerError, NewFailedResponse(err))
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	span.AddEvent("runner.query")
	result, err := runner.Query(queryCtx, req.Query)
	if err != nil {
		span.SetStatus(codes.Error, "query error")
		span.RecordError(err)

		s.p.IncrementCounterValue("query_requests_total", []string{"400"})
		s.p.AddCustomHistogramValue("query_requests_duration_seconds", []string{"400"}, time.Since(now).Seconds())
		c.JSON(http.StatusBadRequest, NewFailedResponse(err))
		return
	}

	s.p.IncrementCounterValue("query_requests_total", []string{"200"})
	s.p.AddCustomHistogramValue("query_requests_duration_seconds", []string{"200"}, time.Since(now).Seconds())
	span.SetStatus(codes.Ok, "success")

	c.JSON(http.StatusOK, NewSuccessResponse(result))
}

func (s *SqlQueryService) findRunner(schema string) (*sqlrunner.SQLRunner, error) {
	result, err, _ := s.sfgroup.Do(schema, func() (any, error) {
		newRunner, err := sqlrunner.NewSQLRunner(schema)
		if err != nil {
			return nil, fmt.Errorf("create SQLRunner: %w", err)
		}

		return newRunner, nil
	})
	if err != nil {
		return nil, err
	}

	typedResult := result.(*sqlrunner.SQLRunner)
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
