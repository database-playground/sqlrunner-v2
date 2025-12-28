package sqlrunner

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("sqlrunner.lib")
