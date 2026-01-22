package telemetry

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func InitTracer(ctx context.Context, serviceName, endpoint string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if strings.TrimSpace(endpoint) == "" {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithTimeout(5 * time.Second),
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		opts = append(opts, otlptracehttp.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(endpoint))
		if !strings.HasPrefix(endpoint, "https://") {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		return func(context.Context) error { return nil }, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func NewTraceID() (trace.TraceID, string, bool) {
	var id trace.TraceID
	if _, err := rand.Read(id[:]); err != nil {
		return trace.TraceID{}, "", false
	}
	return id, hex.EncodeToString(id[:]), true
}

func NewSpanContext(traceID trace.TraceID) (trace.SpanContext, bool) {
	var spanID trace.SpanID
	if _, err := rand.Read(spanID[:]); err != nil {
		return trace.SpanContext{}, false
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	}), true
}

func ContextWithTraceID(ctx context.Context, traceID string) (context.Context, bool) {
	parsed, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ctx, false
	}
	spanCtx, ok := NewSpanContext(parsed)
	if !ok {
		return ctx, false
	}
	spanCtx = trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    spanCtx.TraceID(),
		SpanID:     spanCtx.SpanID(),
		TraceFlags: spanCtx.TraceFlags(),
		Remote:     true,
	})
	return trace.ContextWithSpanContext(ctx, spanCtx), true
}
