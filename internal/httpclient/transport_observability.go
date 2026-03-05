// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package httpclient

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// --- Logging transport ---

// logLevel represents a structured log level.
type logLevel string

const (
	logLevelTrace logLevel = "TRACE"
	logLevelDebug logLevel = "DEBUG"
	logLevelInfo  logLevel = "INFO"
	logLevelWarn  logLevel = "WARN"
	logLevelError logLevel = "ERROR"
)

func (l logLevel) String() string {
	return string(l)
}

func parseLogLevel(l string) (logLevel, bool) {
	level := logLevel(strings.ToUpper(strings.TrimSpace(l)))
	switch level {
	case logLevelTrace, logLevelDebug, logLevelInfo, logLevelWarn, logLevelError:
		return level, true
	default:
		return "", false
	}
}

// logFunc writes a log message to a logger at a specific level.
type logFunc func(logger *service.Logger, msg string)

func logFuncForLevel(l logLevel) logFunc {
	switch l {
	case logLevelTrace:
		return (*service.Logger).Trace
	case logLevelDebug:
		return (*service.Logger).Debug
	case logLevelInfo:
		return (*service.Logger).Info
	case logLevelWarn:
		return (*service.Logger).Warn
	case logLevelError:
		return (*service.Logger).Error
	default:
		return nil
	}
}

// loggingTransport logs HTTP request/response details at a configured level.
type loggingTransport struct {
	inner         http.RoundTripper
	logger        *service.Logger
	logFn         logFunc
	bodyDumpLimit int // 0 = no body dump
}

type accessLogEntry struct {
	Request   *requestLogEntry  `json:"request,omitempty"`
	Response  *responseLogEntry `json:"response,omitempty"`
	ElapsedMS int64             `json:"elapsed_ms"`
	Error     string            `json:"error,omitempty"`
}

type requestLogEntry struct {
	URL    string            `json:"url"`
	Method string            `json:"method"`
	Header map[string]string `json:"header"`
	Body   any               `json:"body,omitempty"`
}

type responseLogEntry struct {
	StatusCode    int               `json:"status_code"`
	ContentLength int64             `json:"content_length"`
	Header        map[string]string `json:"header"`
	Body          any               `json:"body,omitempty"`
}

var _ http.RoundTripper = (*loggingTransport)(nil)

func newLoggingTransport(inner http.RoundTripper, logger *service.Logger, level string, bodyDumpLimit int) http.RoundTripper {
	l, ok := parseLogLevel(level)
	if !ok || logger == nil {
		return inner
	}

	return &loggingTransport{
		inner:         inner,
		logger:        logger,
		logFn:         logFuncForLevel(l),
		bodyDumpLimit: bodyDumpLimit,
	}
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()

	var entry accessLogEntry
	entry.Request = &requestLogEntry{
		URL:    req.URL.Redacted(),
		Method: req.Method,
		Header: flattenHeaders(req.Header),
	}
	if t.bodyDumpLimit > 0 && req.Body != nil {
		// Read a prefix for logging, then restore the full body for downstream.
		prefix := make([]byte, t.bodyDumpLimit)
		n, _ := io.ReadFull(req.Body, prefix)
		prefix = prefix[:n]
		req.Body = readCloser{
			Reader: io.MultiReader(bytes.NewReader(prefix), req.Body),
			Closer: req.Body,
		}
		entry.Request.Body = unmarshalOrString(prefix)
	}

	resp, err := t.inner.RoundTrip(req)
	entry.ElapsedMS = time.Since(start).Milliseconds()

	if resp != nil {
		entry.Response = &responseLogEntry{
			StatusCode:    resp.StatusCode,
			ContentLength: resp.ContentLength,
			Header:        flattenHeaders(resp.Header),
		}
		if t.bodyDumpLimit > 0 && resp.Body != nil {
			captured, replacement := t.captureResponseBody(resp.Body, t.bodyDumpLimit)
			resp.Body = replacement
			entry.Response.Body = captured
		}
	}
	if err != nil {
		entry.Error = err.Error()
	}

	t.logFn(t.logger.With("access_log", entry), "http request log")

	return resp, err
}

// captureResponseBody captures a prefix of the response body for logging and
// returns a replacement ReadCloser that yields the full original body.
func (*loggingTransport) captureResponseBody(body io.ReadCloser, limit int) (captured any, replacement io.ReadCloser) {
	prefix := make([]byte, limit)
	n, _ := io.ReadFull(body, prefix)
	prefix = prefix[:n]
	captured = unmarshalOrString(prefix)
	replacement = readCloser{
		Reader: io.MultiReader(bytes.NewReader(prefix), body),
		Closer: body,
	}
	return
}

func unmarshalOrString(b []byte) any {
	var v any
	if err := json.Unmarshal(b, &v); err == nil {
		return v
	}
	if len(b) > 0 {
		return string(b)
	}
	return nil
}

// sensitiveHeaders lists header names whose values are redacted in access logs.
var sensitiveHeaders = map[string]struct{}{
	"Authorization":       {},
	"Proxy-Authorization": {},
	"Cookie":              {},
	"Set-Cookie":          {},
	"X-Api-Key":           {},
}

func flattenHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if _, redact := sensitiveHeaders[http.CanonicalHeaderKey(k)]; redact {
			out[k] = "REDACTED"
		} else {
			out[k] = strings.Join(v, " ")
		}
	}
	return out
}

// --- Metrics transport ---

// metricsTransport records Benthos metrics per HTTP attempt.
type metricsTransport struct {
	inner   http.RoundTripper
	metrics *clientMetrics
}

var _ http.RoundTripper = (*metricsTransport)(nil)

func newMetricsTransport(inner http.RoundTripper, metrics *clientMetrics) http.RoundTripper {
	if metrics == nil {
		return inner
	}
	return &metricsTransport{inner: inner, metrics: metrics}
}

func (t *metricsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.metrics.activeRequests.Incr(1)
	start := time.Now()

	resp, err := t.inner.RoundTrip(req)

	elapsed := time.Since(start).Nanoseconds()
	method := strings.ToLower(req.Method)
	t.metrics.activeRequests.Decr(1)

	if err != nil {
		t.metrics.requestDuration.Timing(elapsed, method, "err")
		t.metrics.requestErrors.Incr(1, method)
		return resp, err
	}

	code := strconv.Itoa(resp.StatusCode)
	t.metrics.requestDuration.Timing(elapsed, method, code)
	t.metrics.requestCount.Incr(1, method, code)

	return resp, err
}

// --- Tracing transport ---

// tracingTransport creates an OTEL span for each top-level HTTP request,
// capturing total latency including retries.
type tracingTransport struct {
	inner  http.RoundTripper
	tracer trace.Tracer
}

var _ http.RoundTripper = (*tracingTransport)(nil)

func newTracingTransport(inner http.RoundTripper, tp trace.TracerProvider) http.RoundTripper {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return &tracingTransport{
		inner:  inner,
		tracer: tp.Tracer("github.com/redpanda-data/connect/v4/internal/httpclient"),
	}
}

func (t *tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	spanName := "HTTP " + req.Method
	ctx, span := t.tracer.Start(req.Context(), spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", req.Method),
			attribute.String("url.full", req.URL.Redacted()),
			attribute.String("server.address", req.URL.Hostname()),
		),
	)
	defer span.End()

	if port := req.URL.Port(); port != "" {
		span.SetAttributes(attribute.String("server.port", port))
	}

	req = req.WithContext(ctx)
	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return resp, err
	}

	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 400 {
		span.SetStatus(codes.Error, resp.Status)
	}

	return resp, nil
}
