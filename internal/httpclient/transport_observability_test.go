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
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func noopLogger() *service.Logger {
	return service.MockResources().Logger()
}

// --- Logging transport ---

func TestLoggingTransportDisabled(t *testing.T) {
	inner := http.DefaultTransport
	rt := newLoggingTransport(inner, nil, "", -1)
	assert.Equal(t, inner, rt)
}

func TestLoggingTransportDisabledEmptyLevel(t *testing.T) {
	inner := http.DefaultTransport
	rt := newLoggingTransport(inner, nil, "  ", -1)
	assert.Equal(t, inner, rt)
}

func TestLoggingTransportResponseBodyStillReadable(t *testing.T) {
	t.Log("Given: a server returning JSON and a logging transport at DEBUG level")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message":"hello"}`))
	}))
	defer srv.Close()
	rt := newLoggingTransport(http.DefaultTransport, noopLogger(), "DEBUG", -1)

	t.Log("When: sending a request and reading the response body")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log("Then: the full response body is still readable by downstream consumers")
	assert.Equal(t, `{"message":"hello"}`, string(body))
}

func TestLoggingTransportBodyDumpLimitZero(t *testing.T) {
	t.Log("Given: a server returning a body and a logging transport with bodyDumpLimit=0")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("big-body"))
	}))
	defer srv.Close()
	rt := newLoggingTransport(http.DefaultTransport, noopLogger(), "DEBUG", 0)

	t.Log("When: sending a request and reading the response body")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log("Then: the response body is fully readable despite no dump")
	assert.Equal(t, "big-body", string(body))
}

func TestLoggingTransportBodyDumpLimitPositive(t *testing.T) {
	t.Log("Given: a server returning 10 bytes and a logging transport with bodyDumpLimit=5")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer srv.Close()
	rt := newLoggingTransport(http.DefaultTransport, noopLogger(), "DEBUG", 5)

	t.Log("When: sending a POST with a body and reading the response")
	reqBody := bytes.NewReader([]byte("abcdefgh"))
	req, err := http.NewRequest(http.MethodPost, srv.URL, reqBody)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	t.Log("Then: the full response body is preserved for downstream consumers")
	assert.Equal(t, "0123456789", string(body))
}

func TestUnmarshalOrString(t *testing.T) {
	v := unmarshalOrString([]byte(`{"a":1}`))
	m, ok := v.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), m["a"])

	v = unmarshalOrString([]byte("plain text"))
	assert.Equal(t, "plain text", v)

	v = unmarshalOrString(nil)
	assert.Nil(t, v)
}

func TestFlattenHeaders(t *testing.T) {
	h := http.Header{
		"Content-Type": {"application/json"},
		"X-Multi":      {"a", "b"},
	}
	flat := flattenHeaders(h)
	assert.Equal(t, "application/json", flat["Content-Type"])
	assert.Equal(t, "a b", flat["X-Multi"])
}

func TestFlattenHeadersRedactsSensitive(t *testing.T) {
	h := http.Header{
		"Authorization":       {"Bearer secret-token"},
		"Proxy-Authorization": {"Basic creds"},
		"Cookie":              {"session=abc123"},
		"Set-Cookie":          {"id=xyz; Path=/"},
		"X-Api-Key":           {"key-12345"},
		"Content-Type":        {"application/json"},
	}
	flat := flattenHeaders(h)
	assert.Equal(t, "REDACTED", flat["Authorization"])
	assert.Equal(t, "REDACTED", flat["Proxy-Authorization"])
	assert.Equal(t, "REDACTED", flat["Cookie"])
	assert.Equal(t, "REDACTED", flat["Set-Cookie"])
	assert.Equal(t, "REDACTED", flat["X-Api-Key"])
	assert.Equal(t, "application/json", flat["Content-Type"])
}

// --- Metrics transport ---

func TestMetricsTransportRecordsDuration(t *testing.T) {
	t.Log("Given: a server and a metrics transport with mock resources")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := service.MockResources()
	metrics := newClientMetrics(res.Metrics(), "test_http")
	rt := newMetricsTransport(http.DefaultTransport, metrics)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()

	t.Log("Then: the transport wraps (not passthrough)")
	assert.IsType(t, &metricsTransport{}, rt)
}

func TestMetricsTransportNilMetricsPassthrough(t *testing.T) {
	inner := http.DefaultTransport
	rt := newMetricsTransport(inner, nil)
	assert.Equal(t, inner, rt)
}

// --- Tracing transport ---

func TestTracingTransportCreatesSpan(t *testing.T) {
	t.Log("Given: a server and a tracing transport with an in-memory exporter")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	rt := newTracingTransport(http.DefaultTransport, tp)

	t.Log("When: sending a GET request")
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()
	tp.ForceFlush(context.Background())

	t.Log("Then: a span is created with HTTP method and status attributes")
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	span := spans[0]
	assert.Equal(t, "HTTP GET", span.Name)
	assertSpanAttr(t, span, "http.request.method", "GET")
	assertSpanAttr(t, span, "http.response.status_code", 200)
}

func TestTracingTransportErrorStatus(t *testing.T) {
	t.Log("Given: a server returning 500 and a tracing transport")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	rt := newTracingTransport(http.DefaultTransport, tp)

	t.Log("When: sending a POST request")
	req, err := http.NewRequest(http.MethodPost, srv.URL, nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()
	tp.ForceFlush(context.Background())

	t.Log("Then: the span has error status")
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status.Code)
}

func TestTracingTransportNetworkError(t *testing.T) {
	t.Log("Given: a tracing transport wrapping a transport that always fails")
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	failing := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	rt := newTracingTransport(failing, tp)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, "http://unreachable.invalid", nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)

	t.Log("Then: the error is propagated and the span records the error")
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)

	tp.ForceFlush(context.Background())
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	assert.Equal(t, codes.Error, spans[0].Status.Code)
	assert.Contains(t, spans[0].Status.Description, "unexpected EOF")
	var hasErrorEvent bool
	for _, ev := range spans[0].Events {
		if ev.Name == "exception" {
			hasErrorEvent = true
		}
	}
	assert.True(t, hasErrorEvent, "expected exception event from RecordError")
}

// --- Logging transport: error path ---

func TestLoggingTransportInnerError(t *testing.T) {
	t.Log("Given: a logging transport wrapping a transport that always fails")
	failing := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	rt := newLoggingTransport(failing, noopLogger(), "DEBUG", 0)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, "http://unreachable.invalid", nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)

	t.Log("Then: the inner error is propagated")
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

// --- Metrics transport: error path ---

func TestMetricsTransportInnerError(t *testing.T) {
	t.Log("Given: a metrics transport wrapping a transport that always fails")
	failing := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})
	res := service.MockResources()
	metrics := newClientMetrics(res.Metrics(), "test_http")
	rt := newMetricsTransport(failing, metrics)

	t.Log("When: sending a request")
	req, err := http.NewRequest(http.MethodGet, "http://unreachable.invalid", nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)

	t.Log("Then: the inner error is propagated without panic")
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.IsType(t, &metricsTransport{}, rt)
}

// roundTripFunc adapts a function to http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func assertSpanAttr(t *testing.T, span tracetest.SpanStub, key string, val any) {
	t.Helper()
	for _, a := range span.Attributes {
		if string(a.Key) == key {
			switch v := val.(type) {
			case string:
				assert.Equal(t, v, a.Value.AsString())
			case int:
				assert.Equal(t, int64(v), a.Value.AsInt64())
			}
			return
		}
	}
	t.Errorf("attribute %q not found in span", key)
}
