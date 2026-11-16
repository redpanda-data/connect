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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const testBadURL = "http://u:S3cretPass@bad host:8080"

func TestNewBaseTransportProxyURLRedacted(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.ProxyURL = testBadURL

	_, err := newBaseTransport(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid proxy_url")
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestNewConfigFromParsedBaseURLRedacted(t *testing.T) {
	pConf := parseTestYAML(t, `base_url: "`+testBadURL+`"`)

	_, err := NewConfigFromParsed(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base_url is not a valid URL")
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestTracingTransportRedactsURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(t.Context()) }()
	rt := newTracingTransport(http.DefaultTransport, tp)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	u.User = url.UserPassword("u", "S3cretPass")
	u.RawQuery = "token=T0kenValue&x=1"
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	resp.Body.Close()
	tp.ForceFlush(t.Context())

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	var full string
	for _, a := range spans[0].Attributes {
		if a.Key == "url.full" {
			full = a.Value.AsString()
		}
	}
	require.NotEmpty(t, full)
	assert.NotContains(t, full, "S3cretPass")
	assert.NotContains(t, full, "T0kenValue")
	assert.Contains(t, full, "x=xxxxx", "every query value is redacted")
}
