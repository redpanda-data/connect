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

package arc

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newTestServer creates an httptest.Server that captures the request body and
// headers. The returned mutex must be held when reading captured values.
func newTestServer(t *testing.T) (*httptest.Server, *[]byte, *http.Header, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	var received []byte
	var headers http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		headers = r.Header.Clone()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received = b
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, &received, &headers, &mu
}

func TestArcOutput_ColumnarSingleMeasurement(t *testing.T) {
	srv, received, headers, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: testdb
measurement: cpu
format: columnar
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"host":"server01","usage":95.2}`)),
		service.NewMessage([]byte(`{"host":"server02","usage":94.5}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "application/msgpack", headers.Get("Content-Type"))
	assert.Equal(t, "testdb", headers.Get("x-arc-database"))
	assert.Empty(t, headers.Get("Content-Encoding"))

	var payload map[string]any
	require.NoError(t, msgpack.Unmarshal(*received, &payload))

	assert.Equal(t, "cpu", payload["m"])
	columns, ok := payload["columns"].(map[string]any)
	require.True(t, ok, "expected columns map")

	hosts, ok := columns["host"].([]any)
	require.True(t, ok, "expected host column")
	assert.Len(t, hosts, 2)
	assert.Equal(t, "server01", hosts[0])
	assert.Equal(t, "server02", hosts[1])

	usages, ok := columns["usage"].([]any)
	require.True(t, ok, "expected usage column")
	assert.Len(t, usages, 2)

	times, ok := columns["time"].([]any)
	require.True(t, ok, "expected time column")
	assert.Len(t, times, 2)

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_ColumnarMultipleMeasurements(t *testing.T) {
	srv, received, _, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: ${!json("type")}
format: columnar
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"type":"cpu","value":10}`)),
		service.NewMessage([]byte(`{"type":"mem","value":80}`)),
		service.NewMessage([]byte(`{"type":"cpu","value":20}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	var payload map[string]any
	require.NoError(t, msgpack.Unmarshal(*received, &payload))

	batchSlice, ok := payload["batch"].([]any)
	require.True(t, ok, "expected batch array")
	assert.Len(t, batchSlice, 2)

	cpuRecord, ok := batchSlice[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "cpu", cpuRecord["m"])
	cpuCols, ok := cpuRecord["columns"].(map[string]any)
	require.True(t, ok)
	cpuValues, ok := cpuCols["value"].([]any)
	require.True(t, ok)
	assert.Len(t, cpuValues, 2)

	memRecord, ok := batchSlice[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "mem", memRecord["m"])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_RowFormat(t *testing.T) {
	srv, received, _, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: cpu
format: row
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"host":"server01","usage":95.2}`)),
		service.NewMessage([]byte(`{"host":"server02","usage":94.5}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	var payload []any
	require.NoError(t, msgpack.Unmarshal(*received, &payload))
	assert.Len(t, payload, 2)

	row0, ok := payload[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "cpu", row0["m"])

	ts, ok := row0["t"].(int64)
	require.True(t, ok, "expected int64 timestamp")
	assert.Greater(t, ts, int64(0))

	fields, ok := row0["fields"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "server01", fields["host"])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_ZstdCompression(t *testing.T) {
	srv, received, headers, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: test
format: columnar
compression: zstd
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"value":42}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "zstd", headers.Get("Content-Encoding"))

	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	decompressed, err := dec.DecodeAll(*received, nil)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, msgpack.Unmarshal(decompressed, &payload))
	assert.Equal(t, "test", payload["m"])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_GzipCompression(t *testing.T) {
	srv, received, headers, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: test
format: columnar
compression: gzip
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"value":42}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "gzip", headers.Get("Content-Encoding"))
	require.GreaterOrEqual(t, len(*received), 2)
	assert.Equal(t, byte(0x1f), (*received)[0])
	assert.Equal(t, byte(0x8b), (*received)[1])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_BearerToken(t *testing.T) {
	srv, _, headers, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
token: my-secret-token
database: default
measurement: test
format: columnar
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"value":1}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "Bearer my-secret-token", headers.Get("Authorization"))

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid measurement name"})
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: test
format: columnar
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"value":1}`)),
	}

	err = out.WriteBatch(t.Context(), batch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid measurement name")

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_EmptyBatch(t *testing.T) {
	var mu sync.Mutex
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		called = true
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: test
format: columnar
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	require.NoError(t, out.WriteBatch(t.Context(), service.MessageBatch{}))

	mu.Lock()
	defer mu.Unlock()
	assert.False(t, called)

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_TimeColumnCollision(t *testing.T) {
	srv, received, _, mu := newTestServer(t)

	conf, err := outputSpec().ParseYAML(`
base_url: `+srv.URL+`
database: default
measurement: test
format: columnar
compression: none
`, nil)
	require.NoError(t, err)

	out, err := newArcOutput(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	// Messages with a "time" field should not cause duplicate column entries
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"time":1633024800000000,"value":10}`)),
		service.NewMessage([]byte(`{"time":1633024801000000,"value":20}`)),
	}

	require.NoError(t, out.WriteBatch(t.Context(), batch))

	mu.Lock()
	defer mu.Unlock()

	var payload map[string]any
	require.NoError(t, msgpack.Unmarshal(*received, &payload))

	columns, ok := payload["columns"].(map[string]any)
	require.True(t, ok)

	times, ok := columns["time"].([]any)
	require.True(t, ok)
	values, ok := columns["value"].([]any)
	require.True(t, ok)

	// time and value columns must have the same length
	assert.Len(t, times, 2, "time column should have exactly 2 entries, not duplicated")
	assert.Len(t, values, 2)

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_InvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config string
		errMsg string
	}{
		{
			name: "token with newline",
			config: `
base_url: http://localhost:8000
token: "bad\ntoken"
database: default
measurement: test
`,
			errMsg: "token contains invalid characters",
		},
		{
			name: "database with carriage return",
			config: `
base_url: http://localhost:8000
database: "bad\rdb"
measurement: test
`,
			errMsg: "database name contains invalid characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := outputSpec().ParseYAML(tt.config, nil)
			require.NoError(t, err)

			_, err = newArcOutput(conf, service.MockResources())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestConvertTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		unit     string
		expected int64
	}{
		{"microseconds passthrough", 1633024800000000, "us", 1633024800000000},
		{"milliseconds to us", 1633024800000, "ms", 1633024800000000},
		{"seconds to us", 1633024800, "s", 1633024800000000},
		{"nanoseconds to us", 1633024800000000000, "ns", 1633024800000000},
		{"auto detects seconds", 1633024800, "auto", 1633024800000000},
		{"auto detects milliseconds", 1633024800000, "auto", 1633024800000000},
		{"auto detects microseconds", 1633024800000000, "auto", 1633024800000000},
		{"auto detects nanoseconds", 1633024800000000000, "auto", 1633024800000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertTimestamp(tt.value, tt.unit)
			assert.Equal(t, tt.expected, result)
		})
	}
}
