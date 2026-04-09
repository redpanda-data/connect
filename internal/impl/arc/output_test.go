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

func TestArcOutput_ColumnarSingleMeasurement(t *testing.T) {
	var mu sync.Mutex
	var received []byte
	var headers http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		headers = r.Header.Clone()
		var err error
		received, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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
	require.NoError(t, msgpack.Unmarshal(received, &payload))

	assert.Equal(t, "cpu", payload["m"])
	columns, ok := payload["columns"].(map[string]any)
	require.True(t, ok)

	hosts := columns["host"].([]any)
	assert.Len(t, hosts, 2)
	assert.Equal(t, "server01", hosts[0])
	assert.Equal(t, "server02", hosts[1])

	usages := columns["usage"].([]any)
	assert.Len(t, usages, 2)

	times := columns["time"].([]any)
	assert.Len(t, times, 2)

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_ColumnarMultipleMeasurements(t *testing.T) {
	var received []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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

	var payload map[string]any
	require.NoError(t, msgpack.Unmarshal(received, &payload))

	batchSlice, ok := payload["batch"].([]any)
	require.True(t, ok)
	assert.Len(t, batchSlice, 2)

	cpuRecord := batchSlice[0].(map[string]any)
	assert.Equal(t, "cpu", cpuRecord["m"])
	cpuCols := cpuRecord["columns"].(map[string]any)
	assert.Len(t, cpuCols["value"].([]any), 2)

	memRecord := batchSlice[1].(map[string]any)
	assert.Equal(t, "mem", memRecord["m"])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_RowFormat(t *testing.T) {
	var received []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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

	var payload []any
	require.NoError(t, msgpack.Unmarshal(received, &payload))
	assert.Len(t, payload, 2)

	row0 := payload[0].(map[string]any)
	assert.Equal(t, "cpu", row0["m"])
	assert.NotNil(t, row0["t"])
	fields := row0["fields"].(map[string]any)
	assert.Equal(t, "server01", fields["host"])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_ZstdCompression(t *testing.T) {
	var received []byte
	var contentEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentEncoding = r.Header.Get("Content-Encoding")
		var err error
		received, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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

	assert.Equal(t, "zstd", contentEncoding)

	// Verify it's valid zstd
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer dec.Close()
	decompressed, err := dec.DecodeAll(received, nil)
	require.NoError(t, err)

	var payload map[string]any
	require.NoError(t, msgpack.Unmarshal(decompressed, &payload))
	assert.Equal(t, "test", payload["m"])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_GzipCompression(t *testing.T) {
	var received []byte
	var contentEncoding string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentEncoding = r.Header.Get("Content-Encoding")
		var err error
		received, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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

	assert.Equal(t, "gzip", contentEncoding)

	// Verify it's valid gzip by checking magic bytes
	require.True(t, len(received) >= 2)
	assert.Equal(t, byte(0x1f), received[0])
	assert.Equal(t, byte(0x8b), received[1])

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_BearerToken(t *testing.T) {
	var authHeader string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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
	assert.Equal(t, "Bearer my-secret-token", authHeader)

	require.NoError(t, out.Close(t.Context()))
}

func TestArcOutput_ErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid measurement name"})
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conf, err := outputSpec().ParseYAML(`
url: `+srv.URL+`
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
	assert.False(t, called)

	require.NoError(t, out.Close(t.Context()))
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
