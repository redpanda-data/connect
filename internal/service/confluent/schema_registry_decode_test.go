package confluent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/public/x/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runSchemaRegistryServer(t *testing.T, fn func(path string) ([]byte, error)) string {
	t.Helper()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := fn(r.URL.Path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(b) == 0 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Write(b)
	}))
	t.Cleanup(ts.Close)

	return ts.URL
}

func TestSchemaRegistryDecode(t *testing.T) {
	payload3, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{
		Schema: `{
	"namespace": "foo.namespace.com",
	"type":	"record",
	"name": "identity",
	"fields": [
		{ "name": "Name", "type": "string"},
		{ "name": "Address", "type": ["null",{
			"namespace": "my.namespace.com",
			"type":	"record",
			"name": "address",
			"fields": [
				{ "name": "City", "type": "string" },
				{ "name": "State", "type": "string" }
			]
		}],"default":null}
	]
}`,
	})
	require.NoError(t, err)

	returnedSchema3 := false
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/3":
			assert.False(t, returnedSchema3)
			returnedSchema3 = true
			return payload3, nil
		case "/schemas/ids/5":
			return nil, fmt.Errorf("nope")
		}
		return nil, nil
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  "\x00\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			output: `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
		},
		{
			name:   "successful message using cached schema",
			input:  "\x00\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			output: `{"Address":{"my.namespace.com.address":{"City":"foo","State":"bar"}},"Name":"foo"}`,
		},
		{
			name:        "non-empty magic byte",
			input:       "\x06\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			errContains: "version number 6 not supported",
		},
		{
			name:        "non-existing schema",
			input:       "\x00\x00\x00\x00\x04\x06foo\x02\x06foo\x06bar",
			errContains: "schema '4' not found by registry",
		},
		{
			name:        "server fails",
			input:       "\x00\x00\x00\x00\x05\x06foo\x02\x06foo\x06bar",
			errContains: "request failed for schema '5'",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			outMsgs, err := decoder.Process(context.Background(), service.NewMessage([]byte(test.input)))
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				require.Len(t, outMsgs, 1)

				b, err := outMsgs[0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}

		})
	}

	require.NoError(t, decoder.Close(context.Background()))
	decoder.cacheMut.Lock()
	assert.Len(t, decoder.schemas, 0)
	decoder.cacheMut.Unlock()
}

func TestSchemaRegistryDecodeClearExpired(t *testing.T) {
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		return nil, fmt.Errorf("nope")
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, nil, nil)
	require.NoError(t, err)
	require.NoError(t, decoder.Close(context.Background()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	decoder.cacheMut.Lock()
	decoder.schemas = map[int]*cachedSchemaDecoder{
		5:  {lastUsedUnixSeconds: tStale},
		10: {lastUsedUnixSeconds: tNotStale},
		15: {lastUsedUnixSeconds: tNearlyStale},
	}
	decoder.cacheMut.Unlock()

	decoder.clearExpired()

	decoder.cacheMut.Lock()
	assert.Equal(t, map[int]*cachedSchemaDecoder{
		10: {lastUsedUnixSeconds: tNotStale},
		15: {lastUsedUnixSeconds: tNearlyStale},
	}, decoder.schemas)
	decoder.cacheMut.Unlock()
}
