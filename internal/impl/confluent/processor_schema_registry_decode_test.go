package confluent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSchemaRegistryDecoderConfigParse(t *testing.T) {
	configTests := []struct {
		name            string
		config          string
		errContains     string
		expectedBaseURL string
	}{
		{
			name: "bad url",
			config: `
url: huh#%#@$u*not////::example.com
`,
			errContains: `failed to parse url`,
		},
		{
			name: "url with base path",
			config: `
url: http://example.com/v1
`,
			expectedBaseURL: "http://example.com/v1",
		},
		{
			name: "url with basic auth",
			config: `
url: http://example.com/v1
basic_auth:
  enabled: true
  username: user
  password: pass
`,
			expectedBaseURL: "http://example.com/v1",
		},
	}

	spec := schemaRegistryDecoderConfig()
	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.config, env)
			require.NoError(t, err)

			e, err := newSchemaRegistryDecoderFromConfig(conf, service.MockResources())
			if e != nil {
				assert.Equal(t, test.expectedBaseURL, e.schemaRegistryBaseURL.String())
			}

			if err == nil {
				_ = e.Close(context.Background())
			}
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

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
		_, _ = w.Write(b)
	}))
	t.Cleanup(ts.Close)

	return ts.URL
}

const testSchema = `{
	"namespace": "foo.namespace.com",
	"type": "record",
	"name": "identity",
	"fields": [
		{ "name": "Name", "type": "string"},
		{ "name": "Address", "type": ["null",{
			"namespace": "my.namespace.com",
			"type":	"record",
			"name": "address",
			"fields": [
				{ "name": "City", "type": ["null", "string"], "default": null },
				{ "name": "State", "type": "string" }
			]
		}],"default":null},
		{"name": "MaybeHobby", "type": ["null","string"] }
	]
}`

const testSchemaLogicalTypes = `{
	"type": "record",
	"name": "LogicalTypes",
	"fields": [
		{
			"default": null,
			"name": "int_time_millis",
			"type": [
				"null",
				{
					"type": "int",
					"logicalType": "time-millis"
				}
			]
		},
		{
			"default": null,
			"name": "long_time_micros",
			"type": [
				"null",
				{
					"type": "long",
					"logicalType": "time-micros"
				}
			]
		},
		{
			"default": null,
			"name": "long_timestamp_micros",
			"type": [
				"null",
				{
					"type": "long",
					"logicalType": "timestamp-micros"
				}
			]
		},
		{
			"default": null,
			"name": "pos_0_33333333",
			"type": [
				"null",
				{
					"logicalType": "decimal",
					"precision": 16,
					"scale": 2,
					"type": "bytes"
				}
			]
		}
	]
}`

func TestSchemaRegistryDecodeAvro(t *testing.T) {
	payload3, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{
		Schema: testSchema,
	})
	require.NoError(t, err)

	payload4, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{
		Schema: testSchemaLogicalTypes,
	})
	require.NoError(t, err)

	returnedSchema3 := false
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/3":
			assert.False(t, returnedSchema3)
			returnedSchema3 = true
			return payload3, nil
		case "/schemas/ids/4":
			return payload4, nil
		case "/schemas/ids/5":
			return nil, fmt.Errorf("nope")
		}
		return nil, nil
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, false, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
			output: `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"MaybeHobby":{"string":"dancing"},"Name":"foo"}`,
		},
		{
			name:   "successful message with null hobby",
			input:  "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
			output: `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"MaybeHobby":null,"Name":"foo"}`,
		},
		{
			name:   "successful message no address and null hobby",
			input:  "\x00\x00\x00\x00\x03\x06foo\x00\x00",
			output: `{"Name":"foo","MaybeHobby":null,"Address": null}`,
		},
		{
			name:   "successful message with logical types",
			input:  "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
			output: `{"int_time_millis":{"int.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
		},
		{
			name:        "non-empty magic byte",
			input:       "\x06\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			errContains: "version number 6 not supported",
		},
		{
			name:        "non-existing schema",
			input:       "\x00\x00\x00\x00\x06\x06foo\x02\x06foo\x06bar",
			errContains: "schema '6' not found by registry",
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

				jdopts := jsondiff.DefaultJSONOptions()
				diff, explanation := jsondiff.Compare(b, []byte(test.output), &jdopts)
				assert.Equalf(t, jsondiff.FullMatch.String(), diff.String(), "%s: %s", test.name, explanation)
			}
		})
	}

	require.NoError(t, decoder.Close(context.Background()))
	decoder.cacheMut.Lock()
	assert.Len(t, decoder.schemas, 0)
	decoder.cacheMut.Unlock()
}

func TestSchemaRegistryDecodeAvroRawJson(t *testing.T) {
	payload3, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{
		Schema: testSchema,
	})
	require.NoError(t, err)

	payload4, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{
		Schema: testSchemaLogicalTypes,
	})
	require.NoError(t, err)

	returnedSchema3 := false
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/3":
			assert.False(t, returnedSchema3)
			returnedSchema3 = true
			return payload3, nil
		case "/schemas/ids/4":
			return payload4, nil
		case "/schemas/ids/5":
			return nil, fmt.Errorf("nope")
		}
		return nil, nil
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, true, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
			output: `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`,
		},
		{
			name:   "successful message with null hobby",
			input:  "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
			output: `{"Address":{"City":"foo","State":"bar"},"MaybeHobby":null,"Name":"foo"}`,
		},
		{
			name:   "successful message no address and null hobby",
			input:  "\x00\x00\x00\x00\x03\x06foo\x00\x00",
			output: `{"Name":"foo","MaybeHobby":null,"Address": null}`,
		},
		{
			name:   "successful message with logical types",
			input:  "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
			output: `{"int_time_millis":35245000,"long_time_micros":20192000000000,"long_timestamp_micros":62135596800000000,"pos_0_33333333":"!"}`,
		},
		{
			name:        "non-empty magic byte",
			input:       "\x06\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			errContains: "version number 6 not supported",
		},
		{
			name:        "non-existing schema",
			input:       "\x00\x00\x00\x00\x06\x06foo\x02\x06foo\x06bar",
			errContains: "schema '6' not found by registry",
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

				jdopts := jsondiff.DefaultJSONOptions()
				diff, explanation := jsondiff.Compare(b, []byte(test.output), &jdopts)
				assert.Equalf(t, jsondiff.FullMatch.String(), diff.String(), "%s: %s", test.name, explanation)
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

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, false, service.MockResources())
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
