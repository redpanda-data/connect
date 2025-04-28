// Copyright 2024 Redpanda Data, Inc.
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

package confluent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSchemaRegistryDecoderConfigParse(t *testing.T) {
	configTests := []struct {
		name            string
		config          string
		errContains     string
		expectedBaseURL string
		hambaEnabled    bool
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
		{
			name: "hamba enabled",
			config: `
url: http://example.com/v1
avro: 
  raw_unions: false
  preserve_logical_types: true
`,
			expectedBaseURL: "http://example.com/v1",
			hambaEnabled:    true,
		},
		{
			name: "hamba enabled with removing unions",
			config: `
url: http://example.com/v1
avro:
  preserve_logical_types: true
`,
			expectedBaseURL: "http://example.com/v1",
			hambaEnabled:    true,
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
				assert.Equal(t, test.hambaEnabled, e.cfg.avro.useHamba)
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

func runSchemaRegistryServer(t testing.TB, fn func(path string) ([]byte, error)) string {
	t.Helper()

	var reqMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		b, err := fn(r.URL.EscapedPath())
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

const testProtoSchema = `
syntax = "proto3";
package ksql;

message users {
  int64 registertime = 1;
  string userid = 2;
  string regionid = 3;
  string gender = 4;
}`

const testJSONSchema = `{
	"type": "object",
	"properties": {
		"Name": {"type": "string"},
		"Address": {
			"type": ["object", "null"],
			"properties": {
				"City": {"type": "string"},
				"State": {"type": "string"}
			},
			"required": ["State"]
		},
		"MaybeHobby": {"type": ["string", "null"]}
	},
	"required": ["Name"]
}`

func mustJBytes(t testing.TB, obj any) []byte {
	t.Helper()
	b, err := json.Marshal(obj)
	require.NoError(t, err)
	return b
}

func TestSchemaRegistryDecodeAvro(t *testing.T) {
	returnedSchema3Count := 0
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/3":
			returnedSchema3Count++
			return mustJBytes(t, map[string]any{
				"schema": testSchema,
			}), nil
		case "/schemas/ids/4":
			return mustJBytes(t, map[string]any{
				"schema": testSchemaLogicalTypes,
			}), nil
		case "/schemas/ids/5":
			return nil, fmt.Errorf("nope")
		}
		return nil, nil
	})

	tests := []struct {
		schemaID    int
		name        string
		input       string
		output      string
		hambaOutput string
		errContains string
		mapping     string
	}{
		{
			schemaID: 3,
			name:     "successful message",
			input:    "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
			output:   `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"MaybeHobby":{"string":"dancing"},"Name":"foo"}`,
		},
		{
			schemaID: 3,
			name:     "successful message with null hobby",
			input:    "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
			output:   `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"MaybeHobby":null,"Name":"foo"}`,
		},
		{
			schemaID: 3,
			name:     "successful message no address and null hobby",
			input:    "\x00\x00\x00\x00\x03\x06foo\x00\x00",
			output:   `{"Name":"foo","MaybeHobby":null,"Address": null}`,
		},
		{
			schemaID:    4,
			name:        "successful message with logical types",
			input:       "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
			output:      `{"int_time_millis":{"int.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			hambaOutput: `{"int_time_millis":{"int.time-millis":"0001-01-01T09:47:25Z"},"long_time_micros":{"long.time-micros":"0001-08-22T16:53:20Z"},"long_timestamp_micros":{"long.timestamp-micros":"3939-01-01T00:00:00Z"},"pos_0_33333333":{"bytes.decimal":0.33}}`,
		},
		{
			name:        "non-empty magic byte",
			input:       "\x06\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			errContains: "5 byte header for value is missing or does not have 0 magic byte",
		},
		{
			name:        "non-existing schema",
			input:       "\x00\x00\x00\x00\x06\x06foo\x02\x06foo\x06bar",
			errContains: "schema 6 not found by registry: not found",
		},
		{
			name:        "server fails",
			input:       "\x00\x00\x00\x00\x05\x06foo\x02\x06foo\x06bar",
			errContains: "schema 5 not found by registry: nope",
		},
	}

	cfg := decodingConfig{}
	cfg.avro.rawUnions = false
	goAvroDecoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, 10*time.Minute, service.MockResources())
	require.NoError(t, err)
	cfg.avro.useHamba = true
	hambaDecoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, 10*time.Minute, service.MockResources())
	require.NoError(t, err)

	for _, test := range tests {
		test := test
		fn := func(t *testing.T, useHamba bool) {
			decoder := goAvroDecoder
			if useHamba {
				decoder = hambaDecoder
			}
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
				output := test.output
				if useHamba && test.hambaOutput != "" {
					output = test.hambaOutput
				}
				diff, explanation := jsondiff.Compare(b, []byte(output), &jdopts)
				assert.JSONEq(t, output, string(b))
				assert.Equalf(t, jsondiff.FullMatch.String(), diff.String(), "%s: %s", test.name, explanation)

				v, ok := outMsgs[0].MetaGetMut("schema_id")
				assert.True(t, ok)
				assert.Equal(t, test.schemaID, v)
			}
		}
		t.Run("hamba/"+test.name, func(t *testing.T) { fn(t, true) })
		t.Run("goavro/"+test.name, func(t *testing.T) { fn(t, false) })
	}

	for _, decoder := range []*schemaRegistryDecoder{goAvroDecoder, hambaDecoder} {
		require.NoError(t, decoder.Close(context.Background()))
		decoder.cacheMut.Lock()
		assert.Empty(t, decoder.schemas)
		decoder.cacheMut.Unlock()
	}

	assert.Equal(t, 2, returnedSchema3Count)
}

func TestSchemaRegistryDecodeAvroMapping(t *testing.T) {
	const testAvroDebeziumSchema = `{
  "type": "record",
  "name": "Event",
  "namespace": "com.example",
  "fields": [
    {
      "name": "eventId",
      "type": "string"
    },
    {
      "name": "eventTime",
      "type": {
        "type": "long",
        "connect.version": 1,
        "connect.parameters": {
          "__debezium.source.column.type": "DATETIME"
        },
        "connect.default": 0,
        "connect.name": "io.debezium.time.Timestamp"
      },
      "default": 0
    }
  ]
}`
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/7":
			return mustJBytes(t, map[string]any{
				"schema": testAvroDebeziumSchema,
			}), nil
		}
		return nil, nil
	})
	input := "\x00\x00\x00\x00\x07\n12345\x92\xca߄\x9ae"
	// Without this mapping, the above schema returns plain numbers for hamba
	mapping, err := bloblang.GlobalEnvironment().Clone().Parse(`
map isDebeziumTimestampType {
  root = this.type == "long" && this."connect.name" == "io.debezium.time.Timestamp" && !this.exists("logicalType")
}
map debeziumTimestampToAvroTimestamp {
  let mapped_fields = this.fields.or([]).map_each(item -> item.apply("debeziumTimestampToAvroTimestamp"))
  root = match {
    this.type == "record" => this.assign({"fields": $mapped_fields})
    this.type.type() == "array" => this.assign({"type": this.type.map_each(item -> item.apply("debeziumTimestampToAvroTimestamp"))})
    # Add a logical type so that it's decoded as a timestamp instead of a long.
    this.type.type() == "object" && this.type.apply("isDebeziumTimestampType") => this.merge({"type":{"logicalType": "timestamp-millis"}})
    _ => this
  }
}
root = this.apply("debeziumTimestampToAvroTimestamp")
`)
	require.NoError(t, err)
	cfg := decodingConfig{}
	cfg.avro.mapping = mapping
	goAvroDecoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, 10*time.Minute, service.MockResources())
	require.NoError(t, err)
	cfg.avro.useHamba = true
	hambaDecoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, 10*time.Minute, service.MockResources())
	require.NoError(t, err)

	for _, decoder := range []*schemaRegistryDecoder{goAvroDecoder, hambaDecoder} {
		outBatch, err := decoder.Process(context.Background(), service.NewMessage([]byte(input)))
		require.NoError(t, err)
		require.Len(t, outBatch, 1)
		b, err := outBatch[0].AsBytes()
		require.NoError(t, err)
		if decoder == goAvroDecoder {
			assert.JSONEq(t, `{"eventId":"12345", "eventTime":1.738661425801e+12}`, string(b))
		} else {
			assert.JSONEq(t, `{"eventId":"12345", "eventTime":"2025-02-04T09:30:25.801Z"}`, string(b))
		}
	}

	for _, decoder := range []*schemaRegistryDecoder{goAvroDecoder, hambaDecoder} {
		require.NoError(t, decoder.Close(context.Background()))
		decoder.cacheMut.Lock()
		assert.Empty(t, decoder.schemas)
		decoder.cacheMut.Unlock()
	}
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

	returnedSchema3Count := 0
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/3":
			returnedSchema3Count++
			return payload3, nil
		case "/schemas/ids/4":
			return payload4, nil
		case "/schemas/ids/5":
			return nil, fmt.Errorf("nope")
		}
		return nil, nil
	})

	tests := []struct {
		schemaID    int
		name        string
		input       string
		output      string
		hambaOutput string
		errContains string
	}{
		{
			schemaID: 3,
			name:     "successful message",
			input:    "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
			output:   `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`,
		},
		{
			schemaID: 3,
			name:     "successful message with null hobby",
			input:    "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
			output:   `{"Address":{"City":"foo","State":"bar"},"MaybeHobby":null,"Name":"foo"}`,
		},
		{
			schemaID: 3,
			name:     "successful message no address and null hobby",
			input:    "\x00\x00\x00\x00\x03\x06foo\x00\x00",
			output:   `{"Name":"foo","MaybeHobby":null,"Address": null}`,
		},
		{
			schemaID:    4,
			name:        "successful message with logical types",
			input:       "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
			output:      `{"int_time_millis":35245000,"long_time_micros":20192000000000,"long_timestamp_micros":62135596800000000,"pos_0_33333333":"!"}`,
			hambaOutput: `{"int_time_millis":"0001-01-01T09:47:25Z","long_time_micros":"0001-08-22T16:53:20Z","long_timestamp_micros":"3939-01-01T00:00:00Z","pos_0_33333333":0.33}`,
		},
		{
			name:        "non-empty magic byte",
			input:       "\x06\x00\x00\x00\x03\x06foo\x02\x06foo\x06bar",
			errContains: "5 byte header for value is missing or does not have 0 magic byte",
		},
		{
			name:        "non-existing schema",
			input:       "\x00\x00\x00\x00\x06\x06foo\x02\x06foo\x06bar",
			errContains: "schema 6 not found by registry: not found",
		},
		{
			name:        "server fails",
			input:       "\x00\x00\x00\x00\x05\x06foo\x02\x06foo\x06bar",
			errContains: "schema 5 not found by registry: nope",
		},
	}
	cfg := decodingConfig{}
	cfg.avro.rawUnions = true
	goAvroDecoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, 10*time.Minute, service.MockResources())
	require.NoError(t, err)
	cfg.avro.useHamba = true
	hambaDecoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, 10*time.Minute, service.MockResources())
	require.NoError(t, err)

	for _, test := range tests {
		test := test
		fn := func(t *testing.T, useHamba bool) {
			decoder := goAvroDecoder
			if useHamba {
				decoder = hambaDecoder
			}
			outMsgs, err := decoder.Process(context.Background(), service.NewMessage([]byte(test.input)))
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				require.Len(t, outMsgs, 1)

				b, err := outMsgs[0].AsBytes()
				require.NoError(t, err)

				output := test.output
				if useHamba && test.hambaOutput != "" {
					output = test.hambaOutput
				}
				assert.JSONEq(t, output, string(b))
				jdopts := jsondiff.DefaultJSONOptions()
				diff, explanation := jsondiff.Compare(b, []byte(output), &jdopts)
				assert.Equalf(t, jsondiff.FullMatch.String(), diff.String(), "%s: %s", test.name, explanation)

				v, ok := outMsgs[0].MetaGetMut("schema_id")
				assert.True(t, ok)
				assert.Equal(t, test.schemaID, v)
			}
		}
		t.Run("hamba/"+test.name, func(t *testing.T) { fn(t, true) })
		t.Run("goavro/"+test.name, func(t *testing.T) { fn(t, false) })
	}

	for _, decoder := range []*schemaRegistryDecoder{goAvroDecoder, hambaDecoder} {
		require.NoError(t, decoder.Close(context.Background()))
		decoder.cacheMut.Lock()
		assert.Empty(t, decoder.schemas)
		decoder.cacheMut.Unlock()
	}

	assert.Equal(t, 2, returnedSchema3Count)
}

func TestSchemaRegistryDecodeClearExpired(t *testing.T) {
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		return nil, fmt.Errorf("nope")
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, decodingConfig{}, 10*time.Minute, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, decoder.Close(context.Background()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-5 * time.Minute).Unix()

	decoder.cacheMut.Lock()
	decoder.schemas = map[int]*cachedSchemaDecoder{
		5:  {lastUsedUnixSeconds: tStale},
		10: {lastUsedUnixSeconds: tNotStale},
		15: {lastUsedUnixSeconds: tNearlyStale},
	}
	decoder.cacheMut.Unlock()

	decoder.clearExpired(10 * time.Minute)

	decoder.cacheMut.Lock()
	assert.Equal(t, map[int]*cachedSchemaDecoder{
		10: {lastUsedUnixSeconds: tNotStale},
		15: {lastUsedUnixSeconds: tNearlyStale},
	}, decoder.schemas)
	decoder.cacheMut.Unlock()
}

func TestSchemaRegistryDecodeProtobuf(t *testing.T) {
	payload1, err := json.Marshal(struct {
		Type   string `json:"schemaType"`
		Schema string `json:"schema"`
	}{
		Type:   "PROTOBUF",
		Schema: testProtoSchema,
	})
	require.NoError(t, err)

	returnedSchema1 := false
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/1":
			assert.False(t, returnedSchema1)
			returnedSchema1 = true
			return payload1, nil
		}
		return nil, nil
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, decodingConfig{}, 10*time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  "\x00\x00\x00\x00\x01\x00\b\xa2\xb8\xe2\xec\xaf+\x12\x06User_2\x1a\bRegion_9\"\x05OTHER",
			output: `{"registertime":"1490313321506","userid":"User_2","regionid":"Region_9","gender":"OTHER"}`,
		},
		{
			name:        "not supported message",
			input:       "\x00\x00\x00\x00\x01\x04\x00\x02\b\xa2\xb8\xe2\xec\xaf+\x12\x06User_2\x1a\bRegion_9\"\x05OTHER",
			errContains: `is greater than available message definitions`,
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
				assert.JSONEq(t, test.output, string(b), "%s: %s", test.name)

				v, ok := outMsgs[0].MetaGetMut("schema_id")
				assert.True(t, ok)
				assert.Equal(t, 1, v)
			}
		})
	}

	require.NoError(t, decoder.Close(context.Background()))
	decoder.cacheMut.Lock()
	assert.Empty(t, decoder.schemas)
	decoder.cacheMut.Unlock()
}

func TestSchemaRegistryDecodeJson(t *testing.T) {
	returnedSchema3 := false
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/3":
			assert.False(t, returnedSchema3)
			returnedSchema3 = true
			return mustJBytes(t, map[string]any{
				"schema":     testJSONSchema,
				"schemaType": "JSON",
			}), nil
		case "/schemas/ids/5":
			return nil, fmt.Errorf("nope")
		}
		return nil, nil
	})

	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, decodingConfig{}, 10*time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  "\x00\x00\x00\x00\x03{\"Address\":{\"City\":\"foo\",\"State\":\"bar\"},\"MaybeHobby\":\"dancing\",\"Name\":\"foo\"}",
			output: `{"Address":{"City":"foo","State":"bar"},"MaybeHobby":"dancing","Name":"foo"}`,
		},
		{
			name:   "successful message with null hobby",
			input:  "\x00\x00\x00\x00\x03{\"Address\":{\"City\":\"foo\",\"State\":\"bar\"},\"MaybeHobby\":null,\"Name\":\"foo\"}",
			output: `{"Address":{"City":"foo","State":"bar"},"MaybeHobby":null,"Name":"foo"}`,
		},
		{
			name:   "successful message no address and null hobby",
			input:  "\x00\x00\x00\x00\x03{\"Name\":\"foo\",\"MaybeHobby\":null,\"Address\": null}",
			output: `{"Name":"foo","MaybeHobby":null,"Address": null}`,
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
				v, ok := outMsgs[0].MetaGetMut("schema_id")
				assert.True(t, ok)
				assert.Equal(t, 3, v)
			}
		})
	}

	require.NoError(t, decoder.Close(context.Background()))
	decoder.cacheMut.Lock()
	assert.Empty(t, decoder.schemas)
	decoder.cacheMut.Unlock()
}
