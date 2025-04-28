// Copyright 2025 Redpanda Data, Inc.
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
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestHambaAvroReferences(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	rootSchema := `[
  "benthos.namespace.com.foo",
  "benthos.namespace.com.bar"
]`

	fooSchema := `{
	"namespace": "benthos.namespace.com",
	"type": "record",
	"name": "foo",
	"fields": [
		{ "name": "Woof", "type": "string"}
	]
}`

	barSchema := `{
	"namespace": "benthos.namespace.com",
	"type": "record",
	"name": "bar",
	"fields": [
		{ "name": "Moo", "type": "string"}
	]
}`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/root/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     rootSchema,
				"schemaType": "AVRO",
				"references": []any{
					map[string]any{"name": "benthos.namespace.com.foo", "subject": "foo", "version": 10},
					map[string]any{"name": "benthos.namespace.com.bar", "subject": "bar", "version": 20},
				},
			}), nil
		case "/subjects/foo/versions/10", "/schemas/ids/2":
			return mustJBytes(t, map[string]any{
				"id": 2, "version": 10, "schemaType": "AVRO",
				"schema": fooSchema,
			}), nil
		case "/subjects/bar/versions/20", "/schemas/ids/3":
			return mustJBytes(t, map[string]any{
				"id": 3, "version": 20, "schemaType": "AVRO",
				"schema": barSchema,
			}), nil
		}
		return nil, nil
	})

	subj, err := service.NewInterpolatedString("root")
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains []string
	}{
		{
			name:   "a foo",
			input:  `{"Woof":"hhnnnnnnroooo"}`,
			output: `{"Woof":"hhnnnnnnroooo"}`,
		},
		{
			name:   "a bar",
			input:  `{"Moo":"mmuuuuuueew"}`,
			output: `{"Moo":"mmuuuuuueew"}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
			require.NoError(t, err)

			cfg := decodingConfig{}
			cfg.avro.useHamba = true
			cfg.avro.rawUnions = true
			decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, schemaStaleAfter, service.MockResources())
			require.NoError(t, err)

			t.Cleanup(func() {
				_ = encoder.Close(tCtx)
				_ = decoder.Close(tCtx)
			})

			inMsg := service.NewMessage([]byte(test.input))

			encodedMsgs, err := encoder.ProcessBatch(tCtx, service.MessageBatch{inMsg})
			require.NoError(t, err)
			require.Len(t, encodedMsgs, 1)
			require.Len(t, encodedMsgs[0], 1)

			encodedMsg := encodedMsgs[0][0]

			if len(test.errContains) > 0 {
				require.Error(t, encodedMsg.GetError())
				for _, errStr := range test.errContains {
					assert.Contains(t, encodedMsg.GetError().Error(), errStr)
				}
				return
			}

			b, err := encodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, encodedMsg.GetError())
			require.NotEqual(t, test.input, string(b))

			var n any
			require.Error(t, json.Unmarshal(b, &n), "message contents should no longer be valid JSON")

			decodedMsgs, err := decoder.Process(tCtx, encodedMsg)
			require.NoError(t, err)
			require.Len(t, decodedMsgs, 1)

			decodedMsg := decodedMsgs[0]

			b, err = decodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, decodedMsg.GetError())
			require.JSONEq(t, test.output, string(b))
		})
	}
}

func TestHambaDecodeAvroUnions(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	rootSchema := `{
  "type": "record",
  "name": "TestRecord",
  "namespace": "com.example.test",
  "fields": [
    { "name": "booleanField", "type": "boolean" },
    { "name": "intField", "type": "int" },
    { "name": "longField", "type": "long" },
    { "name": "floatField", "type": "float" },
    { "name": "doubleField", "type": "double" },
    { "name": "bytesField", "type": "bytes" },
    { "name": "stringField", "type": "string" },
    { 
      "name": "arrayField", 
      "type": { "type": "array", "items": "int" } 
    },
    { 
      "name": "mapField", 
      "type": { "type": "map", "values": "string" } 
    },
    { 
      "name": "unionField", 
      "type": ["null", "string", "int"] 
    },
    { 
      "name": "fixedField", 
      "type": { "type": "fixed", "name": "FixedType", "size": 16 } 
    },
    { 
      "name": "enumField", 
      "type": { "type": "enum", "name": "EnumType", "symbols": ["A", "B", "C"] } 
    },
    { 
      "name": "recordField", 
      "type": { 
        "type": "record", 
        "name": "NestedRecord", 
        "fields": [
          { "name": "nestedIntField", "type": "int" },
          { "name": "nestedStringField", "type": "string" }
        ]
      } 
    },
    { 
      "name": "dateField", 
      "type": { "type": "int", "logicalType": "date" } 
    },
    { 
      "name": "timestampMillisField", 
      "type": { "type": "long", "logicalType": "timestamp-millis" } 
    },
    { 
      "name": "timestampMicrosField", 
      "type": { "type": "long", "logicalType": "timestamp-micros" } 
    },
    { 
      "name": "timeMillisField", 
      "type": { "type": "int", "logicalType": "time-millis" } 
    },
    { 
      "name": "timeMicrosField", 
      "type": { "type": "long", "logicalType": "time-micros" } 
    },
    { 
      "name": "decimalBytesField", 
      "type": { 
        "type": "bytes", 
        "logicalType": "decimal", 
        "precision": 10, 
        "scale": 2 
      } 
    },
    { 
      "name": "decimalFixedField", 
      "type": { 
        "type": "fixed", 
        "name": "DecimalFixed", 
        "size": 8, 
        "logicalType": "decimal", 
        "precision": 16, 
        "scale": 4 
      } 
    },
    { 
      "name": "uuidField", 
      "type": { "type": "string", "logicalType": "uuid" } 
    }
  ]
}`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/root/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     rootSchema,
				"schemaType": "AVRO",
			}), nil
		}
		return nil, nil
	})

	tests := []struct {
		name         string
		input        string
		output       string
		unnestUnions bool
	}{
		{
			name:         "all types nested union",
			input:        "AZ340UnQtM3BiI/ihdEBfH9KP+XDphvske0/HlUnSXt2V81gmbTQunPejR5yaHhxZHRsd2VscGRqdHgatLHK9QuZ0cXsD+eJsMYNv8a+2Qzc986nBuz76MAG97W//AmGzbjxDuGnvP4NptCcvQvqveF2n/uQ1gbf9eJMABQCcBZrY2hreHN6d29sawJhGm9hcnhscGZteG5rcWUCch5odmpycWR4cGliZGhzaG0CcxpiZ2lzcmR6eWFtcnlpAnQeanN2Y252bWpsbWJzaGlrAnYeZHlrdml1b2l3Z2N1c2RhAmgUcnl2aW96aWxqaQJ4GnZkaG5icnRkbXRxbWQCaRJkY29lYm1lY3MCbB5maHl6eWV1YnBiaHh5cmoABMHn5qsNBrOnYNqXtmbEr69wkjaZ1ALbv/9dGGJsbmxnbnJvYmx1Y4AqstKWqoEy4qfwyModq8jmuAKgqpm+8/zjpswBCv76A5LP83wuR7QwOQAUcmNwdmp5eG5ueA==",
			unnestUnions: false,
			output:       `{"arrayField":[1599687770,-2127082573,-1818624628,-1704448416,846847470,873275126,-1338502524,1998000963,-1877445105,1540592659,124530549,-895622864,-80502128],"booleanField":true,"bytesField":"VSdJe3ZXzWCZtNC6c96N","dateField":"1977-05-12T00:00:00Z","decimalBytesField":-43953964.01,"decimalFixedField":-90179493988032.6912,"doubleField":0.9240627803866316,"enumField":"B","fixedField":[6,179,167,96,218,151,182,102,196,175,175,112,146,54,153,212],"floatField":0.79100776,"intField":-77217295,"longField":7531641714966637864,"mapField":{"a":"oarxlpfmxnkqe","h":"ryviozilji","i":"dcoebmecs","l":"fhyzyeubpbhxyrj","p":"kchkxszwolk","r":"hvjrqdxpibdhshm","s":"bgisrdzyamryi","t":"jsvcnvmjlmbshik","v":"dykviuoiwgcusda","x":"vdhnbrtdmtqmd"},"recordField":{"nestedIntField":-98562030,"nestedStringField":"blnlgnrobluc"},"stringField":"rhxqdtlwelpdjtx","timeMicrosField":"0018-02-06T10:11:19.879705216Z","timeMillisField":"0000-12-28T04:53:24.074Z","timestampMicrosField":"1970-01-06T21:10:24.735729Z","timestampMillisField":"1997-03-24T02:51:42.617Z","unionField":{"int":-1790761441},"uuidField":"rcpvjyxnnx"}`,
		},
		{
			name:         "all types raw union",
			input:        "AZ340UnQtM3BiI/ihdEBfH9KP+XDphvske0/HlUnSXt2V81gmbTQunPejR5yaHhxZHRsd2VscGRqdHgatLHK9QuZ0cXsD+eJsMYNv8a+2Qzc986nBuz76MAG97W//AmGzbjxDuGnvP4NptCcvQvqveF2n/uQ1gbf9eJMABQCcBZrY2hreHN6d29sawJhGm9hcnhscGZteG5rcWUCch5odmpycWR4cGliZGhzaG0CcxpiZ2lzcmR6eWFtcnlpAnQeanN2Y252bWpsbWJzaGlrAnYeZHlrdml1b2l3Z2N1c2RhAmgUcnl2aW96aWxqaQJ4GnZkaG5icnRkbXRxbWQCaRJkY29lYm1lY3MCbB5maHl6eWV1YnBiaHh5cmoABMHn5qsNBrOnYNqXtmbEr69wkjaZ1ALbv/9dGGJsbmxnbnJvYmx1Y4AqstKWqoEy4qfwyModq8jmuAKgqpm+8/zjpswBCv76A5LP83wuR7QwOQAUcmNwdmp5eG5ueA==",
			unnestUnions: true,
			output:       `{"arrayField":[1599687770,-2127082573,-1818624628,-1704448416,846847470,873275126,-1338502524,1998000963,-1877445105,1540592659,124530549,-895622864,-80502128],"booleanField":true,"bytesField":"VSdJe3ZXzWCZtNC6c96N","dateField":"1977-05-12T00:00:00Z","decimalBytesField":-43953964.01,"decimalFixedField":-90179493988032.6912,"doubleField":0.9240627803866316,"enumField":"B","fixedField":[6,179,167,96,218,151,182,102,196,175,175,112,146,54,153,212],"floatField":0.79100776,"intField":-77217295,"longField":7531641714966637864,"mapField":{"a":"oarxlpfmxnkqe","h":"ryviozilji","i":"dcoebmecs","l":"fhyzyeubpbhxyrj","p":"kchkxszwolk","r":"hvjrqdxpibdhshm","s":"bgisrdzyamryi","t":"jsvcnvmjlmbshik","v":"dykviuoiwgcusda","x":"vdhnbrtdmtqmd"},"recordField":{"nestedIntField":-98562030,"nestedStringField":"blnlgnrobluc"},"stringField":"rhxqdtlwelpdjtx","timeMicrosField":"0018-02-06T10:11:19.879705216Z","timeMillisField":"0000-12-28T04:53:24.074Z","timestampMicrosField":"1970-01-06T21:10:24.735729Z","timestampMillisField":"1997-03-24T02:51:42.617Z","unionField":-1790761441,"uuidField":"rcpvjyxnnx"}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			cfg := decodingConfig{}
			cfg.avro.useHamba = true
			cfg.avro.rawUnions = test.unnestUnions
			decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, schemaStaleAfter, service.MockResources())
			require.NoError(t, err)

			t.Cleanup(func() {
				_ = decoder.Close(tCtx)
			})

			b, err := base64.StdEncoding.DecodeString(test.input)
			require.NoError(t, err)
			// Prepend magic bytes
			b = append([]byte{0, 0, 0, 0, 1}, b...)
			inMsg := service.NewMessage(b)

			decodedMsgs, err := decoder.Process(tCtx, inMsg)
			require.NoError(t, err)
			require.Len(t, decodedMsgs, 1)

			decodedMsg := decodedMsgs[0]

			b, err = decodedMsg.AsBytes()
			require.NoError(t, err)

			require.NoError(t, decodedMsg.GetError())
			require.JSONEq(t, test.output, string(b))
		})
	}
}
