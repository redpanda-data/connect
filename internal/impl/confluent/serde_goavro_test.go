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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestAvroReferences(t *testing.T) {
	tCtx, done := context.WithTimeout(t.Context(), time.Second*10)
	defer done()

	rootSchema := `[
  "benthos.namespace.com.foo",
  "benthos.namespace.com.bar",
  "benthos.namespace.com.baz"
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

	bazSchema := `{
	"namespace": "benthos.namespace.com",
	"type": "record",
	"name": "baz",
	"fields": [
		{ "name": "Miao", "type": "benthos.namespace.com.foo" }
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
					map[string]any{"name": "benthos.namespace.com.baz", "subject": "baz", "version": 30},
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
		case "/subjects/baz/versions/30", "/schemas/ids/4":
			return mustJBytes(t, map[string]any {
				"id": 4, 
				"version": 30,
				"schema": bazSchema,
				"schemaType": "AVRO",
				"references": []any{
					map[string]any{"name": "benthos.namespace.com.foo", "subject": "foo", "version": 10},
				},
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
		{
			name:   "a baz",
			input:  `{"Miao":{"Woof":"tsssssssuuuuuuuu"}}`,
			output: `{"Miao":{"Woof":"tsssssssuuuuuuuu"}}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
			require.NoError(t, err)

			cfg := decodingConfig{}
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
