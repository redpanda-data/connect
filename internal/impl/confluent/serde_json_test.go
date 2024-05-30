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

func TestResolveJsonSchema(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	rootSchema := `{
	"type": "object",
	"oneOf": [
		{"$ref": "foo.schema.json"},
		{"$ref": "bar.schema.json"}
	]
}`

	fooSchema := `{
	"$id": "foo.schema.json",
	"type": "object",
	"properties": {
		"Woof": { "type": "string" }
	},
	"required": ["Woof"]
}`

	barSchema := `{
	"$id": "bar.schema.json",
	"type": "object",
	"properties": {
		"Moo": { "type": "string" }
	},
	"required": ["Moo"]
}`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/root/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     rootSchema,
				"schemaType": "JSON",
				"references": []any{
					map[string]any{"name": "foo.schema.json", "subject": "foo", "version": 10},
					map[string]any{"name": "bar.schema.json", "subject": "bar", "version": 20},
				},
			}), nil
		case "/subjects/foo/versions/10", "/schemas/ids/2":
			return mustJBytes(t, map[string]any{
				"id": 2, "version": 10, "schemaType": "JSON",
				"schema": fooSchema,
			}), nil
		case "/subjects/bar/versions/20", "/schemas/ids/3":
			return mustJBytes(t, map[string]any{
				"id": 3, "version": 20, "schemaType": "JSON",
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

			decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, true, service.MockResources())
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
