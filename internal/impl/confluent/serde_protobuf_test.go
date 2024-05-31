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

func TestProtobufEncodeMultipleMessages(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	thingsSchema := `
syntax = "proto3";
package things;

message foo {
  float a = 1;
  string b = 2;
}

message bar {
  string b = 1;
}
`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	subj, err := service.NewInterpolatedString("${! @subject }")
	require.NoError(t, err)

	tests := []struct {
		name        string
		subject     string
		input       string
		output      string
		errContains []string
	}{
		{
			name:    "things foo exact match",
			subject: "things",
			input:   `{"a":123,    "b":"hello world"}`,
			output:  `{"a":123,"b":"hello world"}`,
		},
		{
			name:    "things bar exact match",
			subject: "things",
			input:   `{"b":"hello world"}`,
			output:  `{"b":"hello world"}`,
		},
		{
			name:    "things neither match",
			subject: "things",
			input:   `{"a":123,    "b":"hello world", "c":"what"}`,
			errContains: []string{
				"unknown field \"c\"",
				"unknown field \"a\"",
			},
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
			inMsg.MetaSetMut("subject", test.subject)

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

func TestProtobufReferences(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	thingsSchema := `
syntax = "proto3";
package things;

import "stuffs/thething.proto";

message foo {
  float a = 1;
  string b = 2;
  stuffs.bar c = 3;
}
`

	stuffsSchema := `
syntax = "proto3";
package stuffs;

message bar {
  string d = 1;
}
`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
				"references": []any{
					map[string]any{
						"name":    "stuffs/thething.proto",
						"subject": "stuffs/thething.proto",
						"version": 10,
					},
				},
			}), nil
		case "/subjects/stuffs%2Fthething.proto/versions/10", "/schemas/ids/2":
			return mustJBytes(t, map[string]any{
				"id":         2,
				"version":    10,
				"schema":     stuffsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	subj, err := service.NewInterpolatedString("things")
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains []string
	}{
		{
			name:   "things foo without bar",
			input:  `{"a":123,    "b":"hello world"}`,
			output: `{"a":123,"b":"hello world"}`,
		},
		{
			name:   "things foo with bar",
			input:  `{"a":123,    "b":"hello world", "c":{"d":"and this"}}`,
			output: `{"a":123, "b":"hello world", "c":{"d":"and this"}}`,
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

func runEncoderAgainstInputsMultiple(t testing.TB, urlStr, subject string, inputs [][]byte) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	subj, err := service.NewInterpolatedString(subject)
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = encoder.Close(tCtx)
	})

	n := 10
	if b, ok := t.(*testing.B); ok {
		b.ReportAllocs()
		b.ResetTimer()
		n = b.N
	}

	for i := 0; i < n; i++ {
		inMsg := service.NewMessage(inputs[i%len(inputs)])
		encodedMsgs, err := encoder.ProcessBatch(tCtx, service.MessageBatch{inMsg})
		require.NoError(t, err)
		require.Len(t, encodedMsgs, 1)
		require.Len(t, encodedMsgs[0], 1)
		require.NoError(t, encodedMsgs[0][0].GetError())
	}
}

func TestProtobufEncodeMultipleMessagesCaching(t *testing.T) {
	thingsSchema := `
syntax = "proto3";
package things;

message foo {
  float a = 1;
  string b = 2;
}

message bar {
  float c = 1;
  string d = 2;
}
`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	t.Run("consistent message", func(t *testing.T) {
		runEncoderAgainstInputsMultiple(t, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
		})
	})

	t.Run("alternating messages", func(t *testing.T) {
		runEncoderAgainstInputsMultiple(t, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
			[]byte(`{"c":2.34,"d":"bar"}`),
		})
	})
}

func BenchmarkProtobufEncodeMultipleMessagesCaching(b *testing.B) {
	thingsSchema := `
syntax = "proto3";
package things;

message foo {
  float a = 1;
  string b = 2;
}

message bar {
  float c = 1;
  string d = 2;
}
`

	urlStr := runSchemaRegistryServer(b, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/things/versions/latest", "/schemas/ids/1":
			return mustJBytes(b, map[string]any{
				"id":         1,
				"version":    10,
				"schema":     thingsSchema,
				"schemaType": "PROTOBUF",
			}), nil
		}
		return nil, nil
	})

	b.Run("consistent message", func(b *testing.B) {
		runEncoderAgainstInputsMultiple(b, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
		})
	})

	b.Run("alternating messages", func(b *testing.B) {
		runEncoderAgainstInputsMultiple(b, urlStr, "things", [][]byte{
			[]byte(`{"a":1.23,"b":"foo"}`),
			[]byte(`{"c":2.34,"d":"bar"}`),
		})
	})
}
