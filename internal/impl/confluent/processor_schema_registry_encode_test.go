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
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var noopReqSign = func(fs.FS, *http.Request) error { return nil }

func TestSchemaRegistryEncoderConfigParse(t *testing.T) {
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
subject: foo
`,
			errContains: `parsing url`,
		},
		{
			name: "bad subject",
			config: `
url: http://example.com
subject: ${! bad interpolation }
`,
			errContains: `failed to parse interpolated field`,
		},
		{
			name: "use default period",
			config: `
url: http://example.com
subject: foo
`,
			expectedBaseURL: "http://example.com",
		},
		{
			name: "bad period",
			config: `
url: http://example.com
subject: foo
refresh_period: not a duration
`,
			errContains: "invalid duration",
		},
		{
			name: "url with base path",
			config: `
url: http://example.com/v1
subject: foo
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
subject: foo
`,
			expectedBaseURL: "http://example.com/v1",
		},
	}

	spec := schemaRegistryEncoderConfig()
	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.config, env)
			require.NoError(t, err)

			e, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
			if err == nil {
				_ = e.Close(t.Context())
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

func TestSchemaRegistryEncodeAvro(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     3,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo%2Fbar/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo/bar")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"Name":"foo","MaybeHobby":{"string":"dancing"}}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"my.namespace.com.address":{"City":{"string":"foo"},"State":"bar"}},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
		},
		{
			name:   "successful message no address and null hobby",
			input:  `{"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x00\x00",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":{"my.namespace.com.address":"not this","Name":"foo"}}`,
			errContains: `missing key`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeAvroRawJSON(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     3,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x02\x0edancing",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x02\x02\x06foo\x06bar\x00",
		},
		{
			name:   "successful message no address and null hobby",
			input:  `{"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03\x06foo\x00\x00",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":{"City":"foo","State":30},"Name":"foo","MaybeHobby":null}`,
			errContains: "cannot use json.Number with Avro type string",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeAvroLogicalTypes(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchemaLogicalTypes,
		ID:     4,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message with logical types avro json",
			input:  `{"int_time_millis":{"int.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			output: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
		},
		{
			// Bare union values are accepted alongside the tagged form, so
			// unwrapped input that once required the tagged shape encodes to
			// the same bytes.
			name:   "message with unwrapped unions",
			input:  `{"int_time_millis":35245000,"long_time_micros":20192000000000,"long_timestamp_micros":null,"pos_0_33333333":"!"}`,
			output: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x00\x02\x02!",
		},
		{
			// Wrong union key ("long.time-millis" instead of "int.time-millis")
			// is rejected by twmb/avro — the map value doesn't match any
			// union branch type.
			name:        "message doesnt match schema",
			input:       `{"int_time_millis":{"long.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			errContains: "int_time_millis",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, bErr := outBatches[0][0].AsBytes()
				require.NoError(t, bErr)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeAvroRawJSONLogicalTypes(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchemaLogicalTypes,
		ID:     4,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, true, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message with logical types raw json",
			input:  `{"int_time_millis":35245000,"long_time_micros":20192000000000,"long_timestamp_micros":62135596800000000,"pos_0_33333333":"!"}`,
			output: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
		},
		{
			// Tagged union maps are accepted by Encode — the branch
			// name is matched and the inner value is unwrapped.
			name:   "message with tagged unions",
			input:  `{"int_time_millis":{"int.time-millis":35245000},"long_time_micros":{"long.time-micros":20192000000000},"long_timestamp_micros":{"long.timestamp-micros":62135596800000000},"pos_0_33333333":{"bytes.decimal":"!"}}`,
			output: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
		},
		{
			// String value for a time-millis union field doesn't match the
			// int branch.
			name:        "message doesnt match schema",
			input:       `{"int_time_millis":"35245000","long_time_micros":20192000000000,"long_timestamp_micros":62135596800000000,"pos_0_33333333":"!"}`,
			errContains: "cannot use string with Avro type int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

// testSchemaBytesFixed carries every byte-shaped Avro type: plain bytes and
// fixed, plus a decimal backed by each. fixed is a separate wire shape — no
// length prefix, size checked — and therefore a separate path through both
// readers, so it is pinned next to bytes rather than assumed to follow it.
const testSchemaBytesFixed = `{
	"type": "record",
	"name": "BytesFixed",
	"fields": [
		{"name": "raw", "type": "bytes"},
		{"name": "raw_fixed", "type": {"type": "fixed", "name": "Raw4", "size": 4}},
		{"name": "dec_bytes", "type": {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}},
		{"name": "dec_fixed", "type": {"type": "fixed", "name": "Dec", "size": 16, "logicalType": "decimal", "precision": 38, "scale": 8}}
	]
}`

// testSchemaRawBytes is the smallest schema that a serialise-then-sniff
// encoder corrupts without erroring: encoding/json writes a []byte as base64
// text, every JSON string is a valid Avro JSON bytes value, so the base64
// characters encode cleanly as the field's bytes. A fixed field would be
// caught by its size check; a plain bytes field is not.
const testSchemaRawBytes = `{
	"type": "record",
	"name": "RawBytes",
	"fields": [{"name": "data", "type": "bytes"}]
}`

// testSchemaTimestamp carries the shape only Encode reads: Avro JSON spells a
// timestamp as a number, so RFC 3339 text and time.Time have to keep reaching
// Encode.
const testSchemaTimestamp = `{
	"type": "record",
	"name": "Timestamp",
	"fields": [
		{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-micros"}},
		{"name": "data", "type": "bytes"}
	]
}`

const (
	// raw is e9 00 7f 21: a byte above 0x7f, which Avro JSON spells as the
	// single codepoint U+00E9 and Encode would spell as two UTF-8 bytes.
	// raw_fixed is de ad be ef with no length prefix, dec_bytes is the
	// unscaled 0x21 ("!") at scale 2, and dec_fixed is the unscaled
	// 0xbc614e (12345678) at scale 8, left-padded to its 16 declared bytes.
	testWireBytesFixed = "\x00\x00\x00\x00\x04" +
		"\x08\xe9\x00\x7f!" +
		"\xde\xad\xbe\xef" +
		"\x02!" +
		"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xbcaN"

	testWireRawBytes = "\x00\x00\x00\x00\x04\x08\xe9\x00\x7f!"

	// 2021-01-01T00:00:00Z in micros, then the two bytes "ok".
	testWireTimestamp = "\x00\x00\x00\x00\x04\x80\x80\xac\xbe\xed\xf2\xdb\x05\x04ok"
)

// runAvroSchemaRegistry serves schemaSpec as schema ID 4, both by ID and as
// the latest version of subject "foo".
func runAvroSchemaRegistry(t *testing.T, schemaSpec string) string {
	t.Helper()

	byID, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{
		Schema: schemaSpec,
	})
	require.NoError(t, err)

	bySubject, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: schemaSpec,
		ID:     4,
	})
	require.NoError(t, err)

	return runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/schemas/ids/4":
			return byID, nil
		case "/subjects/foo/versions/latest":
			return bySubject, nil
		}
		return nil, errors.New("nope")
	})
}

func newTestAvroEncoder(t *testing.T, urlStr string, rawJSON bool) *schemaRegistryEncoder {
	t.Helper()

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, rawJSON, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = encoder.Close(context.Background()) })
	return encoder
}

// TestSchemaRegistryAvroDecodeEncodeRoundTrip pins the symmetry of the two
// processors: whatever schema_registry_decode emits must re-encode, through
// schema_registry_encode on the same schema, to the exact bytes it was decoded
// from — in both avro_raw_json modes, which differ only in whether unions are
// tagged.
//
// Decimals backed by bytes are the case that broke this. Avro JSON spells a
// bytes value as one codepoint per byte, so the unscaled value 0x21 is emitted
// as "!", which the native encoder read as a decimal in decimal notation and
// rejected. Decimals backed by fixed are the same defect on a different wire
// shape, and plain bytes above 0x7f are the same spelling mismatch without a
// logical type on top, so all three ride along.
func TestSchemaRegistryAvroDecodeEncodeRoundTrip(t *testing.T) {
	for _, schemaCase := range []struct {
		name   string
		schema string
		wires  []string
	}{
		{
			name:   "logical_types",
			schema: testSchemaLogicalTypes,
			wires: []string{
				"\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!",
				// Every union on its null branch.
				"\x00\x00\x00\x00\x04\x00\x00\x00\x00",
			},
		},
		{
			name:   "bytes_and_fixed",
			schema: testSchemaBytesFixed,
			wires:  []string{testWireBytesFixed},
		},
	} {
		t.Run(schemaCase.name, func(t *testing.T) {
			urlStr := runAvroSchemaRegistry(t, schemaCase.schema)

			for _, rawJSON := range []bool{false, true} {
				t.Run(fmt.Sprintf("avro_raw_json=%v", rawJSON), func(t *testing.T) {
					cfg := decodingConfig{}
					cfg.avro.rawUnions = rawJSON
					decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, schemaStaleAfter, service.MockResources())
					require.NoError(t, err)
					defer func() { _ = decoder.Close(t.Context()) }()

					encoder := newTestAvroEncoder(t, urlStr, rawJSON)

					for _, wire := range schemaCase.wires {
						decoded, err := decoder.Process(t.Context(), service.NewMessage([]byte(wire)))
						require.NoError(t, err)
						require.Len(t, decoded, 1)

						outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{decoded[0]})
						require.NoError(t, err)
						require.Len(t, outBatches, 1)
						require.Len(t, outBatches[0], 1)
						require.NoError(t, outBatches[0][0].GetError())

						b, err := outBatches[0][0].AsBytes()
						require.NoError(t, err)
						assert.Equal(t, wire, string(b))
					}
				})
			}
		})
	}
}

// TestSchemaRegistryEncodeAvroMessageForm pins which of the two readers each
// message form gets, and that both spell the same value as the same bytes.
//
// The two disagree about strings and neither is a superset of the other. Avro
// JSON writes a bytes or fixed value as one codepoint per byte, which only
// DecodeJSON understands; Go natives write it as a []byte, and a timestamp as
// time.Time or RFC 3339 text, which only Encode understands. Choosing between
// them by serialising the message and seeing whether the result parses as
// Avro JSON does not work: encoding/json renders a nested []byte as base64
// text, and DecodeJSON accepts any string for a bytes field, so the base64
// characters get encoded with no error raised.
func TestSchemaRegistryEncodeAvroMessageForm(t *testing.T) {
	structured := func(v map[string]any) func() *service.Message {
		return func() *service.Message {
			m := service.NewMessage(nil)
			m.SetStructuredMut(v)
			return m
		}
	}
	raw := func(s string) func() *service.Message {
		return func() *service.Message { return service.NewMessage([]byte(s)) }
	}

	for _, test := range []struct {
		name   string
		schema string
		msg    func() *service.Message
		output string
	}{
		{
			// A mapping such as `root.data = this.b64.decode("base64")`,
			// or anything downstream of schema_registry_decode with
			// preserve_logical_types.
			name:   "structured bytes field from a Go []byte",
			schema: testSchemaRawBytes,
			msg:    structured(map[string]any{"data": []byte{0xe9, 0x00, 0x7f, '!'}}),
			output: testWireRawBytes,
		},
		{
			// The same four bytes as Avro JSON. "é" is U+00E9, one
			// codepoint standing for the single byte 0xe9 — not its two
			// UTF-8 bytes.
			name:   "raw bytes field as Avro JSON codepoints",
			schema: testSchemaRawBytes,
			msg:    raw(`{"data":"é\u0000\u007f!"}`),
			output: testWireRawBytes,
		},
		{
			name:   "structured bytes and fixed fields from Go values",
			schema: testSchemaBytesFixed,
			msg: structured(map[string]any{
				"raw":       []byte{0xe9, 0x00, 0x7f, '!'},
				"raw_fixed": []byte{0xde, 0xad, 0xbe, 0xef},
				"dec_bytes": big.NewRat(33, 100),
				"dec_fixed": big.NewRat(12345678, 100000000),
			}),
			output: testWireBytesFixed,
		},
		{
			name:   "raw bytes and fixed fields as Avro JSON codepoints",
			schema: testSchemaBytesFixed,
			msg:    raw(`{"raw":"é\u0000\u007f!","raw_fixed":"\u00de\u00ad\u00be\u00ef","dec_bytes":"!","dec_fixed":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u00bcaN"}`),
			output: testWireBytesFixed,
		},
		{
			name:   "structured timestamp from time.Time",
			schema: testSchemaTimestamp,
			msg: structured(map[string]any{
				"ts":   time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				"data": []byte("ok"),
			}),
			output: testWireTimestamp,
		},
		{
			name:   "structured timestamp from an RFC 3339 string",
			schema: testSchemaTimestamp,
			msg: structured(map[string]any{
				"ts":   "2021-01-01T00:00:00Z",
				"data": []byte("ok"),
			}),
			output: testWireTimestamp,
		},
		{
			// Avro JSON has no spelling for this, so DecodeJSON rejects the
			// payload and it falls through to Encode.
			name:   "raw timestamp as an RFC 3339 string",
			schema: testSchemaTimestamp,
			msg:    raw(`{"ts":"2021-01-01T00:00:00Z","data":"ok"}`),
			output: testWireTimestamp,
		},
		{
			name:   "raw timestamp as Avro JSON micros",
			schema: testSchemaTimestamp,
			msg:    raw(`{"ts":1609459200000000,"data":"ok"}`),
			output: testWireTimestamp,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoder := newTestAvroEncoder(t, runAvroSchemaRegistry(t, test.schema), false)

			outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{test.msg()})
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)
			require.NoError(t, outBatches[0][0].GetError())

			b, err := outBatches[0][0].AsBytes()
			require.NoError(t, err)
			assert.Equal(t, test.output, string(b))
		})
	}
}

// TestSchemaRegistryAvroPreserveLogicalTypesRoundTrip is the same symmetry
// check as the decode/encode round trip, but over the structured form:
// preserve_logical_types hands the encoder a Go value tree carrying []byte for
// bytes and fixed fields rather than an Avro JSON payload. It is the shortest
// real pipeline that reaches the encoder with a nested []byte.
func TestSchemaRegistryAvroPreserveLogicalTypesRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name   string
		schema string
		wire   string
	}{
		{name: "bytes", schema: testSchemaRawBytes, wire: testWireRawBytes},
		{name: "bytes_and_fixed", schema: testSchemaBytesFixed, wire: testWireBytesFixed},
	} {
		t.Run(test.name, func(t *testing.T) {
			urlStr := runAvroSchemaRegistry(t, test.schema)

			cfg := decodingConfig{}
			cfg.avro.rawUnions = true
			cfg.avro.preserveLogicalTypes = true
			decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, schemaStaleAfter, service.MockResources())
			require.NoError(t, err)
			defer func() { _ = decoder.Close(t.Context()) }()

			encoder := newTestAvroEncoder(t, urlStr, true)

			decoded, err := decoder.Process(t.Context(), service.NewMessage([]byte(test.wire)))
			require.NoError(t, err)
			require.Len(t, decoded, 1)
			require.True(t, decoded[0].HasStructured(), "preserve_logical_types must emit a structured message")

			outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{decoded[0]})
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)
			require.NoError(t, outBatches[0][0].GetError())

			b, err := outBatches[0][0].AsBytes()
			require.NoError(t, err)
			assert.Equal(t, test.wire, string(b))
		})
	}
}

// newTestAvroEncoderInputEnc is newTestAvroEncoder with the input_encoding
// field set, which the plain helper leaves at its auto default.
func newTestAvroEncoderInputEnc(t *testing.T, urlStr string, inputEnc string) *schemaRegistryEncoder {
	t.Helper()

	encoder := newTestAvroEncoder(t, urlStr, false)
	encoder.avroInputEnc = inputEnc
	return encoder
}

// TestSchemaRegistryAvroDecodeMappedEncodeRoundTrip is the round trip through
// the pipeline shape that reaches production: decode, then some processor in
// between, then encode. A mapping, a branch, a jq — anything that reads the
// message structurally parses the payload and drops it, so the message arrives
// at the encoder carrying Avro JSON values in structured form.
//
// Nothing distinguishes that message from one a mapping built from scratch:
// AsStructuredMut clears the raw bytes, so both are a structured message with
// no payload behind it, and the Avro JSON string "!" is the same Go value as a
// decimal written "3.33". Form cannot answer it and neither can the values, so
// auto reads these the plain way and fails on the bytes-backed decimal. Only a
// declared input_encoding gets the round trip back.
func TestSchemaRegistryAvroDecodeMappedEncodeRoundTrip(t *testing.T) {
	for _, schemaCase := range []struct {
		name   string
		schema string
		wire   string
	}{
		{name: "logical_types", schema: testSchemaLogicalTypes, wire: "\x00\x00\x00\x00\x04\x02\x90\xaf\xce!\x02\x80\x80揪\x97\t\x02\x80\x80\xde\xf2\xdf\xff\xdf\xdc\x01\x02\x02!"},
		{name: "bytes_and_fixed", schema: testSchemaBytesFixed, wire: testWireBytesFixed},
	} {
		t.Run(schemaCase.name, func(t *testing.T) {
			urlStr := runAvroSchemaRegistry(t, schemaCase.schema)

			for _, rawJSON := range []bool{false, true} {
				t.Run(fmt.Sprintf("avro_raw_json=%v", rawJSON), func(t *testing.T) {
					cfg := decodingConfig{}
					cfg.avro.rawUnions = rawJSON
					decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, schemaStaleAfter, service.MockResources())
					require.NoError(t, err)
					defer func() { _ = decoder.Close(t.Context()) }()

					decoded, err := decoder.Process(t.Context(), service.NewMessage([]byte(schemaCase.wire)))
					require.NoError(t, err)
					require.Len(t, decoded, 1)
					require.False(t, decoded[0].HasStructured(), "decoder without preserve_logical_types emits a raw payload")

					// The processor in between.
					_, err = decoded[0].AsStructuredMut()
					require.NoError(t, err)
					require.True(t, decoded[0].HasStructured(), "reading the message structurally caches the parse")
					require.False(t, decoded[0].HasBytes(), "and drops the payload it parsed")

					encoder := newTestAvroEncoderInputEnc(t, urlStr, avroInputEncAvroJSON)
					outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{decoded[0]})
					require.NoError(t, err)
					require.Len(t, outBatches, 1)
					require.Len(t, outBatches[0], 1)
					require.NoError(t, outBatches[0][0].GetError())

					b, err := outBatches[0][0].AsBytes()
					require.NoError(t, err)
					assert.Equal(t, schemaCase.wire, string(b))
				})
			}
		})
	}
}

// TestSchemaRegistryEncodeAvroInputEncoding pins what each input_encoding does
// with the values only one reader understands, and that the modes disagree
// where they are meant to: the same JSON string is a different value under
// each, which is the whole reason the field exists.
func TestSchemaRegistryEncodeAvroInputEncoding(t *testing.T) {
	const schemaDecimal = `{
		"type": "record",
		"name": "Decimal",
		"fields": [{"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}}]
	}`

	// The unscaled 0x21 at scale 2, which is 0.33, and 3.33 as the unscaled 333.
	const (
		wireAvroJSONReading = "\x00\x00\x00\x00\x04\x02!"
		wireNativeReading   = "\x00\x00\x00\x00\x04\x04\x01M"
	)

	structured := func(v map[string]any) func() *service.Message {
		return func() *service.Message {
			m := service.NewMessage(nil)
			m.SetStructuredMut(v)
			return m
		}
	}
	raw := func(s string) func() *service.Message {
		return func() *service.Message { return service.NewMessage([]byte(s)) }
	}

	for _, test := range []struct {
		name     string
		schema   string
		inputEnc string
		msg      func() *service.Message
		output   string
		errStr   string
	}{
		{
			// The case auto cannot serve: Avro JSON values that arrived
			// structured because something parsed the payload.
			name:     "avro_json reads a structured Avro JSON string as codepoints",
			schema:   schemaDecimal,
			inputEnc: avroInputEncAvroJSON,
			msg:      structured(map[string]any{"dec": "!"}),
			output:   wireAvroJSONReading,
		},
		{
			// The same string under auto, which reads a structured message
			// the plain way and rejects it.
			name:     "auto rejects the same string",
			schema:   schemaDecimal,
			inputEnc: avroInputEncAuto,
			msg:      structured(map[string]any{"dec": "!"}),
			errStr:   "invalid decimal string",
		},
		{
			// Hand-written decimal notation, which Avro JSON reads as four
			// codepoint bytes and native reads as the number.
			name:     "native reads a raw decimal string as decimal notation",
			schema:   schemaDecimal,
			inputEnc: avroInputEncNative,
			msg:      raw(`{"dec":"3.33"}`),
			output:   wireNativeReading,
		},
		{
			name:     "auto reads the same raw payload as Avro JSON",
			schema:   schemaDecimal,
			inputEnc: avroInputEncAuto,
			msg:      raw(`{"dec":"3.33"}`),
			output:   "\x00\x00\x00\x00\x04\b3.33",
		},
		{
			// Encode is the only reader that takes these, so native must keep
			// reaching it.
			name:     "native takes time.Time",
			schema:   testSchemaTimestamp,
			inputEnc: avroInputEncNative,
			msg: structured(map[string]any{
				"ts":   time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				"data": []byte("ok"),
			}),
			output: testWireTimestamp,
		},
		{
			// A tree holding a []byte was never Avro JSON: serialising it
			// writes base64 text that DecodeJSON would accept as the value.
			// Refusing it is the point — the alternative is encoding bytes
			// nobody supplied.
			name:     "avro_json refuses a Go []byte rather than encoding its base64",
			schema:   testSchemaRawBytes,
			inputEnc: avroInputEncAvroJSON,
			msg:      structured(map[string]any{"data": []byte{0xe9, 0x00, 0x7f, '!'}}),
			errStr:   "Avro JSON cannot spell",
		},
		{
			// A raw payload keeps its own bytes, so the same mode encodes the
			// identical value without complaint.
			name:     "avro_json takes the same bytes as a raw payload",
			schema:   testSchemaRawBytes,
			inputEnc: avroInputEncAvroJSON,
			msg:      raw(`{"data":"é\u0000\u007f!"}`),
			output:   testWireRawBytes,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoder := newTestAvroEncoderInputEnc(t, runAvroSchemaRegistry(t, test.schema), test.inputEnc)

			outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{test.msg()})
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			if test.errStr != "" {
				procErr := outBatches[0][0].GetError()
				require.Error(t, procErr)
				assert.Contains(t, procErr.Error(), test.errStr)

				// A message that fails to encode must reach a dead-letter
				// output unchanged, so pin the payload itself rather than
				// merely the absence of a wire header — an emptied payload
				// would satisfy that and lose the data.
				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.NotEmpty(t, b, "a failed encode must not empty the message")
				assert.False(t, strings.HasPrefix(string(b), "\x00"), "a failed encode must not emit an Avro payload")
				return
			}

			require.NoError(t, outBatches[0][0].GetError())
			b, err := outBatches[0][0].AsBytes()
			require.NoError(t, err)
			assert.Equal(t, test.output, string(b))
		})
	}
}

// TestSchemaRegistryEncodeAvroNativeKeepsLoudFailures pins that native does not
// reinterpret what Encode rejects. Every string is a valid Avro JSON bytes
// value, so a reader that retried these as Avro JSON would silently encode
// their codepoints — "abc" as 0x616263 — instead of reporting the mistake.
func TestSchemaRegistryEncodeAvroNativeKeepsLoudFailures(t *testing.T) {
	const schemaDecimal = `{
		"type": "record",
		"name": "Decimal",
		"fields": [{"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 16, "scale": 2}}]
	}`
	urlStr := runAvroSchemaRegistry(t, schemaDecimal)

	// "1e5" is deliberately absent: Encode reads scientific notation as the
	// number 100000, which is a value it accepts rather than one it rejects.
	for _, value := range []string{"abc", "3.333", "99999999999999999.99"} {
		t.Run(value, func(t *testing.T) {
			for _, inputEnc := range []string{avroInputEncNative, avroInputEncAuto} {
				t.Run(inputEnc, func(t *testing.T) {
					encoder := newTestAvroEncoderInputEnc(t, urlStr, inputEnc)

					msg := service.NewMessage(nil)
					msg.SetStructuredMut(map[string]any{"dec": value})

					outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
					require.NoError(t, err)
					require.Error(t, outBatches[0][0].GetError(), "must not be reinterpreted as Avro JSON codepoints")
				})
			}
		})
	}
}

// TestSchemaRegistryEncodeAvroJSONIgnoresCachedBytes pins that avro_json reads
// the structured tree rather than whatever the message happens to have cached.
//
// AsBytes serialises the structured form and keeps the result, so any component
// that looks at the payload first — a log processor, an output interpolation,
// this processor's own subject interpolation — leaves the message holding bytes
// derived from the tree. For a []byte field those bytes are base64 text, and
// every JSON string is a valid Avro JSON bytes value, so reading them back
// would encode the characters of "6QB/IQ==" with no error raised. Trusting a
// cached payload is indistinguishable from trusting an original one, so neither
// is trusted.
func TestSchemaRegistryEncodeAvroJSONIgnoresCachedBytes(t *testing.T) {
	encoder := newTestAvroEncoderInputEnc(t, runAvroSchemaRegistry(t, testSchemaRawBytes), avroInputEncAvroJSON)

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any{"data": []byte{0xe9, 0x00, 0x7f, '!'}})

	// Whatever the earlier component did.
	cached, err := msg.AsBytes()
	require.NoError(t, err)
	require.Contains(t, string(cached), "6QB/IQ==", "AsBytes must have cached the base64 spelling")
	require.True(t, msg.HasBytes(), "and left it on the message")

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	procErr := outBatches[0][0].GetError()
	require.Error(t, procErr, "a []byte tree must be refused, not encoded from its base64")
	assert.Contains(t, procErr.Error(), "Avro JSON cannot spell")

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, string(cached), string(b), "a refused message must reach a dead-letter output untouched")
}

// TestSchemaRegistryEncodeAvroJSONUnserialisableTree pins what happens to a
// value JSON cannot spell at all. NaN is admitted by the type check and refused
// by the marshaller, and the message must survive that with its contents and a
// diagnosis that names the real cause.
func TestSchemaRegistryEncodeAvroJSONUnserialisableTree(t *testing.T) {
	const schemaDouble = `{
		"type": "record",
		"name": "Double",
		"fields": [{"name": "d", "type": "double"}]
	}`
	encoder := newTestAvroEncoderInputEnc(t, runAvroSchemaRegistry(t, schemaDouble), avroInputEncAvroJSON)

	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any{"d": math.NaN()})

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	procErr := outBatches[0][0].GetError()
	require.Error(t, procErr)
	assert.Contains(t, procErr.Error(), "serialising structured data", "the error must name the real cause")

	// The structured form is what survives. Asking such a message for bytes
	// yields nothing whatever the encoder does, because benthos serialises the
	// tree to answer and JSON has no spelling for NaN, so the guarantee worth
	// pinning is that the encoder left the contents alone.
	native, err := outBatches[0][0].AsStructured()
	require.NoError(t, err)
	tree, ok := native.(map[string]any)
	require.True(t, ok)
	assert.True(t, math.IsNaN(tree["d"].(float64)), "the tree must reach a dead-letter output intact")
}

// TestSchemaRegistryEncodeAvroInputEncodingFromConfig pins that the field
// reaches the encoder from YAML, and that a config which never mentions it gets
// auto — the value that preserves the behaviour every existing pipeline has.
func TestSchemaRegistryEncodeAvroInputEncodingFromConfig(t *testing.T) {
	for _, test := range []struct {
		name   string
		conf   string
		expect string
	}{
		{
			name:   "absent avro block defaults to auto",
			conf:   `subject: foo`,
			expect: avroInputEncAuto,
		},
		{
			name: "avro block without the field defaults to auto",
			conf: `
subject: foo
avro:
  raw_json: true`,
			expect: avroInputEncAuto,
		},
		{
			name: "explicit avro_json",
			conf: `
subject: foo
avro:
  input_encoding: avro_json`,
			expect: avroInputEncAvroJSON,
		},
		{
			name: "explicit native",
			conf: `
subject: foo
avro:
  input_encoding: native`,
			expect: avroInputEncNative,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			urlStr := runSchemaRegistryServer(t, func(string) ([]byte, error) {
				return nil, errors.New("nope")
			})

			conf, err := schemaRegistryEncoderConfig().ParseYAML("url: "+urlStr+"\n"+test.conf, nil)
			require.NoError(t, err)

			encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
			require.NoError(t, err)
			t.Cleanup(func() { _ = encoder.Close(context.Background()) })

			assert.Equal(t, test.expect, encoder.avroInputEnc)
		})
	}

	// A value outside the enum is caught by config linting rather than by
	// parsing, so what matters here is that one reaching the encoder anyway
	// cannot leave it in a mode it has no branch for: everything unrecognised
	// reads as auto, which is the behaviour a pipeline had before this field
	// existed.
	t.Run("an unrecognised value reads as auto", func(t *testing.T) {
		urlStr := runAvroSchemaRegistry(t, testSchemaRawBytes)
		encoder := newTestAvroEncoderInputEnc(t, urlStr, "nope")

		outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{
			service.NewMessage([]byte(`{"data":"é\u0000\u007f!"}`)),
		})
		require.NoError(t, err)
		require.NoError(t, outBatches[0][0].GetError())

		b, err := outBatches[0][0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, testWireRawBytes, string(b), "must behave as auto")
	})
}

func TestSchemaRegistryEncodeClearExpired(t *testing.T) {
	urlStr := runSchemaRegistryServer(t, func(string) ([]byte, error) {
		return nil, fmt.Errorf("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, encoder.Close(t.Context()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	encoder.cacheMut.Lock()
	encoder.schemas = map[string]cachedSchemaEncoder{
		"5":  {lastUsedUnixSeconds: tStale, lastUpdatedUnixSeconds: tNotStale},
		"10": {lastUsedUnixSeconds: tNotStale, lastUpdatedUnixSeconds: tNotStale},
		"15": {lastUsedUnixSeconds: tNearlyStale, lastUpdatedUnixSeconds: tNotStale},
	}
	encoder.cacheMut.Unlock()

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	assert.Equal(t, map[string]cachedSchemaEncoder{
		"10": {lastUsedUnixSeconds: tNotStale, lastUpdatedUnixSeconds: tNotStale},
		"15": {lastUsedUnixSeconds: tNearlyStale, lastUpdatedUnixSeconds: tNotStale},
	}, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeRefresh(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     2,
	})
	require.NoError(t, err)

	barFirst, err := json.Marshal(struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{
		Schema: testSchema,
		ID:     12,
	})
	require.NoError(t, err)

	var fooReqs, barReqs int32
	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/foo/versions/latest":
			atomic.AddInt32(&fooReqs, 1)
			return fooFirst, nil
		case "/subjects/bar/versions/latest":
			atomic.AddInt32(&barReqs, 1)
			return barFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, encoder.Close(t.Context()))

	tStale := time.Now().Add(-time.Hour).Unix()
	tNotStale := time.Now().Unix()
	tNearlyStale := time.Now().Add(-(schemaStaleAfter / 2)).Unix()

	encoder.nowFn = func() time.Time {
		return time.Unix(tNotStale, 0)
	}

	encoder.cacheMut.Lock()
	encoder.schemas = map[string]cachedSchemaEncoder{
		"foo": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tStale,
			id:                     1,
		},
		"bar": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNearlyStale,
			id:                     11,
		},
	}
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(0), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(0), atomic.LoadInt32(&barReqs))

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	tmpFoo := encoder.schemas["foo"]
	tmpFoo.encoder = nil
	encoder.schemas["foo"] = tmpFoo
	assert.Equal(t, map[string]cachedSchemaEncoder{
		"foo": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNotStale,
			id:                     2,
		},
		"bar": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNearlyStale,
			id:                     11,
		},
	}, encoder.schemas)
	tmpBar := encoder.schemas["bar"]
	tmpBar.lastUpdatedUnixSeconds = tStale
	encoder.schemas["bar"] = tmpBar
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(1), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(0), atomic.LoadInt32(&barReqs))

	encoder.refreshEncoders()

	encoder.cacheMut.Lock()
	tmpBar = encoder.schemas["bar"]
	tmpBar.encoder = nil
	encoder.schemas["bar"] = tmpBar
	assert.Equal(t, map[string]cachedSchemaEncoder{
		"foo": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNotStale,
			id:                     2,
		},
		"bar": {
			lastUsedUnixSeconds:    tNotStale,
			lastUpdatedUnixSeconds: tNotStale,
			id:                     12,
		},
	}, encoder.schemas)
	encoder.cacheMut.Unlock()

	assert.Equal(t, int32(1), atomic.LoadInt32(&fooReqs))
	assert.Equal(t, int32(1), atomic.LoadInt32(&barReqs))
}

func TestSchemaRegistryEncodeJSON(t *testing.T) {
	fooFirst, err := json.Marshal(struct {
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
		ID         int    `json:"id"`
	}{
		Schema:     testJSONSchema,
		SchemaType: "JSON",
		ID:         3,
	})
	require.NoError(t, err)

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return fooFirst, nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Minute*10, time.Minute, service.MockResources())
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       string
		output      string
		errContains string
	}{
		{
			name:   "successful message",
			input:  `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`,
			output: "\x00\x00\x00\x00\x03{\"Address\":{\"City\":\"foo\",\"State\":\"bar\"},\"Name\":\"foo\",\"MaybeHobby\":\"dancing\"}",
		},
		{
			name:   "successful message null hobby",
			input:  `{"Address":{"City": "foo","State":"bar"},"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03{\"Address\":{\"City\": \"foo\",\"State\":\"bar\"},\"Name\":\"foo\",\"MaybeHobby\":null}",
		},
		{
			name:   "successful message no address and null hobby",
			input:  `{"Name":"foo","MaybeHobby":null}`,
			output: "\x00\x00\x00\x00\x03{\"Name\":\"foo\",\"MaybeHobby\":null}",
		},
		{
			name:        "message doesnt match schema",
			input:       `{"Address":"not this","Name":"foo"}`,
			errContains: "json message does not conform to schema",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outBatches, err := encoder.ProcessBatch(
				t.Context(),
				service.MessageBatch{service.NewMessage([]byte(test.input))},
			)
			require.NoError(t, err)
			require.Len(t, outBatches, 1)
			require.Len(t, outBatches[0], 1)

			err = outBatches[0][0].GetError()
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				assert.Equal(t, test.output, string(b))
			}
		})
	}

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

func TestSchemaRegistryEncodeJSONConstantRefreshes(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m != t.Name() {
		t.Skip()
	}

	fooID := int64(1)
	nextFoo := func() []byte {
		t.Helper()
		fooData, err := json.Marshal(struct {
			Schema     string `json:"schema"`
			SchemaType string `json:"schemaType"`
			ID         int64  `json:"id"`
		}{
			Schema:     testJSONSchema,
			SchemaType: "JSON",
			ID:         atomic.AddInt64(&fooID, 1),
		})
		require.NoError(t, err)
		return fooData
	}

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if path == "/subjects/foo/versions/latest" {
			return nextFoo(), nil
		}
		return nil, errors.New("nope")
	})

	subj, err := service.NewInterpolatedString("foo")
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoder(urlStr, noopReqSign, nil, subj, false, time.Millisecond, time.Millisecond*10, service.MockResources())
	require.NoError(t, err)

	input := `{"Address":{"City":"foo","State":"bar"},"Name":"foo","MaybeHobby":"dancing"}`
	outputPrefix := "\x00\x00\x00"
	outputSuffix := "{\"Address\":{\"City\":\"foo\",\"State\":\"bar\"},\"Name\":\"foo\",\"MaybeHobby\":\"dancing\"}"

	tStarted := time.Now()

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for time.Since(tStarted) <= (time.Second * 300) {

				outBatches, err := encoder.ProcessBatch(
					t.Context(),
					service.MessageBatch{service.NewMessage([]byte(input))},
				)
				require.NoError(t, err)
				require.Len(t, outBatches, 1)
				require.Len(t, outBatches[0], 1)

				err = outBatches[0][0].GetError()
				require.NoError(t, err)

				b, err := outBatches[0][0].AsBytes()
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(string(b), outputPrefix), string(b))
				require.True(t, strings.HasSuffix(string(b), outputSuffix), string(b))
			}
		})
	}

	wg.Wait()

	require.NoError(t, encoder.Close(t.Context()))
	encoder.cacheMut.Lock()
	assert.Empty(t, encoder.schemas)
	encoder.cacheMut.Unlock()
}

//------------------------------------------------------------------------------
// Metadata-mode tests
//------------------------------------------------------------------------------

// metaMockRegistration records a single CreateSchema call.
type metaMockRegistration struct {
	Subject   string
	SchemaStr string
	Normalize bool
	ID        int
}

// metaMockState holds all the tracked state from a mock registry.
type metaMockState struct {
	mu            sync.Mutex
	nextID        int
	calls         map[string]int         // subject → count
	registrations []metaMockRegistration // ordered list
	schemas       map[int]string         // id → schema body
	idToSubject   map[int]string         // id → subject (for versions endpoint)
	idToVersion   map[int]int            // id → version within subject
	subjectVer    map[string]int         // subject → next version counter
}

func newMetaMockState() *metaMockState {
	return &metaMockState{
		nextID:      1,
		calls:       map[string]int{},
		schemas:     map[int]string{},
		idToSubject: map[int]string{},
		idToVersion: map[int]int{},
		subjectVer:  map[string]int{},
	}
}

func (s *metaMockState) getCalls() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make(map[string]int, len(s.calls))
	maps.Copy(cp, s.calls)
	return cp
}

func (s *metaMockState) getRegistrations() []metaMockRegistration {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]metaMockRegistration, len(s.registrations))
	copy(cp, s.registrations)
	return cp
}

// runMetaMockRegistry creates a mock schema registry that handles
// POST /subjects/{subject}/versions for CreateSchema, returning incrementing IDs.
// It also handles the franz-go follow-up GET requests for schema validation.
func runMetaMockRegistry(t *testing.T) (url string, state *metaMockState) {
	t.Helper()

	state = newMetaMockState()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		defer state.mu.Unlock()

		path := r.URL.Path

		// POST /subjects/{subject}/versions — CreateSchema
		if r.Method == http.MethodPost && strings.Contains(path, "/subjects/") && strings.HasSuffix(path, "/versions") {
			body, _ := io.ReadAll(r.Body)
			subject := strings.TrimPrefix(path, "/subjects/")
			subject = strings.TrimSuffix(subject, "/versions")
			state.calls[subject]++

			normalize := r.URL.Query().Get("normalize") == "true"

			id := state.nextID
			state.nextID++

			var posted map[string]any
			_ = json.Unmarshal(body, &posted)
			schemaStr, _ := posted["schema"].(string)
			state.schemas[id] = schemaStr
			state.idToSubject[id] = subject

			state.subjectVer[subject]++
			version := state.subjectVer[subject]
			state.idToVersion[id] = version

			state.registrations = append(state.registrations, metaMockRegistration{
				Subject:   subject,
				SchemaStr: schemaStr,
				Normalize: normalize,
				ID:        id,
			})

			resp, _ := json.Marshal(map[string]int{"id": id})
			_, _ = w.Write(resp)
			return
		}

		// GET /schemas/ids/{id}/versions — franz-go calls this after CreateSchema.
		if r.Method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/") && strings.HasSuffix(path, "/versions") {
			idPart := strings.TrimPrefix(path, "/schemas/ids/")
			idPart = strings.TrimSuffix(idPart, "/versions")
			var id int
			if _, err := fmt.Sscanf(idPart, "%d", &id); err == nil {
				if subject, ok := state.idToSubject[id]; ok {
					resp, _ := json.Marshal([]map[string]any{
						{"subject": subject, "version": state.idToVersion[id]},
					})
					_, _ = w.Write(resp)
					return
				}
			}
		}

		// GET /schemas/ids/{id} — GetSchemaByID
		if r.Method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/") && !strings.HasSuffix(path, "/versions") {
			idPart := strings.TrimPrefix(path, "/schemas/ids/")
			var id int
			if _, err := fmt.Sscanf(idPart, "%d", &id); err == nil {
				if schemaBody, ok := state.schemas[id]; ok {
					resp, _ := json.Marshal(map[string]any{
						"schema": schemaBody,
						"id":     id,
					})
					_, _ = w.Write(resp)
					return
				}
			}
		}

		// GET /subjects/{subject}/versions/{version} — franz-go fetches this to validate
		if r.Method == http.MethodGet && strings.Contains(path, "/subjects/") && strings.Contains(path, "/versions/") {
			parts := strings.SplitN(strings.TrimPrefix(path, "/subjects/"), "/versions/", 2)
			if len(parts) == 2 {
				var version int
				if _, err := fmt.Sscanf(parts[1], "%d", &version); err == nil {
					// Find the schema ID by subject+version.
					for id, subj := range state.idToSubject {
						if subj == parts[0] && state.idToVersion[id] == version {
							resp, _ := json.Marshal(map[string]any{
								"subject": parts[0],
								"version": version,
								"id":      id,
								"schema":  state.schemas[id],
							})
							_, _ = w.Write(resp)
							return
						}
					}
				}
			}
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	return ts.URL, state
}

func makeCommonSchemaMeta(t *testing.T, fields ...schema.Common) any {
	t.Helper()
	c := schema.Common{
		Type:     schema.Object,
		Name:     "test_record",
		Children: fields,
	}
	return c.ToAny()
}

func TestSchemaRegistryEncodeMetadataAvroHappyPath(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "age", Type: schema.Int32},
	)

	msg := service.NewMessage([]byte(`{"name":"alice","age":30}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	require.NoError(t, outBatches[0][0].GetError())

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	// Verify Confluent wire format: magic byte + 4-byte schema ID + Avro binary.
	require.Greater(t, len(b), 5, "output must have wire header")
	assert.Equal(t, byte(0x00), b[0], "magic byte")
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(1), schemaID)
	assert.Equal(t, 1, mockState.getCalls()["test-subject"])
}

func TestSchemaRegistryEncodeMetadataMissingMetadata(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	msg := service.NewMessage([]byte(`{"name":"alice"}`))
	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)

	msgErr := outBatches[0][0].GetError()
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "schema metadata key")
}

func TestSchemaRegistryEncodeMetadataCaching(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})

	for range 2 {
		msg := service.NewMessage([]byte(`{"x":1}`))
		msg.MetaSetMut("schema", schemaMeta)
		outBatches, bErr := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
		require.NoError(t, bErr)
		require.NoError(t, outBatches[0][0].GetError())
	}

	assert.Equal(t, 1, mockState.getCalls()["test-subject"], "schema should be registered only once")
}

func TestSchemaRegistryEncodeMetadataSchemaEvolution(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemav1 := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg1 := service.NewMessage([]byte(`{"x":1}`))
	msg1.MetaSetMut("schema", schemav1)
	out1, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg1})
	require.NoError(t, err)
	require.NoError(t, out1[0][0].GetError())

	schemav2 := makeCommonSchemaMeta(t,
		schema.Common{Name: "x", Type: schema.Int32},
		schema.Common{Name: "y", Type: schema.String},
	)
	msg2 := service.NewMessage([]byte(`{"x":1,"y":"hello"}`))
	msg2.MetaSetMut("schema", schemav2)
	out2, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg2})
	require.NoError(t, err)
	require.NoError(t, out2[0][0].GetError())

	assert.Equal(t, 2, mockState.getCalls()["test-subject"])

	b1, _ := out1[0][0].AsBytes()
	b2, _ := out2[0][0].AsBytes()
	id1 := binary.BigEndian.Uint32(b1[1:5])
	id2 := binary.BigEndian.Uint32(b2[1:5])
	assert.NotEqual(t, id1, id2, "different schemas should get different IDs")
}

func TestSchemaRegistryEncodeMetadataRegistryError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, ts.URL), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)

	msgErr := outBatches[0][0].GetError()
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "registering schema")
}

func TestSchemaRegistryEncodeMetadataJSONSchemaHappyPath(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: json_schema
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "age", Type: schema.Int32},
	)
	msg := service.NewMessage([]byte(`{"name":"alice","age":30}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	assert.Equal(t, `{"name":"alice","age":30}`, string(b[5:]))
	assert.Equal(t, 1, mockState.getCalls()["test-subject"])
}

func TestSchemaRegistryEncodeMetadataJSONSchemaValidationFailure(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: json_schema
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "age", Type: schema.Int32},
	)
	msg := service.NewMessage([]byte(`{"name":"alice","age":"not a number"}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)

	msgErr := outBatches[0][0].GetError()
	require.Error(t, msgErr)
	assert.Contains(t, msgErr.Error(), "does not conform to schema")
}

func TestSchemaRegistryEncodeMetadataConfigValidation(t *testing.T) {
	spec := schemaRegistryEncoderConfig()
	env := service.NewEnvironment()

	tests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "schema_metadata without format",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
`,
			errContains: "format is required",
		},
		{
			name: "format without schema_metadata",
			config: `
url: http://example.com
subject: foo
format: avro
`,
			errContains: "format is only used when schema_metadata is set",
		},
		{
			name: "avro format without explicit raw_json",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
`,
			errContains: "avro.raw_json to be explicitly set",
		},
		{
			name: "avro format with avro.raw_json succeeds",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
avro:
  raw_json: true
`,
		},
		{
			name: "avro format with deprecated avro_raw_json still requires avro.raw_json",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
avro_raw_json: true
`,
			errContains: "avro.raw_json to be explicitly set",
		},
		{
			name: "json_schema format without raw_json succeeds",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: json_schema
`,
		},
		{
			name: "avro.raw_json overrides avro_raw_json",
			config: `
url: http://example.com
subject: foo
schema_metadata: schema
format: avro
avro_raw_json: false
avro:
  raw_json: true
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := spec.ParseYAML(test.config, env)
			require.NoError(t, err)

			e, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
			if e != nil {
				_ = e.Close(t.Context())
			}
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

//------------------------------------------------------------------------------
// Additional metadata-mode coverage
//------------------------------------------------------------------------------

func TestSchemaRegistryEncodeMetadataAvroJSONEncoding(t *testing.T) {
	// Test with avro.raw_json: false — messages must use Avro JSON union format.
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: false
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "hobby", Type: schema.String, Optional: true},
	)

	// Avro JSON format: optional fields require {"string": "value"} wrapper.
	msg := service.NewMessage([]byte(`{"name":"alice","hobby":{"string":"dancing"}}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)
	require.Greater(t, len(b), 5, "output must have wire header + avro binary")

	// Verify null hobby also works in Avro JSON format.
	msg2 := service.NewMessage([]byte(`{"name":"bob","hobby":null}`))
	msg2.MetaSetMut("schema", schemaMeta)
	out2, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg2})
	require.NoError(t, err)
	require.NoError(t, out2[0][0].GetError())

	_ = mockState
}

func TestSchemaRegistryEncodeMetadataRecordNameAndNamespace(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
  record_name: CustomRecord
  namespace: com.example.test
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Use a schema with no root name so the configured record_name is used.
	c := schema.Common{
		Type:     schema.Object,
		Children: []schema.Common{{Name: "x", Type: schema.Int32}},
	}
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", c.ToAny())

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	regs := mockState.getRegistrations()
	require.Len(t, regs, 1)

	var avroSchema map[string]any
	require.NoError(t, json.Unmarshal([]byte(regs[0].SchemaStr), &avroSchema))
	assert.Equal(t, "CustomRecord", avroSchema["name"])
	assert.Equal(t, "com.example.test", avroSchema["namespace"])
}

func TestSchemaRegistryEncodeMetadataRecordNameFromSubject(t *testing.T) {
	// When record_name is not set and Common.Name is empty, derive from subject.
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: my-topic-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Schema with no root name — subject should be used as fallback.
	c := schema.Common{
		Type:     schema.Object,
		Children: []schema.Common{{Name: "x", Type: schema.Int32}},
	}
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", c.ToAny())

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	regs := mockState.getRegistrations()
	require.Len(t, regs, 1)

	var avroSchema map[string]any
	require.NoError(t, json.Unmarshal([]byte(regs[0].SchemaStr), &avroSchema))
	assert.Equal(t, "my_topic_value", avroSchema["name"], "hyphens should be sanitized to underscores")
}

func TestSchemaRegistryEncodeMetadataSubjectInterpolation(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: ${! meta("kafka_topic") }-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})

	// Two messages with different topics → different subjects → separate registrations.
	msg1 := service.NewMessage([]byte(`{"x":1}`))
	msg1.MetaSetMut("kafka_topic", "topicA")
	msg1.MetaSetMut("schema", schemaMeta)

	msg2 := service.NewMessage([]byte(`{"x":2}`))
	msg2.MetaSetMut("kafka_topic", "topicB")
	msg2.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg1, msg2})
	require.NoError(t, err)
	require.Len(t, outBatches[0], 2)
	require.NoError(t, outBatches[0][0].GetError())
	require.NoError(t, outBatches[0][1].GetError())

	calls := mockState.getCalls()
	assert.Equal(t, 1, calls["topicA-value"])
	assert.Equal(t, 1, calls["topicB-value"])
}

func TestSchemaRegistryEncodeMetadataMixedBatch(t *testing.T) {
	// A batch where one message has schema metadata and another doesn't.
	// The invalid message should get an error; the valid one should succeed.
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})

	good := service.NewMessage([]byte(`{"x":1}`))
	good.MetaSetMut("schema", schemaMeta)

	bad := service.NewMessage([]byte(`{"x":2}`))
	// bad has no schema metadata

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{good, bad})
	require.NoError(t, err)
	require.Len(t, outBatches[0], 2)

	require.NoError(t, outBatches[0][0].GetError(), "good message should succeed")

	badErr := outBatches[0][1].GetError()
	require.Error(t, badErr, "bad message should have error")
	assert.Contains(t, badErr.Error(), "schema metadata key")
}

func TestSchemaRegistryEncodeMetadataNormalize(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
normalize: true
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	regs := mockState.getRegistrations()
	require.Len(t, regs, 1)
	assert.True(t, regs[0].Normalize, "normalize should be true in the CreateSchema request")
}

func TestExtractFingerprint(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		meta := map[string]any{"fingerprint": "abc123", "type": "OBJECT"}
		fp, err := extractFingerprint(meta)
		require.NoError(t, err)
		assert.Equal(t, "abc123", fp)
	})

	t.Run("not a map", func(t *testing.T) {
		_, err := extractFingerprint("not a map")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map[string]any")
	})

	t.Run("missing fingerprint", func(t *testing.T) {
		meta := map[string]any{"type": "OBJECT"}
		_, err := extractFingerprint(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing or invalid fingerprint")
	})

	t.Run("fingerprint wrong type", func(t *testing.T) {
		meta := map[string]any{"fingerprint": 12345}
		_, err := extractFingerprint(meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing or invalid fingerprint")
	})
}

func TestSchemaRegistryEncodeMetadataPurgeStale(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Encode a message to populate the metaEncoders cache.
	schemaMeta := makeCommonSchemaMeta(t, schema.Common{Name: "x", Type: schema.Int32})
	msg := service.NewMessage([]byte(`{"x":1}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.NoError(t, outBatches[0][0].GetError())

	// Verify cache has an entry.
	encoder.metaCacheMut.RLock()
	assert.Len(t, encoder.metaEncoders, 1)
	encoder.metaCacheMut.RUnlock()

	// Manually set lastUsedUnixSeconds to a stale time.
	tStale := time.Now().Add(-time.Hour).Unix()
	encoder.metaCacheMut.Lock()
	for k, v := range encoder.metaEncoders {
		v.lastUsedUnixSeconds = tStale
		encoder.metaEncoders[k] = v
	}
	encoder.metaCacheMut.Unlock()

	// Run purge.
	encoder.purgeStaleMetaEncoders()

	// Cache should now be empty.
	encoder.metaCacheMut.RLock()
	assert.Empty(t, encoder.metaEncoders, "stale entries should be purged")
	encoder.metaCacheMut.RUnlock()
}

func TestSchemaRegistryEncodeMetadataConcurrent(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: test-subject
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "x", Type: schema.Int32},
	)

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 50 {
				msg := service.NewMessage([]byte(`{"x":42}`))
				msg.MetaSetMut("schema", schemaMeta)

				outBatches, bErr := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
				if bErr != nil {
					t.Errorf("ProcessBatch error: %v", bErr)
					return
				}
				if msgErr := outBatches[0][0].GetError(); msgErr != nil {
					t.Errorf("message error: %v", msgErr)
					return
				}

				b, bErr := outBatches[0][0].AsBytes()
				if bErr != nil {
					t.Errorf("AsBytes error: %v", bErr)
					return
				}
				if len(b) <= 5 {
					t.Errorf("output too short: %d bytes", len(b))
					return
				}
			}
		})
	}
	wg.Wait()

	// Despite 500 total calls, schema should only be registered once.
	assert.Equal(t, 1, mockState.getCalls()["test-subject"])
}

func TestSchemaRegistryEncodeMetadataAvroTimestamp(t *testing.T) {
	urlStr, mockState := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: products-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	// Simulate the exact schema a CDC source would produce for a table with
	// a TIMESTAMPTZ column.
	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "id", Type: schema.Int32},
		schema.Common{Name: "name", Type: schema.String},
		schema.Common{Name: "price", Type: schema.String},
		schema.Common{Name: "in_stock", Type: schema.Boolean},
		schema.Common{Name: "created_at", Type: schema.Timestamp, Optional: true},
	)

	msg := service.NewMessage([]byte(`{"id":79,"name":"budget gadget","price":"79.06","in_stock":true,"created_at":"2026-03-19T10:05:09.934345Z"}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	require.NoError(t, outBatches[0][0].GetError(), "encoding a CDC message with a timestamp field should succeed")

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	// Verify Confluent wire format header.
	require.Greater(t, len(b), 5, "output must have wire header")
	assert.Equal(t, byte(0x00), b[0], "magic byte")
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(1), schemaID)
	assert.Equal(t, 1, mockState.getCalls()["products-value"])
}

// TestSchemaRegistryEncodeMetadataAvroAllTypes exercises every schema.Common
// type through the full ProcessBatch → newAvroEncoder path, verifying that the
// encoder produces valid Avro binary that can be decoded back to the original
// values.
func TestSchemaRegistryEncodeMetadataAvroAllTypes(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: all-types-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "b", Type: schema.Boolean},
		schema.Common{Name: "i32", Type: schema.Int32},
		schema.Common{Name: "i64", Type: schema.Int64},
		schema.Common{Name: "f32", Type: schema.Float32},
		schema.Common{Name: "f64", Type: schema.Float64},
		schema.Common{Name: "s", Type: schema.String},
		schema.Common{Name: "blob", Type: schema.ByteArray},
		schema.Common{Name: "ts", Type: schema.Timestamp},
		schema.Common{Name: "opt_s", Type: schema.String, Optional: true},
		schema.Common{Name: "opt_null", Type: schema.String, Optional: true},
		schema.Common{Name: "opt_ts", Type: schema.Timestamp, Optional: true},
		schema.Common{Name: "arr", Type: schema.Array, Children: []schema.Common{
			{Type: schema.Int32},
		}},
		schema.Common{Name: "m", Type: schema.Map, Children: []schema.Common{
			{Type: schema.String},
		}},
		schema.Common{Name: "nested", Type: schema.Object, Children: []schema.Common{
			{Name: "x", Type: schema.Int32},
			{Name: "y", Type: schema.String},
		}},
	)

	// Use SetStructuredMut to simulate CDC source providing native Go types.
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any{
		"b":        true,
		"i32":      int64(42),
		"i64":      int64(9876543210),
		"f32":      float64(1.5),
		"f64":      float64(3.141592653589793),
		"s":        "hello",
		"blob":     "binary-data",
		"ts":       "2026-03-19T10:05:09.934345Z",
		"opt_s":    "present",
		"opt_null": nil,
		"opt_ts":   "2026-03-19T12:00:00Z",
		"arr":      []any{float64(1), float64(2), float64(3)},
		"m":        map[string]any{"env": "prod", "region": "us"},
		"nested":   map[string]any{"x": float64(7), "y": "inner"},
	})
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	require.NoError(t, outBatches[0][0].GetError(), "encoding all types should succeed")

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	// Verify Confluent wire format header.
	require.Greater(t, len(b), 5, "output must have wire header")
	assert.Equal(t, byte(0x00), b[0], "magic byte")
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(1), schemaID)

	// Decode back and verify values survived the round-trip.
	registeredSchema := outBatches[0][0]
	cfg := decodingConfig{}
	cfg.avro.rawUnions = true
	decoder, err := newSchemaRegistryDecoder(urlStr, noopReqSign, nil, cfg, schemaStaleAfter, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = decoder.Close(t.Context()) }()

	decodedMsgs, err := decoder.Process(t.Context(), registeredSchema)
	require.NoError(t, err)
	require.Len(t, decodedMsgs, 1)
	require.NoError(t, decodedMsgs[0].GetError())

	// The decoder returns JSON text, so we re-parse to verify values
	// round-tripped correctly.
	decodedBytes, err := decodedMsgs[0].AsBytes()
	require.NoError(t, err)

	var dm map[string]any
	require.NoError(t, json.Unmarshal(decodedBytes, &dm))

	assert.Equal(t, true, dm["b"])
	assert.EqualValues(t, 42, dm["i32"])
	assert.EqualValues(t, 9876543210, dm["i64"])
	assert.InDelta(t, 1.5, dm["f32"], 0.01)
	assert.InDelta(t, 3.141592653589793, dm["f64"], 0.0001)
	assert.Equal(t, "hello", dm["s"])
	assert.Equal(t, "binary-data", dm["blob"])

	// Verify timestamp values, not just non-nil.
	// raw_json decodes timestamp-millis as epoch millis in JSON.
	tsVal, ok := dm["ts"].(float64)
	require.True(t, ok, "ts should be a number, got %T", dm["ts"])
	expectedTsMillis, _ := time.Parse(time.RFC3339Nano, "2026-03-19T10:05:09.934345Z")
	assert.Equal(t, expectedTsMillis.UnixMilli(), int64(tsVal))

	assert.Equal(t, "present", dm["opt_s"])
	assert.Nil(t, dm["opt_null"])

	optTsVal, ok := dm["opt_ts"].(float64)
	require.True(t, ok, "opt_ts should be a number, got %T", dm["opt_ts"])
	expectedOptTs, _ := time.Parse(time.RFC3339Nano, "2026-03-19T12:00:00Z")
	assert.Equal(t, expectedOptTs.UnixMilli(), int64(optTsVal))

	arr, ok := dm["arr"].([]any)
	require.True(t, ok)
	require.Len(t, arr, 3)
	assert.EqualValues(t, 1, arr[0])
	assert.EqualValues(t, 2, arr[1])
	assert.EqualValues(t, 3, arr[2])

	m, ok := dm["m"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", m["env"])
	assert.Equal(t, "us", m["region"])

	nested, ok := dm["nested"].(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 7, nested["x"])
	assert.Equal(t, "inner", nested["y"])
}

// TestSchemaRegistryEncodeMetadataAvroAllTypesFromJSON is the same as
// TestSchemaRegistryEncodeMetadataAvroAllTypes but uses JSON bytes instead of
// SetStructuredMut, simulating the path where messages arrive as JSON text
// (all numbers as float64, timestamps as strings).
func TestSchemaRegistryEncodeMetadataAvroAllTypesFromJSON(t *testing.T) {
	urlStr, _ := runMetaMockRegistry(t)

	spec := schemaRegistryEncoderConfig()
	conf, err := spec.ParseYAML(fmt.Sprintf(`
url: %s
subject: all-types-json-value
schema_metadata: schema
format: avro
avro:
  raw_json: true
`, urlStr), service.NewEnvironment())
	require.NoError(t, err)

	encoder, err := newSchemaRegistryEncoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer func() { _ = encoder.Close(t.Context()) }()

	schemaMeta := makeCommonSchemaMeta(t,
		schema.Common{Name: "b", Type: schema.Boolean},
		schema.Common{Name: "i32", Type: schema.Int32},
		schema.Common{Name: "i64", Type: schema.Int64},
		schema.Common{Name: "f32", Type: schema.Float32},
		schema.Common{Name: "f64", Type: schema.Float64},
		schema.Common{Name: "s", Type: schema.String},
		schema.Common{Name: "ts", Type: schema.Timestamp},
		schema.Common{Name: "opt_ts", Type: schema.Timestamp, Optional: true},
		schema.Common{Name: "arr", Type: schema.Array, Children: []schema.Common{
			{Type: schema.Int32},
		}},
		schema.Common{Name: "m", Type: schema.Map, Children: []schema.Common{
			{Type: schema.String},
		}},
	)

	msg := service.NewMessage([]byte(`{
		"b": true,
		"i32": 42,
		"i64": 9876543210,
		"f32": 1.5,
		"f64": 3.141592653589793,
		"s": "hello",
		"ts": "2026-03-19T10:05:09.934345Z",
		"opt_ts": "2026-03-19T12:00:00Z",
		"arr": [1, 2, 3],
		"m": {"env": "prod"}
	}`))
	msg.MetaSetMut("schema", schemaMeta)

	outBatches, err := encoder.ProcessBatch(t.Context(), service.MessageBatch{msg})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)
	require.NoError(t, outBatches[0][0].GetError(), "encoding all types from JSON should succeed")

	b, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)
	require.Greater(t, len(b), 5, "output must have wire header")
	assert.Equal(t, byte(0x00), b[0], "magic byte")
}
