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

package confluent

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/redpanda-data/benthos/v4/public/service"

	pq "github.com/redpanda-data/connect/v4/internal/impl/parquet"
	"github.com/redpanda-data/connect/v4/internal/impl/protobuf/common"
)

// These tests run the same batch through a parquet_encode_protobuf processor
// and through a schema_registry_decode processor followed by a parquet_encode
// processor, both built from config and executed by the framework, and
// require the same outcome: the same rows, the same metadata, the same
// failures.

const directProtoFlat = `
syntax = "proto3";
package directtest;

enum Kind {
  KIND_UNSET = 0;
  KIND_BID = 1;
  KIND_NO_BID = 2;
}

message Impression {
  int64 id = 1;
  string publisher = 2;
  int32 width = 3;
  bool viewable = 4;
  float price = 5;
  repeated int32 deal_ids = 6;
  repeated string tags = 7;
  Kind kind = 8;
  optional int32 floor = 9;
}

message Click {
  int64 id = 1;
  string publisher = 2;
}
`

// directProtoFlatV2 adds a field to Impression.
const directProtoFlatV2 = `
syntax = "proto3";
package directtest;

enum Kind {
  KIND_UNSET = 0;
  KIND_BID = 1;
  KIND_NO_BID = 2;
}

message Impression {
  int64 id = 1;
  string publisher = 2;
  int32 width = 3;
  bool viewable = 4;
  float price = 5;
  repeated int32 deal_ids = 6;
  repeated string tags = 7;
  Kind kind = 8;
  optional int32 floor = 9;
  string region = 10;
}
`

const directProtoNested = `
syntax = "proto3";
package directtest;

message Device {
  string os = 1;
}

message Impression {
  int64 id = 1;
  string publisher = 2;
  Device device = 3;
}
`

const directAvroSchema = `{
	"type": "record",
	"name": "impression",
	"fields": [
		{ "name": "id", "type": "long" },
		{ "name": "publisher", "type": "string" }
	]
}`

const directParquetSchema = `
schema:
  - { name: id, type: INT64 }
  - { name: publisher, type: UTF8, optional: true }
  - { name: width, type: INT32, optional: true }
  - { name: viewable, type: BOOLEAN, optional: true }
  - { name: price, type: FLOAT, optional: true }
  - { name: deal_ids, type: INT32, repeated: true }
  - { name: tags, type: UTF8, repeated: true }
  - { name: kind, type: UTF8, optional: true }
  - { name: floor, type: INT32, optional: true }
  - { name: region, type: UTF8, optional: true }
default_compression: zstd
`

func directTestRegistry(t *testing.T) string {
	t.Helper()
	schemas := map[string]map[string]any{
		"/schemas/ids/1": {"schemaType": "PROTOBUF", "schema": directProtoFlat},
		"/schemas/ids/2": {"schemaType": "PROTOBUF", "schema": directProtoFlatV2},
		"/schemas/ids/3": {"schemaType": "PROTOBUF", "schema": directProtoNested},
		"/schemas/ids/4": {"schema": directAvroSchema},
	}
	return runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		if s, ok := schemas[path]; ok {
			return mustJBytes(t, s), nil
		}
		return nil, nil
	})
}

func directTestMessageDescriptor(t *testing.T, schema, name string) protoreflect.MessageDescriptor {
	t.Helper()
	files, _, err := common.RegistriesFromMap(map[string]string{".": schema})
	require.NoError(t, err)
	fd, err := files.FindFileByPath(".")
	require.NoError(t, err)
	md := fd.Messages().ByName(protoreflect.Name(name))
	require.NotNil(t, md)
	return md
}

// directFrame prepends the confluent wire header for the schema ID and
// message index path.
func directFrame(id int, indexes []int, payload []byte) []byte {
	b := []byte{0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(b[1:], uint32(id))
	if len(indexes) == 1 && indexes[0] == 0 {
		b = append(b, 0)
	} else {
		b = binary.AppendVarint(b, int64(len(indexes)))
		for _, idx := range indexes {
			b = binary.AppendVarint(b, int64(idx))
		}
	}
	return append(b, payload...)
}

func directImpression(t *testing.T, md protoreflect.MessageDescriptor, i int) []byte {
	t.Helper()
	fds := md.Fields()
	msg := dynamicpb.NewMessage(md)
	msg.Set(fds.ByName("id"), protoreflect.ValueOfInt64(int64(1000+i)))
	if i%3 != 0 {
		msg.Set(fds.ByName("publisher"), protoreflect.ValueOfString(fmt.Sprintf("publisher-%d", i%5)))
	}
	if fd := fds.ByName("price"); fd != nil {
		if i%2 == 0 {
			msg.Set(fds.ByName("width"), protoreflect.ValueOfInt32(int32(300+i)))
			msg.Set(fds.ByName("viewable"), protoreflect.ValueOfBool(true))
		}
		msg.Set(fd, protoreflect.ValueOfFloat32(float32(i)+0.25))
		deals := msg.Mutable(fds.ByName("deal_ids")).List()
		for j := range i % 4 {
			deals.Append(protoreflect.ValueOfInt32(int32(j * 7)))
		}
		tags := msg.Mutable(fds.ByName("tags")).List()
		for j := range i % 3 {
			tags.Append(protoreflect.ValueOfString(strings.Repeat("t", j)))
		}
		msg.Set(fds.ByName("kind"), protoreflect.ValueOfEnum(protoreflect.EnumNumber(1+i%2)))
		if i%4 == 0 {
			msg.Set(fds.ByName("floor"), protoreflect.ValueOfInt32(0))
		}
	}
	if fd := fds.ByName("region"); fd != nil && i%2 == 1 {
		msg.Set(fd, protoreflect.ValueOfString("us-east"))
	}
	b, err := proto.Marshal(msg)
	require.NoError(t, err)
	return b
}

func directTestBatch(payloads ...[]byte) service.MessageBatch {
	batch := make(service.MessageBatch, len(payloads))
	for i, p := range payloads {
		m := service.NewMessage(p)
		m.MetaSetMut("kafka_topic", "impressions")
		m.MetaSetMut("kafka_offset", i)
		batch[i] = m
	}
	return batch
}

func directTestProcessors(t *testing.T, conf string) []*service.OwnedProcessor {
	t.Helper()
	spec := service.NewConfigSpec().Field(service.NewProcessorListField("processors"))
	parsed, err := spec.ParseYAML(conf, nil)
	require.NoError(t, err)
	procs, err := parsed.FieldProcessorList("processors")
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, p := range procs {
			_ = p.Close(t.Context())
		}
	})
	return procs
}

func indent(s, prefix string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n")
}

type directTestPipelines struct {
	combined  []*service.OwnedProcessor
	separate  []*service.OwnedProcessor
	processor *parquetEncodeProtobufProcessor
}

// newDirectTestPipelines builds the combined processor, the two separate
// processors it stands for, and a bare combined processor used to observe
// which path a batch takes.
func newDirectTestPipelines(t *testing.T, url, decodeConf, encodeConf string) directTestPipelines {
	t.Helper()
	decodeConf = "url: " + url + "\n" + strings.TrimSpace(decodeConf)
	both := decodeConf + "\n" + strings.TrimSpace(encodeConf)

	conf, err := parquetEncodeProtobufConfig().ParseYAML(both, nil)
	require.NoError(t, err)
	decoder, err := newSchemaRegistryDecoderFromConfig(conf, service.MockResources())
	require.NoError(t, err)
	encoder, err := pq.NewEncodeProcessorFromConfig(conf, nil)
	require.NoError(t, err)
	bare := newParquetEncodeProtobufProcessor(decoder, encoder, service.MockResources())
	t.Cleanup(func() { _ = bare.Close(t.Context()) })

	return directTestPipelines{
		combined: directTestProcessors(t, "processors:\n  - parquet_encode_protobuf:\n"+indent(both, "      ")),
		separate: directTestProcessors(t, "processors:\n  - schema_registry_decode:\n"+indent(decodeConf, "      ")+
			"\n  - parquet_encode:\n"+indent(encodeConf, "      ")),
		processor: bare,
	}
}

func readParquetRows(t *testing.T, file []byte) []parquet.Row {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(file), int64(len(file)))
	require.NoError(t, err)
	var out []parquet.Row
	for _, rg := range f.RowGroups() {
		rr := rg.Rows()
		for {
			buf := make([]parquet.Row, 256)
			n, err := rr.ReadRows(buf)
			for _, r := range buf[:n] {
				out = append(out, r.Clone())
			}
			if err != nil {
				break
			}
		}
		require.NoError(t, rr.Close())
	}
	return out
}

func requireSameParquet(t *testing.T, want, got []byte) {
	t.Helper()
	wantRows, gotRows := readParquetRows(t, want), readParquetRows(t, got)
	require.Len(t, gotRows, len(wantRows))
	for i := range wantRows {
		require.Len(t, gotRows[i], len(wantRows[i]), "row %d", i)
		for j, w := range wantRows[i] {
			g := gotRows[i][j]
			same := w.Kind() == g.Kind() && w.IsNull() == g.IsNull() && w.Column() == g.Column() &&
				w.RepetitionLevel() == g.RepetitionLevel() && w.DefinitionLevel() == g.DefinitionLevel() &&
				bytes.Equal(w.AppendBytes(nil), g.AppendBytes(nil))
			require.Truef(t, same, "row %d value %d: want %+v, got %+v", i, j, w, g)
		}
	}
}

// requireSameOutcome runs the payloads through both pipelines and compares
// everything observable about the result. It returns the number of rows
// written when the batch was encoded, or -1 when it was not.
func (p directTestPipelines) requireSameOutcome(t *testing.T, payloads ...[]byte) int {
	t.Helper()
	want, wantErr := service.ExecuteProcessors(t.Context(), p.separate, directTestBatch(payloads...))
	got, gotErr := service.ExecuteProcessors(t.Context(), p.combined, directTestBatch(payloads...))
	require.Equal(t, wantErr == nil, gotErr == nil, "want %v, got %v", wantErr, gotErr)
	require.Len(t, got, len(want))

	rows := -1
	for i := range want {
		require.Len(t, got[i], len(want[i]), "batch %d", i)
		for j, w := range want[i] {
			g := got[i][j]

			wErr, gErr := w.GetError(), g.GetError()
			require.Equal(t, wErr == nil, gErr == nil, "message %d: want error %v, got %v", j, wErr, gErr)
			if wErr != nil {
				assert.Equal(t, wErr.Error(), gErr.Error(), "message %d", j)
			}

			wMeta, gMeta := map[string]any{}, map[string]any{}
			require.NoError(t, w.MetaWalkMut(func(k string, v any) error { wMeta[k] = v; return nil }))
			require.NoError(t, g.MetaWalkMut(func(k string, v any) error { gMeta[k] = v; return nil }))
			assert.Equal(t, wMeta, gMeta, "message %d metadata", j)

			if wErr != nil {
				wStruct, wStructErr := w.AsStructured()
				gStruct, gStructErr := g.AsStructured()
				if wStructErr == nil || gStructErr == nil {
					assert.Equal(t, wStruct, gStruct, "message %d contents", j)
					continue
				}
				wBytes, err := w.AsBytes()
				require.NoError(t, err)
				gBytes, err := g.AsBytes()
				require.NoError(t, err)
				assert.Equal(t, wBytes, gBytes, "message %d contents", j)
				continue
			}

			wBytes, err := w.AsBytes()
			require.NoError(t, err)
			gBytes, err := g.AsBytes()
			require.NoError(t, err)
			requireSameParquet(t, wBytes, gBytes)
			rows = len(readParquetRows(t, gBytes))
		}
	}
	return rows
}

func (p directTestPipelines) tookDirectPath(t *testing.T, payloads ...[]byte) bool {
	t.Helper()
	_, direct, err := p.processor.processBatch(t.Context(), directTestBatch(payloads...))
	require.NoError(t, err)
	return direct
}

const directDecodeConf = `
protobuf:
  use_proto_names: true
  serialize_to_json: false
`

func TestParquetEncodeProtobufMatchesSeparateProcessors(t *testing.T) {
	url := directTestRegistry(t)
	impression := directTestMessageDescriptor(t, directProtoFlat, "Impression")
	impressionV2 := directTestMessageDescriptor(t, directProtoFlatV2, "Impression")
	click := directTestMessageDescriptor(t, directProtoFlat, "Click")
	nested := directTestMessageDescriptor(t, directProtoNested, "Impression")

	var flat, flatV2, clicks, nestedMsgs [][]byte
	for i := range 50 {
		flat = append(flat, directFrame(1, []int{0}, directImpression(t, impression, i)))
		flatV2 = append(flatV2, directFrame(2, []int{0}, directImpression(t, impressionV2, i)))
		clicks = append(clicks, directFrame(1, []int{1}, directImpression(t, click, i)))
		nestedMsgs = append(nestedMsgs, directFrame(3, []int{1}, directImpression(t, nested, i)))
	}
	mixed := append(append([][]byte{}, flat[:10]...), flatV2[:10]...)
	mixed = append(mixed, flat[10:20]...)

	explicitZero := directFrame(1, []int{0}, protowire.AppendVarint(protowire.AppendTag(nil, 3, protowire.VarintType), 0))
	unknownField := directFrame(1, []int{0}, protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 5))
	malformed := directFrame(1, []int{0}, []byte{0x08})
	badIndex := directFrame(1, []int{7}, directImpression(t, impression, 1))
	avroMsg := directFrame(4, nil, nil)[:5]
	avroMsg = append(avroMsg, 0x02, 0x06, 'a', 'b', 'c')

	tests := []struct {
		name       string
		decodeConf string
		encodeConf string
		payloads   [][]byte
		direct     bool
		rows       int
	}{
		{"flat protobuf", directDecodeConf, directParquetSchema, flat, true, 50},
		{"single message", directDecodeConf, directParquetSchema, flat[:1], true, 1},
		{"second message type of the schema", directDecodeConf, directParquetSchema, clicks, true, 50},
		{"two schema versions in one batch", directDecodeConf, directParquetSchema, mixed, true, 30},
		{"two message types in one batch", directDecodeConf, directParquetSchema, append(append([][]byte{}, flat[:5]...), clicks[:5]...), true, 10},
		{"enum numbers", "protobuf:\n  use_proto_names: true\n  use_enum_numbers: true\n  serialize_to_json: false", strings.Replace(directParquetSchema, "name: kind, type: UTF8", "name: kind, type: INT32", 1), flat, true, 50},
		{"emit unpopulated", "protobuf:\n  use_proto_names: true\n  emit_unpopulated: true\n  serialize_to_json: false", directParquetSchema, flat, true, 50},
		{"emit default values", "protobuf:\n  use_proto_names: true\n  emit_default_values: true\n  serialize_to_json: false", directParquetSchema, flat, true, 50},
		{"json names", "protobuf:\n  serialize_to_json: false", strings.Replace(directParquetSchema, "name: deal_ids", "name: dealIds", 1), flat, true, 50},
		{"plain encoding and snappy", directDecodeConf, strings.Replace(directParquetSchema, "default_compression: zstd", "default_compression: snappy\ndefault_encoding: PLAIN", 1), flat, true, 50},

		{"nested message schema", directDecodeConf, directParquetSchema, nestedMsgs, false, 50},
		{"explicit zero value on the wire", directDecodeConf, directParquetSchema, [][]byte{flat[0], explicitZero, flat[1]}, false, 3},
		{"unknown field on the wire", directDecodeConf, directParquetSchema, [][]byte{flat[0], unknownField, flat[1]}, false, 3},
		{"eligible and nested schemas in one batch", directDecodeConf, directParquetSchema, append(append([][]byte{}, flat[:5]...), nestedMsgs[:5]...), false, 10},
		{"column type the direct path does not cover", directDecodeConf, strings.Replace(directParquetSchema, "name: width, type: INT32", "name: width, type: DOUBLE", 1), flat, false, 50},
		{"avro", directDecodeConf, "schema:\n  - { name: id, type: INT64 }\n  - { name: publisher, type: UTF8 }", [][]byte{avroMsg, avroMsg}, false, 2},

		{"malformed protobuf", directDecodeConf, directParquetSchema, [][]byte{flat[0], malformed, flat[1]}, false, -1},
		{"message index out of range", directDecodeConf, directParquetSchema, [][]byte{flat[0], badIndex}, false, -1},
		{"missing header", directDecodeConf, directParquetSchema, [][]byte{flat[0], []byte("not framed")}, false, -1},
		{"empty message", directDecodeConf, directParquetSchema, [][]byte{flat[0], {}}, false, -1},
		{"unknown schema id", directDecodeConf, directParquetSchema, [][]byte{directFrame(77, []int{0}, nil)}, false, -1},
		{"every message malformed", directDecodeConf, directParquetSchema, [][]byte{malformed, malformed}, false, -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := newDirectTestPipelines(t, url, test.decodeConf, test.encodeConf)
			rows := p.requireSameOutcome(t, test.payloads...)
			assert.Equal(t, test.rows, rows)
			assert.Equal(t, test.direct, p.tookDirectPath(t, test.payloads...))
			// A second batch takes the same path and still agrees.
			p.requireSameOutcome(t, test.payloads...)
			assert.Equal(t, test.direct, p.tookDirectPath(t, test.payloads...))
		})
	}
}

// TestParquetEncodeProtobufSerializeToJSON covers the default of
// serialize_to_json, with which the direct path is never taken.
func TestParquetEncodeProtobufSerializeToJSON(t *testing.T) {
	url := directTestRegistry(t)
	click := directTestMessageDescriptor(t, directProtoFlat, "Click")
	var payloads [][]byte
	for i := range 10 {
		payloads = append(payloads, directFrame(1, []int{1}, directImpression(t, click, i)))
	}
	p := newDirectTestPipelines(t, url, "protobuf:\n  use_proto_names: true", "schema:\n  - { name: id, type: UTF8 }\n  - { name: publisher, type: UTF8, optional: true }")
	assert.Equal(t, 10, p.requireSameOutcome(t, payloads...))
	assert.False(t, p.tookDirectPath(t, payloads...))
}

// TestParquetEncodeProtobufLeavesInputUntouched checks that a batch refused
// half way through the direct path reaches the regular path unmodified.
func TestParquetEncodeProtobufLeavesInputUntouched(t *testing.T) {
	url := directTestRegistry(t)
	impression := directTestMessageDescriptor(t, directProtoFlat, "Impression")
	var payloads [][]byte
	for i := range 3000 {
		payloads = append(payloads, directFrame(1, []int{0}, directImpression(t, impression, i)))
	}
	// Refused only at the very last message, after chunks were flushed.
	payloads = append(payloads, directFrame(1, []int{0}, protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 5)))
	originals := make([][]byte, len(payloads))
	for i, b := range payloads {
		originals[i] = bytes.Clone(b)
	}

	p := newDirectTestPipelines(t, url, directDecodeConf, directParquetSchema)
	assert.Equal(t, 3001, p.requireSameOutcome(t, payloads...))
	assert.False(t, p.tookDirectPath(t, payloads...))
	assert.Equal(t, originals, payloads)
}
