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

package parquet

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/protobuf/common"
)

// The tests in this file hold the direct protobuf to parquet path to one
// rule: whenever it accepts a batch, the file it writes has exactly the rows
// that schema_registry_decode followed by parquet_encode write for that
// batch. Anything else it must refuse.

type pbTestField struct {
	name     string
	number   int32
	kind     descriptorpb.FieldDescriptorProto_Type
	repeated bool
	// explicit gives a singular field presence tracking (proto3 optional).
	explicit bool
	// typeName is set for enum and message fields.
	typeName string
}

type pqTestColumn struct {
	name     string
	typ      string
	repeated bool
	optional bool
}

const pbTestEnumName = ".directtest.Colour"

// pbTestDescriptor builds a proto3 message named Msg from the fields, in a
// file that also declares an enum and a nested message type.
func pbTestDescriptor(tb testing.TB, fields []pbTestField) protoreflect.MessageDescriptor {
	tb.Helper()
	msg := &descriptorpb.DescriptorProto{Name: new("Msg")}
	for _, f := range fields {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if f.repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		fd := &descriptorpb.FieldDescriptorProto{
			Name:   new(f.name),
			Number: new(f.number),
			Type:   f.kind.Enum(),
			Label:  label.Enum(),
		}
		if f.typeName != "" {
			fd.TypeName = new(f.typeName)
		}
		if f.explicit {
			fd.Proto3Optional = new(true)
			fd.OneofIndex = new(int32(len(msg.OneofDecl)))
			msg.OneofDecl = append(msg.OneofDecl, &descriptorpb.OneofDescriptorProto{Name: new("_" + f.name)})
		}
		msg.Field = append(msg.Field, fd)
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    new("directtest.proto"),
		Package: new("directtest"),
		Syntax:  new("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: new("Colour"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: new("COLOUR_UNSET"), Number: proto.Int32(0)},
				{Name: new("COLOUR_RED"), Number: proto.Int32(1)},
				{Name: new("COLOUR_BLUE"), Number: proto.Int32(2)},
				{Name: new("COLOUR_NEGATIVE"), Number: proto.Int32(-3)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{
			msg,
			{
				Name: new("Child"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:   new("x"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				}},
			},
		},
	}, nil)
	require.NoError(tb, err)
	return file.Messages().Get(0)
}

func pqTestProcessor(tb testing.TB, cols []pqTestColumn, extra string) *parquetEncodeProcessor {
	tb.Helper()
	var conf strings.Builder
	conf.WriteString(extra)
	conf.WriteString("\nschema:\n")
	for _, c := range cols {
		fmt.Fprintf(&conf, "  - { name: %s, type: %s, repeated: %v, optional: %v }\n", c.name, c.typ, c.repeated, c.optional)
	}
	pConf, err := parquetEncodeProcessorConfig().ParseYAML(conf.String(), nil)
	require.NoError(tb, err)
	proc, err := newParquetEncodeProcessorFromConfig(pConf, nil)
	require.NoError(tb, err)
	return proc
}

// regularEncode is what the pipeline does without the direct path:
// schema_registry_decode with serialize_to_json false, then parquet_encode.
func regularEncode(md protoreflect.MessageDescriptor, opts ProtobufDirectOptions, proc *parquetEncodeProcessor, data [][]byte) ([]byte, error) {
	decoder := common.NewHyperPbDecoder(md, common.DefaultProfilingOptions)
	jsonOpts := protojson.MarshalOptions{
		UseProtoNames:     opts.UseProtoNames,
		UseEnumNumbers:    opts.UseEnumNumbers,
		EmitUnpopulated:   opts.EmitUnpopulated,
		EmitDefaultValues: opts.EmitDefaultValues,
	}
	batch := make(service.MessageBatch, len(data))
	for i, b := range data {
		m := service.NewMessage(b)
		if err := decoder.WithDecoded(b, func(msg proto.Message) error {
			return common.ToMessageFast(msg.ProtoReflect(), jsonOpts, m)
		}); err != nil {
			return nil, err
		}
		batch[i] = m
	}
	out, err := proc.ProcessBatch(context.Background(), batch)
	if err != nil {
		return nil, err
	}
	return out[0][0].AsBytes()
}

func directEncode(md protoreflect.MessageDescriptor, opts ProtobufDirectOptions, proc *parquetEncodeProcessor, data [][]byte) (file []byte, planErr, encodeErr error) {
	enc := &EncodeProcessor{p: proc}
	plan, err := enc.NewProtobufPlan(md, opts)
	if err != nil {
		return nil, err, nil
	}
	plans := make([]*ProtobufPlan, len(data))
	for i := range plans {
		plans[i] = plan
	}
	file, err = enc.EncodeProtobuf(plans, data)
	return file, nil, err
}

// diffParquetFiles returns a description of the first difference between the
// rows of two parquet files, or the empty string when they hold the same
// values with the same levels in the same columns.
func diffParquetFiles(tb testing.TB, want, got []byte) string {
	tb.Helper()
	wantRows, gotRows := readAllRows(tb, want), readAllRows(tb, got)
	if len(wantRows) != len(gotRows) {
		return fmt.Sprintf("row count: want %d, got %d", len(wantRows), len(gotRows))
	}
	for i := range wantRows {
		w, g := wantRows[i], gotRows[i]
		if len(w) != len(g) {
			return fmt.Sprintf("row %d value count: want %v, got %v", i, w, g)
		}
		for j := range w {
			same := w[j].Kind() == g[j].Kind() &&
				w[j].IsNull() == g[j].IsNull() &&
				w[j].Column() == g[j].Column() &&
				w[j].RepetitionLevel() == g[j].RepetitionLevel() &&
				w[j].DefinitionLevel() == g[j].DefinitionLevel() &&
				bytes.Equal(w[j].AppendBytes(nil), g[j].AppendBytes(nil))
			if !same {
				return fmt.Sprintf("row %d value %d: want %+v, got %+v", i, j, w[j], g[j])
			}
		}
	}
	return ""
}

type pbTestKind struct {
	name     string
	kind     descriptorpb.FieldDescriptorProto_Type
	typeName string
	zero     protoreflect.Value
	values   []protoreflect.Value
}

func pbTestKinds() []pbTestKind {
	i32 := protoreflect.ValueOfInt32
	i64 := protoreflect.ValueOfInt64
	u32 := protoreflect.ValueOfUint32
	u64 := protoreflect.ValueOfUint64
	f32 := protoreflect.ValueOfFloat32
	f64 := protoreflect.ValueOfFloat64
	str := protoreflect.ValueOfString
	byt := protoreflect.ValueOfBytes
	enum := func(n int32) protoreflect.Value { return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)) }
	return []pbTestKind{
		{"bool", descriptorpb.FieldDescriptorProto_TYPE_BOOL, "", protoreflect.ValueOfBool(false), []protoreflect.Value{protoreflect.ValueOfBool(true)}},
		{"int32", descriptorpb.FieldDescriptorProto_TYPE_INT32, "", i32(0), []protoreflect.Value{i32(1), i32(-1), i32(math.MaxInt32), i32(math.MinInt32), i32(123456)}},
		{"sint32", descriptorpb.FieldDescriptorProto_TYPE_SINT32, "", i32(0), []protoreflect.Value{i32(1), i32(-1), i32(math.MaxInt32), i32(math.MinInt32)}},
		{"sfixed32", descriptorpb.FieldDescriptorProto_TYPE_SFIXED32, "", i32(0), []protoreflect.Value{i32(1), i32(-1), i32(math.MaxInt32), i32(math.MinInt32)}},
		{"uint32", descriptorpb.FieldDescriptorProto_TYPE_UINT32, "", u32(0), []protoreflect.Value{u32(1), u32(math.MaxUint32), u32(1 << 31)}},
		{"fixed32", descriptorpb.FieldDescriptorProto_TYPE_FIXED32, "", u32(0), []protoreflect.Value{u32(1), u32(math.MaxUint32), u32(1 << 31)}},
		{"int64", descriptorpb.FieldDescriptorProto_TYPE_INT64, "", i64(0), []protoreflect.Value{i64(1), i64(-1), i64(math.MaxInt64), i64(math.MinInt64), i64(1 << 40)}},
		{"sint64", descriptorpb.FieldDescriptorProto_TYPE_SINT64, "", i64(0), []protoreflect.Value{i64(1), i64(-1), i64(math.MaxInt64), i64(math.MinInt64)}},
		{"sfixed64", descriptorpb.FieldDescriptorProto_TYPE_SFIXED64, "", i64(0), []protoreflect.Value{i64(1), i64(-1), i64(math.MaxInt64), i64(math.MinInt64)}},
		{"uint64", descriptorpb.FieldDescriptorProto_TYPE_UINT64, "", u64(0), []protoreflect.Value{u64(1), u64(math.MaxUint64), u64(1 << 63)}},
		{"fixed64", descriptorpb.FieldDescriptorProto_TYPE_FIXED64, "", u64(0), []protoreflect.Value{u64(1), u64(math.MaxUint64), u64(1 << 63)}},
		{"float", descriptorpb.FieldDescriptorProto_TYPE_FLOAT, "", f32(0), []protoreflect.Value{f32(1.5), f32(-1.5), f32(math.MaxFloat32), f32(math.SmallestNonzeroFloat32), f32(float32(math.Inf(1))), f32(float32(math.Inf(-1))), f32(float32(math.NaN())), f32(0.1)}},
		{"double", descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, "", f64(0), []protoreflect.Value{f64(1.5), f64(-1.5), f64(math.MaxFloat64), f64(math.SmallestNonzeroFloat64), f64(math.Inf(1)), f64(math.Inf(-1)), f64(math.NaN()), f64(0.1)}},
		{"string", descriptorpb.FieldDescriptorProto_TYPE_STRING, "", str(""), []protoreflect.Value{str("a"), str("héllo ✓ 世界"), str(strings.Repeat("x", 5000)), str("0"), str("null")}},
		{"bytes", descriptorpb.FieldDescriptorProto_TYPE_BYTES, "", byt(nil), []protoreflect.Value{byt([]byte{1}), byt([]byte{0xff, 0xfe, 0x00}), byt([]byte("plain"))}},
		{"enum", descriptorpb.FieldDescriptorProto_TYPE_ENUM, pbTestEnumName, enum(0), []protoreflect.Value{enum(1), enum(2), enum(-3), enum(99)}},
	}
}

func pbMarshal(tb testing.TB, md protoreflect.MessageDescriptor, set func(m *dynamicpb.Message)) []byte {
	tb.Helper()
	m := dynamicpb.NewMessage(md)
	set(m)
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	require.NoError(tb, err)
	return b
}

var directOptionCombos = []ProtobufDirectOptions{
	{},
	{UseProtoNames: true},
	{UseEnumNumbers: true},
	{EmitUnpopulated: true},
	{EmitDefaultValues: true},
	{EmitUnpopulated: true, EmitDefaultValues: true, UseEnumNumbers: true, UseProtoNames: true},
}

// TestProtobufDirectTypeMatrix runs every protobuf scalar kind against every
// parquet column type the processor can be configured with. Wherever the
// direct path accepts the pair, its output must match the regular path.
func TestProtobufDirectTypeMatrix(t *testing.T) {
	pqTypes := []string{"BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "BYTE_ARRAY", "UTF8", "ENUM", "TIMESTAMP", "JSON", "UUID", "BSON"}
	type mode struct {
		name                         string
		repeated, optional, explicit bool
	}
	modes := []mode{
		{"optional-implicit", false, true, false},
		{"optional-explicit", false, true, true},
		{"required-implicit", false, false, false},
		{"required-explicit", false, false, true},
		{"repeated", true, false, false},
	}

	accepted := map[string]bool{}
	for _, k := range pbTestKinds() {
		for _, pqType := range pqTypes {
			for _, m := range modes {
				for oi, opts := range directOptionCombos {
					name := fmt.Sprintf("%s/%s/%s/opts%d", k.name, pqType, m.name, oi)
					t.Run(name, func(t *testing.T) {
						md := pbTestDescriptor(t, []pbTestField{
							{name: "the_value", number: 3, kind: k.kind, repeated: m.repeated, explicit: m.explicit, typeName: k.typeName},
							{name: "unmapped", number: 9, kind: descriptorpb.FieldDescriptorProto_TYPE_STRING},
						})
						colName := "theValue"
						if opts.UseProtoNames {
							colName = "the_value"
						}
						proc := pqTestProcessor(t, []pqTestColumn{
							{name: colName, typ: pqType, repeated: m.repeated, optional: m.optional},
							{name: "no_such_field", typ: "INT64", optional: true},
							{name: "no_such_required", typ: "UTF8"},
							{name: "no_such_list", typ: "DOUBLE", repeated: true},
						}, "")
						fd := md.Fields().ByNumber(3)
						other := md.Fields().ByNumber(9)

						var data [][]byte
						data = append(data, pbMarshal(t, md, func(*dynamicpb.Message) {}))
						data = append(data, pbMarshal(t, md, func(msg *dynamicpb.Message) { msg.Set(other, protoreflect.ValueOfString("ignored")) }))
						if m.repeated {
							lists := [][]protoreflect.Value{{k.values[0]}, k.values, {k.zero, k.values[0], k.zero}, {k.zero}}
							for _, l := range lists {
								data = append(data, pbMarshal(t, md, func(msg *dynamicpb.Message) {
									list := msg.Mutable(fd).List()
									for _, v := range l {
										list.Append(v)
									}
								}))
							}
						} else {
							for _, v := range k.values {
								data = append(data, pbMarshal(t, md, func(msg *dynamicpb.Message) { msg.Set(fd, v) }))
							}
							if m.explicit {
								data = append(data, pbMarshal(t, md, func(msg *dynamicpb.Message) { msg.Set(fd, k.zero) }))
							}
						}

						got, planErr, encErr := directEncode(md, opts, proc, data)
						if planErr != nil {
							return
						}
						accepted[fmt.Sprintf("%s/%s", k.name, pqType)] = true
						require.NoError(t, encErr, "canonical messages must be accepted once a plan exists")

						want, err := regularEncode(md, opts, proc, data)
						require.NoError(t, err, "the direct path accepted a batch the regular path fails on")
						if diff := diffParquetFiles(t, want, got); diff != "" {
							t.Fatalf("direct output differs from regular output: %s", diff)
						}
					})
				}
			}
		}
	}

	// The pairs the direct path is expected to cover. A change here is a
	// change of what the direct path accepts and deserves a close look.
	want := []string{
		"bool/BOOLEAN",
		"int32/INT32", "int32/INT64", "sint32/INT32", "sint32/INT64", "sfixed32/INT32", "sfixed32/INT64",
		"uint32/INT64", "fixed32/INT64",
		"int64/INT64", "sint64/INT64", "sfixed64/INT64",
		"float/FLOAT", "float/DOUBLE", "double/DOUBLE",
		"string/BYTE_ARRAY", "string/UTF8", "string/ENUM",
		"bytes/BYTE_ARRAY",
		"enum/BYTE_ARRAY", "enum/UTF8", "enum/ENUM", "enum/INT32", "enum/INT64",
	}
	var got []string
	for k := range accepted {
		got = append(got, k)
	}
	assert.ElementsMatch(t, want, got)
}

// TestProtobufDirectRefusesSchemas lists schema shapes that must always go
// through the regular path.
func TestProtobufDirectRefusesSchemas(t *testing.T) {
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	i64 := descriptorpb.FieldDescriptorProto_TYPE_INT64
	msgT := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	flat := []pbTestField{{name: "a", number: 1, kind: i64}, {name: "b", number: 2, kind: str}}
	flatCols := []pqTestColumn{{name: "a", typ: "INT64", optional: true}, {name: "b", typ: "UTF8", optional: true}}

	tests := []struct {
		name     string
		fields   []pbTestField
		cols     []pqTestColumn
		extra    string
		contains string
	}{
		{"nested message", append([]pbTestField{{name: "child", number: 5, kind: msgT, typeName: ".directtest.Child"}}, flat...), flatCols, "", "nested message"},
		{"repeated nested message", append([]pbTestField{{name: "children", number: 5, kind: msgT, typeName: ".directtest.Child", repeated: true}}, flat...), flatCols, "", "nested message"},
		{"list into scalar column", []pbTestField{{name: "a", number: 1, kind: i64, repeated: true}}, flatCols, "", "disagree on being repeated"},
		{"scalar into list column", flat, []pqTestColumn{{name: "a", typ: "INT64", repeated: true}}, "", "disagree on being repeated"},
		{"narrowing", flat, []pqTestColumn{{name: "a", typ: "INT32", optional: true}}, "", "cannot be written directly"},
		{"string into number", flat, []pqTestColumn{{name: "b", typ: "INT64", optional: true}}, "", "cannot be written directly"},
		{"timestamp column", flat, []pqTestColumn{{name: "a", typ: "TIMESTAMP", optional: true}}, "", "unsupported logical type"},
		{"huge field number", []pbTestField{{name: "a", number: 1 << 20, kind: i64}}, flatCols, "", "above the supported maximum"},
		{"schema from metadata", flat, flatCols, "schema_metadata: foo", "taken from metadata"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			md := pbTestDescriptor(t, test.fields)
			proc := pqTestProcessor(t, test.cols, test.extra)
			_, err := (&EncodeProcessor{p: proc}).NewProtobufPlan(md, ProtobufDirectOptions{})
			require.ErrorContains(t, err, test.contains)
		})
	}

	t.Run("group column", func(t *testing.T) {
		pConf, err := parquetEncodeProcessorConfig().ParseYAML(`
schema:
  - name: a
    type: INT64
  - name: nested
    fields:
      - { name: x, type: INT64 }
`, nil)
		require.NoError(t, err)
		proc, err := newParquetEncodeProcessorFromConfig(pConf, nil)
		require.NoError(t, err)
		_, err = (&EncodeProcessor{p: proc}).NewProtobufPlan(pbTestDescriptor(t, flat), ProtobufDirectOptions{})
		require.ErrorContains(t, err, "is a group")
	})
}

// directTestSchema is a flat message exercising every supported kind, used
// by the tests that feed the direct path unusual wire data.
func directTestSchema(tb testing.TB) (protoreflect.MessageDescriptor, *parquetEncodeProcessor) {
	T := func(s string) descriptorpb.FieldDescriptorProto_Type {
		return descriptorpb.FieldDescriptorProto_Type(descriptorpb.FieldDescriptorProto_Type_value["TYPE_"+strings.ToUpper(s)])
	}
	md := pbTestDescriptor(tb, []pbTestField{
		{name: "i32", number: 1, kind: T("int32")},
		{name: "i64", number: 2, kind: T("int64")},
		{name: "s", number: 3, kind: T("string")},
		{name: "b", number: 4, kind: T("bool")},
		{name: "f", number: 5, kind: T("float")},
		{name: "d", number: 6, kind: T("double")},
		{name: "si", number: 7, kind: T("sint32")},
		{name: "u", number: 8, kind: T("uint32")},
		{name: "e", number: 9, kind: T("enum"), typeName: pbTestEnumName},
		{name: "raw", number: 10, kind: T("bytes")},
		{name: "oi", number: 11, kind: T("int32"), explicit: true},
		{name: "os", number: 12, kind: T("string"), explicit: true},
		{name: "ri", number: 13, kind: T("int32"), repeated: true},
		{name: "rs", number: 14, kind: T("string"), repeated: true},
		{name: "rd", number: 15, kind: T("double"), repeated: true},
		{name: "rf", number: 16, kind: T("fixed32"), repeated: true},
		{name: "nocol", number: 17, kind: T("string")},
		{name: "rsi", number: 18, kind: T("sint64"), repeated: true},
		{name: "rb", number: 19, kind: T("bool"), repeated: true},
		{name: "re", number: 20, kind: T("enum"), typeName: pbTestEnumName, repeated: true},
	})
	proc := pqTestProcessor(tb, []pqTestColumn{
		{name: "i32", typ: "INT32", optional: true},
		{name: "i64", typ: "INT64"},
		{name: "s", typ: "UTF8", optional: true},
		{name: "b", typ: "BOOLEAN", optional: true},
		{name: "f", typ: "FLOAT", optional: true},
		{name: "d", typ: "DOUBLE"},
		{name: "si", typ: "INT64", optional: true},
		{name: "u", typ: "INT64", optional: true},
		{name: "e", typ: "UTF8", optional: true},
		{name: "raw", typ: "BYTE_ARRAY", optional: true},
		{name: "oi", typ: "INT32", optional: true},
		{name: "os", typ: "UTF8"},
		{name: "ri", typ: "INT32", repeated: true},
		{name: "rs", typ: "UTF8", repeated: true},
		{name: "rd", typ: "DOUBLE", repeated: true},
		{name: "rf", typ: "INT64", repeated: true},
		{name: "rsi", typ: "INT64", repeated: true},
		{name: "rb", typ: "BOOLEAN", repeated: true},
		{name: "re", typ: "UTF8", repeated: true},
		{name: "extra", typ: "UTF8", optional: true},
	}, "default_compression: zstd")
	return md, proc
}

// requireDirectSound asserts the rule of this file for one batch: if the
// direct path accepts it, the regular path accepts it too and writes the same
// rows. It reports whether the direct path accepted the batch.
func requireDirectSound(tb testing.TB, md protoreflect.MessageDescriptor, opts ProtobufDirectOptions, proc *parquetEncodeProcessor, data [][]byte) bool {
	tb.Helper()
	got, planErr, encErr := directEncode(md, opts, proc, data)
	require.NoError(tb, planErr)
	if encErr != nil {
		require.ErrorIs(tb, encErr, errDirectRefused)
		return false
	}
	want, err := regularEncode(md, opts, proc, data)
	require.NoError(tb, err, "the direct path accepted a batch the regular path fails on")
	if diff := diffParquetFiles(tb, want, got); diff != "" {
		tb.Fatalf("direct output differs from regular output: %s", diff)
	}
	return true
}

func wireTag(num int, typ protowire.Type) []byte {
	return protowire.AppendTag(nil, protowire.Number(num), typ)
}

func wireVarint(num int, v uint64) []byte {
	return protowire.AppendVarint(wireTag(num, protowire.VarintType), v)
}

func wireBytes(num int, v []byte) []byte {
	return protowire.AppendBytes(wireTag(num, protowire.BytesType), v)
}

func wireCat(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

// TestProtobufDirectWireForms feeds the direct path valid but less common
// encodings. Some it writes, some it refuses, none may differ from the
// regular path.
func TestProtobufDirectWireForms(t *testing.T) {
	md, proc := directTestSchema(t)
	fixed32 := func(num int, v uint32) []byte {
		return protowire.AppendFixed32(wireTag(num, protowire.Fixed32Type), v)
	}
	fixed64 := func(num int, v uint64) []byte {
		return protowire.AppendFixed64(wireTag(num, protowire.Fixed64Type), v)
	}
	packedVarints := func(vs ...uint64) []byte {
		var b []byte
		for _, v := range vs {
			b = protowire.AppendVarint(b, v)
		}
		return b
	}

	tests := []struct {
		name     string
		msg      []byte
		accepted bool
	}{
		{"empty message", nil, true},
		{"fields out of order", wireCat(wireBytes(3, []byte("x")), wireVarint(1, 5), wireVarint(2, 7)), true},
		{"negative int32 as ten byte varint", wireVarint(1, uint64(math.MaxUint64)), true},
		{"unpacked repeated int32", wireCat(wireVarint(13, 1), wireVarint(13, 0), wireVarint(13, 3)), true},
		{"packed repeated int32", wireBytes(13, packedVarints(1, 0, 3)), true},
		{"packed then unpacked", wireCat(wireBytes(13, packedVarints(1, 2)), wireVarint(13, 3)), true},
		{"interleaved repeated fields", wireCat(wireVarint(13, 1), wireBytes(14, []byte("a")), wireVarint(13, 2), wireBytes(14, []byte(""))), true},
		{"empty packed payload", wireBytes(13, nil), true},
		{"empty packed payload then values", wireCat(wireBytes(13, nil), wireVarint(13, 4)), true},
		{"packed doubles", wireBytes(15, protowire.AppendFixed64(protowire.AppendFixed64(nil, math.Float64bits(1.5)), math.Float64bits(0))), true},
		{"unpacked doubles", wireCat(fixed64(15, math.Float64bits(2.5)), fixed64(15, math.Float64bits(math.NaN()))), true},
		{"packed fixed32", wireBytes(16, protowire.AppendFixed32(protowire.AppendFixed32(nil, 7), math.MaxUint32)), true},
		{"unpacked fixed32", wireCat(fixed32(16, 7), fixed32(16, 0)), true},
		{"packed sint64", wireBytes(18, packedVarints(protowire.EncodeZigZag(-5), protowire.EncodeZigZag(math.MinInt64), 0)), true},
		{"packed bools", wireBytes(19, packedVarints(1, 0, 1)), true},
		{"packed enums with unknown number", wireBytes(20, packedVarints(1, 77, 0)), true},
		{"overlong varint encoding", wireCat(wireTag(1, protowire.VarintType), []byte{0x85, 0x80, 0x00}), true},
		{"explicit presence zero", wireCat(wireVarint(11, 0), wireBytes(12, nil)), true},
		{"float canonical NaN", fixed32(5, 0x7fc00000), true},
		{"float signalling NaN", fixed32(5, 0x7f803030), false},
		{"float negative NaN", fixed32(5, 0xffc00000), false},
		{"float NaN with payload", fixed32(5, 0x7fc00001), false},
		{"double canonical NaN", fixed64(6, math.Float64bits(math.NaN())), true},
		{"double signalling NaN", fixed64(6, 0x7ff0000000003030), true},
		{"double negative NaN", fixed64(6, 0xfff8000000000000), true},
		{"repeated double NaN with payload", wireCat(fixed64(15, 0x7ff8000000000001), fixed64(15, 0x7ff0000000000001)), true},
		{"negative zero float", fixed32(5, math.Float32bits(float32(math.Copysign(0, -1)))), false},
		{"negative zero double", fixed64(6, math.Float64bits(math.Copysign(0, -1))), false},

		{"explicit zero int", wireVarint(1, 0), false},
		{"explicit zero string", wireBytes(3, nil), false},
		{"explicit false", wireVarint(4, 0), false},
		{"explicit zero in unmapped field", wireBytes(17, nil), false},
		{"scalar written twice", wireCat(wireVarint(1, 5), wireVarint(1, 6)), false},
		{"unmapped scalar written twice", wireCat(wireBytes(17, []byte("a")), wireBytes(17, []byte("b"))), false},
		{"unknown field", wireVarint(99, 5), false},
		{"unknown field beyond table", wireVarint(100000, 5), false},
		{"wrong wire type", wireBytes(1, []byte("abc")), false},
		{"fixed for varint field", fixed32(1, 5), false},
		{"varint for string field", wireVarint(3, 5), false},
		{"int32 out of range", wireVarint(1, 1<<33), false},
		{"uint32 out of range", wireVarint(8, 1<<33), false},
		{"sint32 out of range", wireVarint(7, 1<<33), false},
		{"bool out of range", wireVarint(4, 2), false},
		{"emoji", wireBytes(3, []byte("padding 😀 padding")), true},
		{"last code point below U+40000", wireBytes(3, []byte("\U0003FFFF")), true},
		{"first code point of U+40000", wireBytes(3, []byte("\U00040000")), false},
		{"private use plane", wireBytes(3, []byte("00000000\U000baeba00000")), false},
		{"last code point", wireBytes(14, []byte("\U0010FFFF")), false},
		{"private use plane in unmapped field", wireBytes(17, []byte("\U000F0000")), false},
		{"invalid utf8", wireBytes(3, []byte{0xff, 0xfe}), false},
		{"invalid utf8 after ascii", wireBytes(3, []byte{'a', 'b', 0xc3}), false},
		{"surrogate half", wireBytes(3, []byte{0xed, 0xa0, 0x80}), false},
		{"overlong encoding", wireBytes(3, []byte{0xc0, 0xaf}), false},
		{"invalid utf8 in unmapped field", wireBytes(17, []byte{0xff}), false},
		{"invalid utf8 in repeated string", wireBytes(14, []byte{0xc0}), false},
		{"truncated varint", wireCat(wireTag(1, protowire.VarintType), []byte{0x80}), false},
		{"truncated string", wireCat(wireTag(3, protowire.BytesType), []byte{5, 'a'}), false},
		{"truncated packed fixed", wireBytes(15, []byte{1, 2, 3}), false},
		{"group", wireCat(wireTag(1, protowire.StartGroupType), wireTag(1, protowire.EndGroupType)), false},
		{"field number zero", []byte{0x00, 0x01}, false},
		{"trailing garbage", wireCat(wireVarint(1, 5), []byte{0xff}), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Surrounded by ordinary rows so that levels and row boundaries
			// of the neighbours are checked as well.
			plain := wireCat(wireVarint(1, 1), wireVarint(13, 9), wireBytes(14, []byte("z")))
			data := [][]byte{plain, test.msg, plain}
			accepted := requireDirectSound(t, md, ProtobufDirectOptions{UseProtoNames: true}, proc, data)
			assert.Equal(t, test.accepted, accepted)
		})
	}
}

// randomDirectCase builds a random flat schema with a random parquet schema
// over a subset of its fields, and random canonical messages for it.
func randomDirectCase(tb testing.TB, rng *rand.Rand, opts ProtobufDirectOptions, rows int) (protoreflect.MessageDescriptor, *parquetEncodeProcessor, [][]byte) {
	kinds := pbTestKinds()
	pqFor := map[string][]string{
		"bool": {"BOOLEAN"}, "int32": {"INT32", "INT64"}, "sint32": {"INT32", "INT64"}, "sfixed32": {"INT32", "INT64"},
		"uint32": {"INT64"}, "fixed32": {"INT64"}, "int64": {"INT64"}, "sint64": {"INT64"}, "sfixed64": {"INT64"},
		"float": {"FLOAT", "DOUBLE"}, "double": {"DOUBLE"}, "string": {"UTF8", "BYTE_ARRAY"}, "bytes": {"BYTE_ARRAY"},
		"enum": {"UTF8", "ENUM"},
	}
	if opts.UseEnumNumbers {
		pqFor["enum"] = []string{"INT32", "INT64"}
	}
	var (
		fields    []pbTestField
		cols      []pqTestColumn
		fieldKind []pbTestKind
	)
	numFields := 1 + rng.Intn(40)
	number := int32(0)
	for i := range numFields {
		k := kinds[rng.Intn(len(kinds))]
		number += int32(1 + rng.Intn(40))
		f := pbTestField{name: fmt.Sprintf("f%d", i), number: number, kind: k.kind, typeName: k.typeName}
		switch rng.Intn(4) {
		case 0:
			f.repeated = true
		case 1:
			f.explicit = true
		}
		fields = append(fields, f)
		fieldKind = append(fieldKind, k)
		choices := pqFor[k.name]
		if len(choices) == 0 || rng.Intn(6) == 0 {
			continue // no column for this field
		}
		cols = append(cols, pqTestColumn{
			name:     f.name,
			typ:      choices[rng.Intn(len(choices))],
			repeated: f.repeated,
			optional: !f.repeated && rng.Intn(3) > 0,
		})
	}
	for i := range rng.Intn(3) {
		cols = append(cols, pqTestColumn{name: fmt.Sprintf("only_parquet%d", i), typ: "INT64", optional: i%2 == 0})
	}
	if len(cols) == 0 {
		cols = append(cols, pqTestColumn{name: "only_parquet", typ: "INT64", optional: true})
	}
	rng.Shuffle(len(cols), func(i, j int) { cols[i], cols[j] = cols[j], cols[i] })

	md := pbTestDescriptor(tb, fields)
	extra := []string{"default_compression: zstd", "default_compression: snappy\ndefault_encoding: PLAIN", ""}[rng.Intn(3)]
	proc := pqTestProcessor(tb, cols, extra)

	data := make([][]byte, rows)
	for r := range data {
		data[r] = pbMarshal(tb, md, func(msg *dynamicpb.Message) {
			for i, f := range fields {
				if rng.Intn(4) == 0 {
					continue
				}
				fd := md.Fields().ByNumber(protoreflect.FieldNumber(f.number))
				k := fieldKind[i]
				pick := func() protoreflect.Value {
					if rng.Intn(5) == 0 {
						return k.zero
					}
					return k.values[rng.Intn(len(k.values))]
				}
				if f.repeated {
					list := msg.Mutable(fd).List()
					for range rng.Intn(5) {
						list.Append(pick())
					}
					continue
				}
				msg.Set(fd, pick())
			}
		})
	}
	return md, proc, data
}

// TestProtobufDirectRandomSchemas compares both paths over many random
// schemas and messages.
func TestProtobufDirectRandomSchemas(t *testing.T) {
	rng := rand.New(rand.NewSource(20261119))
	for i := range 300 {
		opts := directOptionCombos[rng.Intn(len(directOptionCombos))]
		opts.UseProtoNames = true
		md, proc, data := randomDirectCase(t, rng, opts, 1+rng.Intn(60))
		if !requireDirectSound(t, md, opts, proc, data) {
			t.Fatalf("case %d: canonical messages were refused", i)
		}
	}
}

// TestProtobufDirectLargeBatch crosses page and chunk boundaries.
func TestProtobufDirectLargeBatch(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	md, proc := directTestSchema(t)
	fds := md.Fields()
	const rows = 150_000
	data := make([][]byte, rows)
	for r := range data {
		data[r] = pbMarshal(t, md, func(msg *dynamicpb.Message) {
			if r%7 != 0 {
				msg.Set(fds.ByName("i32"), protoreflect.ValueOfInt32(int32(rng.Intn(1000)+1)))
			}
			msg.Set(fds.ByName("i64"), protoreflect.ValueOfInt64(rng.Int63()+1))
			if r%3 != 0 {
				msg.Set(fds.ByName("s"), protoreflect.ValueOfString(fmt.Sprintf("value-%d-%d", r, rng.Int63())))
			}
			list := msg.Mutable(fds.ByName("rs")).List()
			for range rng.Intn(6) {
				list.Append(protoreflect.ValueOfString(strings.Repeat("y", rng.Intn(40))))
			}
			ints := msg.Mutable(fds.ByName("ri")).List()
			for range rng.Intn(4) {
				ints.Append(protoreflect.ValueOfInt32(rng.Int31()))
			}
		})
	}
	require.True(t, requireDirectSound(t, md, ProtobufDirectOptions{UseProtoNames: true}, proc, data))
}

// TestProtobufDirectWideSchema covers the 101 column benchmark schema.
func TestProtobufDirectWideSchema(t *testing.T) {
	fields := wideFields()
	md := wideDescriptor(t, fields)
	data := wideData(t, md, 3000)
	for _, encoding := range []string{"PLAIN", "DELTA_LENGTH_BYTE_ARRAY"} {
		proc := wideEncodeProcessor(t, fields, encoding)
		require.True(t, requireDirectSound(t, md, ProtobufDirectOptions{UseProtoNames: true}, proc, data))
	}
}

// TestProtobufDirectMixedPlans writes one file from messages of two schema
// versions, as happens when a topic's schema evolves mid batch.
func TestProtobufDirectMixedPlans(t *testing.T) {
	i64 := descriptorpb.FieldDescriptorProto_TYPE_INT64
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	v1 := pbTestDescriptor(t, []pbTestField{{name: "a", number: 1, kind: i64}, {name: "b", number: 2, kind: str}})
	v2 := pbTestDescriptor(t, []pbTestField{{name: "a", number: 1, kind: i64}, {name: "b", number: 2, kind: str}, {name: "c", number: 40, kind: str, repeated: true}})
	proc := pqTestProcessor(t, []pqTestColumn{
		{name: "a", typ: "INT64", optional: true},
		{name: "b", typ: "UTF8"},
		{name: "c", typ: "UTF8", repeated: true},
	}, "")
	enc := &EncodeProcessor{p: proc}
	p1, err := enc.NewProtobufPlan(v1, ProtobufDirectOptions{})
	require.NoError(t, err)
	p2, err := enc.NewProtobufPlan(v2, ProtobufDirectOptions{})
	require.NoError(t, err)

	m1 := wireCat(wireVarint(1, 5), wireBytes(2, []byte("one")))
	m2 := wireCat(wireVarint(1, 6), wireBytes(40, []byte("x")), wireBytes(40, []byte("y")))
	got, err := enc.EncodeProtobuf([]*ProtobufPlan{p1, p2, p1, p2}, [][]byte{m1, m2, nil, nil})
	require.NoError(t, err)

	// Every v1 message is also a valid v2 message, so the regular path over
	// v2 gives the expected file.
	want, err := regularEncode(v2, ProtobufDirectOptions{}, proc, [][]byte{m1, m2, nil, nil})
	require.NoError(t, err)
	require.Empty(t, diffParquetFiles(t, want, got))

	// A v2 message is refused by the v1 plan, which does not know field 40.
	_, err = enc.EncodeProtobuf([]*ProtobufPlan{p1}, [][]byte{m2})
	require.ErrorIs(t, err, errDirectRefused)
}

// FuzzProtobufDirect throws arbitrary bytes at the direct path. Whatever it
// accepts must match the regular path.
func FuzzProtobufDirect(f *testing.F) {
	md, proc := directTestSchema(f)
	rng := rand.New(rand.NewSource(1))
	fds := md.Fields()
	kinds := map[protoreflect.Kind]pbTestKind{}
	for _, k := range pbTestKinds() {
		kinds[pbTestDescriptor(f, []pbTestField{{name: "x", number: 1, kind: k.kind, typeName: k.typeName}}).Fields().Get(0).Kind()] = k
	}
	for range 40 {
		f.Add(pbMarshal(f, md, func(msg *dynamicpb.Message) {
			for i := range fds.Len() {
				fd := fds.Get(i)
				k := kinds[fd.Kind()]
				if rng.Intn(3) == 0 {
					continue
				}
				if fd.IsList() {
					list := msg.Mutable(fd).List()
					for range rng.Intn(4) {
						list.Append(k.values[rng.Intn(len(k.values))])
					}
					continue
				}
				msg.Set(fd, k.values[rng.Intn(len(k.values))])
			}
		}), byte(rng.Intn(4)))
	}
	f.Add([]byte{}, byte(0))
	f.Add(wireCat(wireVarint(1, 0)), byte(1))
	f.Add(wireCat(wireVarint(13, 1), wireBytes(13, []byte{1, 2, 3})), byte(2))

	plain := wireCat(wireVarint(1, 1), wireVarint(13, 9), wireBytes(14, []byte("z")))
	f.Fuzz(func(t *testing.T, msg []byte, flags byte) {
		opts := ProtobufDirectOptions{
			UseProtoNames:     true,
			EmitUnpopulated:   flags&1 != 0,
			EmitDefaultValues: flags&2 != 0,
		}
		requireDirectSound(t, md, opts, proc, [][]byte{plain, msg, plain})
	})
}
