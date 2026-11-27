// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"math/rand"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/protobuf/common"
)

// End to end cost of a wide protobuf topic written as parquet, comparing the
// regular protobuf -> Go map -> parquet path against writing parquet columns
// straight from the protobuf wire format.
//
// The schema mirrors a real 101 column workload: 49 int32, 23 string,
// 12 int64, 9 bool, 7 lists and 1 float.

type wideField struct {
	name     string
	kind     descriptorpb.FieldDescriptorProto_Type
	repeated bool
	pqType   string
}

func wideFields() []wideField {
	var fields []wideField
	add := func(n int, kind descriptorpb.FieldDescriptorProto_Type, repeated bool, pqType string) {
		for range n {
			fields = append(fields, wideField{fmt.Sprintf("f%03d", len(fields)), kind, repeated, pqType})
		}
	}
	add(49, descriptorpb.FieldDescriptorProto_TYPE_INT32, false, "INT32")
	add(23, descriptorpb.FieldDescriptorProto_TYPE_STRING, false, "UTF8")
	add(12, descriptorpb.FieldDescriptorProto_TYPE_INT64, false, "INT64")
	add(9, descriptorpb.FieldDescriptorProto_TYPE_BOOL, false, "BOOLEAN")
	add(4, descriptorpb.FieldDescriptorProto_TYPE_INT32, true, "INT32")
	add(3, descriptorpb.FieldDescriptorProto_TYPE_STRING, true, "UTF8")
	add(1, descriptorpb.FieldDescriptorProto_TYPE_FLOAT, false, "FLOAT")
	return fields
}

func wideDescriptor(tb testing.TB, fields []wideField) protoreflect.MessageDescriptor {
	msg := &descriptorpb.DescriptorProto{Name: new("Wide")}
	for i, f := range fields {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if f.repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		msg.Field = append(msg.Field, &descriptorpb.FieldDescriptorProto{
			Name:     new(f.name),
			JsonName: new(f.name),
			Number:   new(int32(i + 1)),
			Type:     f.kind.Enum(),
			Label:    label.Enum(),
		})
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        new("wide.proto"),
		Package:     new("bench"),
		Syntax:      new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{msg},
	}, nil)
	require.NoError(tb, err)
	return fd.Messages().Get(0)
}

// wideData returns rows encoded protobuf messages with a mix of low, medium
// and high cardinality strings, roughly 900 bytes each.
func wideData(tb testing.TB, md protoreflect.MessageDescriptor, rows int) [][]byte {
	rng := rand.New(rand.NewSource(42))
	pool := func(n, length int) []string {
		out := make([]string, n)
		for i := range out {
			b := make([]byte, length)
			for j := range b {
				b[j] = "abcdefghijklmnopqrstuvwxyz0123456789"[rng.Intn(36)]
			}
			out[i] = string(b)
		}
		return out
	}
	low, mid := pool(50, 12), pool(5000, 24)

	out := make([][]byte, rows)
	fds := md.Fields()
	for r := range out {
		msg := dynamicpb.NewMessage(md)
		for i := range fds.Len() {
			fd := fds.Get(i)
			str := func() string {
				switch i % 5 {
				case 0, 1:
					return low[rng.Intn(len(low))]
				case 2, 3:
					return mid[rng.Intn(len(mid))]
				default:
					return fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
				}
			}
			scalar := func() protoreflect.Value {
				switch fd.Kind() {
				case protoreflect.Int32Kind:
					if i%3 == 0 {
						return protoreflect.ValueOfInt32(int32(1 + rng.Intn(20)))
					}
					return protoreflect.ValueOfInt32(int32(1 + rng.Intn(1_000_000)))
				case protoreflect.Int64Kind:
					return protoreflect.ValueOfInt64(1 + rng.Int63n(1<<40))
				case protoreflect.BoolKind:
					return protoreflect.ValueOfBool(true)
				case protoreflect.FloatKind:
					return protoreflect.ValueOfFloat32(rng.Float32() + 0.01)
				default:
					return protoreflect.ValueOfString(str())
				}
			}
			if fd.IsList() {
				l := msg.Mutable(fd).List()
				for range 1 + rng.Intn(4) {
					l.Append(scalar())
				}
				continue
			}
			msg.Set(fd, scalar())
		}
		b, err := proto.Marshal(msg)
		require.NoError(tb, err)
		out[r] = b
	}
	return out
}

func wideEncodeProcessor(tb testing.TB, fields []wideField, encoding string) *parquetEncodeProcessor {
	var conf strings.Builder
	fmt.Fprintf(&conf, "default_compression: zstd\ndefault_encoding: %s\nschema:\n", encoding)
	for _, f := range fields {
		if f.repeated {
			fmt.Fprintf(&conf, "  - { name: %s, type: %s, repeated: true }\n", f.name, f.pqType)
		} else {
			fmt.Fprintf(&conf, "  - { name: %s, type: %s, optional: true }\n", f.name, f.pqType)
		}
	}
	pConf, err := parquetEncodeProcessorConfig().ParseYAML(conf.String(), nil)
	require.NoError(tb, err)
	proc, err := newParquetEncodeProcessorFromConfig(pConf, nil)
	require.NoError(tb, err)
	return proc
}

// currentPath is what the pipeline does today: schema_registry_decode with
// serialize_to_json false followed by parquet_encode.
func currentPath(decoder common.ProtobufDecoder, proc *parquetEncodeProcessor, data [][]byte) ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true}
	batch := make(service.MessageBatch, len(data))
	for i, b := range data {
		m := service.NewMessage(b)
		if err := decoder.WithDecoded(b, func(msg proto.Message) error {
			return common.ToMessageFast(msg.ProtoReflect(), opts, m)
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

//------------------------------------------------------------------------------

// directPlan writes parquet rows straight from decoded protobuf messages for
// flat schemas (top level optional, required or repeated leaves).
//------------------------------------------------------------------------------

func readAllRows(tb testing.TB, file []byte) []parquet.Row {
	f, err := parquet.OpenFile(bytes.NewReader(file), int64(len(file)))
	require.NoError(tb, err)
	var out []parquet.Row
	for _, rg := range f.RowGroups() {
		rr := rg.Rows()
		for {
			buf := make([]parquet.Row, 512)
			n, err := rr.ReadRows(buf)
			for _, r := range buf[:n] {
				out = append(out, r.Clone())
			}
			if err != nil {
				break
			}
		}
		require.NoError(tb, rr.Close())
	}
	return out
}

func BenchmarkWideProtoToParquet(b *testing.B) {
	const rows = 10_000
	fields := wideFields()
	md := wideDescriptor(b, fields)
	data := wideData(b, md, rows)

	var protoBytes int
	for _, d := range data {
		protoBytes += len(d)
	}
	report := func(b *testing.B, fileLen int) {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
		b.ReportMetric(float64(fileLen)/rows, "filebytes/row")
		b.ReportMetric(float64(protoBytes)/rows, "protobytes/row")
	}

	for _, encoding := range []string{"PLAIN", "DELTA_LENGTH_BYTE_ARRAY"} {
		proc := wideEncodeProcessor(b, fields, encoding)

		b.Run("path=regular/encoding="+encoding, func(b *testing.B) {
			decoder := common.NewHyperPbDecoder(md, common.DefaultProfilingOptions)
			var n int
			b.ReportAllocs()
			for b.Loop() {
				out, err := currentPath(decoder, proc, data)
				if err != nil {
					b.Fatal(err)
				}
				n = len(out)
			}
			report(b, n)
		})

		b.Run("path=direct/encoding="+encoding, func(b *testing.B) {
			enc := &EncodeProcessor{p: proc}
			plan, err := enc.NewProtobufPlan(md, ProtobufDirectOptions{UseProtoNames: true})
			require.NoError(b, err)
			plans := make([]*ProtobufPlan, len(data))
			for i := range plans {
				plans[i] = plan
			}
			var n int
			b.ReportAllocs()
			for b.Loop() {
				out, err := enc.EncodeProtobuf(plans, data)
				if err != nil {
					b.Fatal(err)
				}
				n = len(out)
			}
			report(b, n)
		})
	}
}
