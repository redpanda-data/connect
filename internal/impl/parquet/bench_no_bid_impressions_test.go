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

// Benchmarks the decode -> parquet_encode hot path of a customer pipeline
// (no_bid_impressions.yaml) using synthetic Protobuf rows derived from the
// pipeline's parquet_encode schema.
//
//   NO_BID_CONFIG=$PWD/no_bid_impressions.yaml go test -run '^$' \
//     -bench BenchmarkNoBidImpressions -benchmem ./internal/impl/parquet/

import (
	"context"
	"crypto/sha256"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/protobuf/common"
)

type noBidColumn struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Repeated bool   `yaml:"repeated"`
}

type noBidFixture struct {
	encodeConf map[string]any // the pipeline's parquet_encode block
	columns    []noBidColumn
	msgDesc    protoreflect.MessageDescriptor
	raw        [][]byte // serialised Protobuf rows
}

func loadNoBidFixture(tb testing.TB, rows int) *noBidFixture {
	tb.Helper()
	path := os.Getenv("NO_BID_CONFIG")
	if path == "" {
		tb.Skip("NO_BID_CONFIG not set")
	}
	b, err := os.ReadFile(path)
	require.NoError(tb, err)

	var conf struct {
		Pipeline struct {
			Processors []map[string]any `yaml:"processors"`
		} `yaml:"pipeline"`
	}
	require.NoError(tb, yaml.Unmarshal(b, &conf))

	f := &noBidFixture{}
	for _, p := range conf.Pipeline.Processors {
		if pe, ok := p["parquet_encode"].(map[string]any); ok {
			f.encodeConf = pe
		}
	}
	require.NotNil(tb, f.encodeConf, "no parquet_encode processor in config")

	schemaBytes, err := yaml.Marshal(f.encodeConf["schema"])
	require.NoError(tb, err)
	require.NoError(tb, yaml.Unmarshal(schemaBytes, &f.columns))

	f.msgDesc = noBidMessageDescriptor(tb, f.columns)
	rng := rand.New(rand.NewPCG(1, 2))
	f.raw = make([][]byte, rows)
	for i := range f.raw {
		f.raw[i], err = proto.Marshal(noBidRandomRow(rng, f.msgDesc))
		require.NoError(tb, err)
	}
	return f
}

func noBidMessageDescriptor(tb testing.TB, cols []noBidColumn) protoreflect.MessageDescriptor {
	tb.Helper()
	msg := &descriptorpb.DescriptorProto{Name: proto.String("NoBidImpression")}
	for i, c := range cols {
		var t descriptorpb.FieldDescriptorProto_Type
		switch c.Type {
		case "INT32":
			t = descriptorpb.FieldDescriptorProto_TYPE_INT32
		case "INT64":
			t = descriptorpb.FieldDescriptorProto_TYPE_INT64
		case "UTF8":
			t = descriptorpb.FieldDescriptorProto_TYPE_STRING
		case "BOOLEAN":
			t = descriptorpb.FieldDescriptorProto_TYPE_BOOL
		case "FLOAT":
			t = descriptorpb.FieldDescriptorProto_TYPE_FLOAT
		default:
			tb.Fatalf("unsupported column type %v", c.Type)
		}
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if c.Repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		msg.Field = append(msg.Field, &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(c.Name),
			Number: proto.Int32(int32(i + 1)),
			Type:   t.Enum(),
			Label:  label.Enum(),
		})
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("no_bid.proto"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{msg},
	}, nil)
	require.NoError(tb, err)
	return fd.Messages().Get(0)
}

// Low-cardinality strings stand in for enum-like columns (domains, app
// stores, versions); id-like columns get high-cardinality hex.
var noBidStringPool = func() []string {
	out := make([]string, 64)
	for i := range out {
		out[i] = fmt.Sprintf("value-%02d.example.com/path", i)
	}
	return out
}()

func noBidRandomString(rng *rand.Rand, name string) string {
	switch {
	case rng.IntN(5) == 0:
		return ""
	case strings.HasSuffix(name, "_id") || strings.HasSuffix(name, "hash"):
		return fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64())
	default:
		return noBidStringPool[rng.IntN(len(noBidStringPool))]
	}
}

func noBidRandomScalar(rng *rand.Rand, fd protoreflect.FieldDescriptor) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.Int32Kind:
		if rng.IntN(4) == 0 {
			return protoreflect.ValueOfInt32(0)
		}
		return protoreflect.ValueOfInt32(rng.Int32N(2000))
	case protoreflect.Int64Kind:
		return protoreflect.ValueOfInt64(1_760_000_000_000 + rng.Int64N(3_600_000))
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(noBidRandomString(rng, string(fd.Name())))
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(rng.IntN(2) == 0)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(rng.Float32())
	}
	panic("unreachable")
}

func noBidRandomRow(rng *rand.Rand, md protoreflect.MessageDescriptor) proto.Message {
	m := dynamicpb.NewMessage(md)
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if fd.IsList() {
			l := m.Mutable(fd).List()
			for range rng.IntN(5) {
				l.Append(noBidRandomScalar(rng, fd))
			}
			continue
		}
		m.Set(fd, noBidRandomScalar(rng, fd))
	}
	return m
}

// decodeBatch mirrors schema_registry_decode with use_proto_names,
// emit_default_values and serialize_to_json: false.
func (f *noBidFixture) decodeBatch(tb testing.TB, dec common.ProtobufDecoder, raw [][]byte) service.MessageBatch {
	opts := protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}
	batch := make(service.MessageBatch, len(raw))
	for i, b := range raw {
		m := service.NewMessage(b)
		if err := dec.WithDecoded(b, func(msg proto.Message) error {
			return common.ToMessageFast(msg.ProtoReflect(), opts, m)
		}); err != nil {
			tb.Fatal(err)
		}
		batch[i] = m
	}
	return batch
}

func (f *noBidFixture) encoder(tb testing.TB, overrides map[string]any) *parquetEncodeProcessor {
	tb.Helper()
	conf := map[string]any{}
	maps.Copy(conf, f.encodeConf)
	for k, v := range overrides {
		if v == nil {
			delete(conf, k)
		} else {
			conf[k] = v
		}
	}
	b, err := yaml.Marshal(conf)
	require.NoError(tb, err)
	pConf, err := parquetEncodeProcessorConfig().ParseYAML(string(b), nil)
	require.NoError(tb, err)
	proc, err := newParquetEncodeProcessorFromConfig(pConf, nil)
	require.NoError(tb, err)
	return proc
}

func reportPerRow(b *testing.B, rows int, outBytes int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
	b.ReportMetric(float64(outBytes)/float64(rows), "file_B/row")
}

func BenchmarkNoBidImpressions(b *testing.B) {
	const maxRows = 100_000
	f := loadNoBidFixture(b, maxRows)
	dec := common.NewHyperPbDecoder(f.msgDesc, common.DefaultProfilingOptions)

	// The prod config's batch (~8.7k rows per partition per 5s) and larger windows.
	batchSizes := []int{8_700, 52_000, 100_000}

	variants := []struct {
		name      string
		overrides map[string]any
	}{
		{"current", nil},
		{"default_encoding", map[string]any{"default_encoding": nil}},
		{"snappy", map[string]any{"default_compression": "snappy"}},
		{"lz4raw", map[string]any{"default_compression": "lz4raw"}},
		{"uncompressed", map[string]any{"default_compression": "uncompressed"}},
		{"gzip", map[string]any{"default_compression": "gzip"}},
		{"brotli", map[string]any{"default_compression": "brotli"}},
	}

	b.Run("decode", func(b *testing.B) {
		raw := f.raw[:batchSizes[0]]
		for b.Loop() {
			_ = f.decodeBatch(b, dec, raw)
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(raw)), "ns/row")
	})

	for _, size := range batchSizes {
		decoded := f.decodeBatch(b, dec, f.raw[:size])
		for _, v := range variants {
			proc := f.encoder(b, v.overrides)
			b.Run(fmt.Sprintf("encode/variant=%s/rows=%d", v.name, size), func(b *testing.B) {
				var outBytes int
				for b.Loop() {
					out, err := proc.ProcessBatch(context.Background(), decoded)
					if err != nil {
						b.Fatal(err)
					}
					outBytes = len(mustBytes(b, out[0][0]))
				}
				reportPerRow(b, size, outBytes)
			})
		}
	}

	// Decode plus encode of a fresh batch, the work one pipeline thread does
	// per input batch.
	for _, size := range batchSizes {
		for _, v := range variants {
			proc := f.encoder(b, v.overrides)
			b.Run(fmt.Sprintf("end_to_end/variant=%s/rows=%d", v.name, size), func(b *testing.B) {
				var outBytes int
				for b.Loop() {
					batch := f.decodeBatch(b, dec, f.raw[:size])
					out, err := proc.ProcessBatch(context.Background(), batch)
					if err != nil {
						b.Fatal(err)
					}
					outBytes = len(mustBytes(b, out[0][0]))
				}
				reportPerRow(b, size, outBytes)
			})
		}
	}
}

func mustBytes(tb testing.TB, m *service.Message) []byte {
	b, err := m.AsBytes()
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// TestNoBidImpressionsOutputDigest logs a digest of each encoded file so
// output can be compared byte for byte across code versions.
func TestNoBidImpressionsOutputDigest(t *testing.T) {
	f := loadNoBidFixture(t, 8_700)
	dec := common.NewHyperPbDecoder(f.msgDesc, common.DefaultProfilingOptions)
	for _, name := range []string{"current", "default_encoding"} {
		var overrides map[string]any
		if name == "default_encoding" {
			overrides = map[string]any{"default_encoding": nil}
		}
		out, err := f.encoder(t, overrides).ProcessBatch(context.Background(), f.decodeBatch(t, dec, f.raw))
		require.NoError(t, err)
		t.Logf("digest %s file_sha256=%x rows_sha256=%x", name, sha256.Sum256(mustBytes(t, out[0][0])), noBidRowsDigest(t, out[0][0]))
	}
}

// noBidRowsDigest hashes the rows read back from an encoded file, for
// variants whose file bytes are not deterministic.
func noBidRowsDigest(tb testing.TB, m *service.Message) [32]byte {
	conf, err := parquetDecodeProcessorConfig().ParseYAML("", nil)
	require.NoError(tb, err)
	proc, err := newParquetDecodeProcessorFromConfig(conf, nil)
	require.NoError(tb, err)
	rows, err := proc.Process(context.Background(), m)
	require.NoError(tb, err)
	h := sha256.New()
	for _, r := range rows {
		h.Write(mustBytes(tb, r))
		h.Write([]byte{'\n'})
	}
	return [32]byte(h.Sum(nil))
}
