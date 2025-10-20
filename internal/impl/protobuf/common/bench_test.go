/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package common

import (
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// loadTestFileDescriptorSet loads test proto descriptors as a FileDescriptorSet
func loadTestFileDescriptorSet(t testing.TB) (protoreflect.MessageDescriptor, *protoregistry.Types) {
	t.Helper()
	mockResources := service.MockResources()

	// Load the schema as FileDescriptorSet
	schema, err := ParseFromFS(mockResources.FS(), []string{"../../../../config/test/protobuf/schema"})
	if err != nil {
		t.Fatal(err)
	}

	// Build registries to get the message descriptor and types
	files, types, err := BuildRegistries(schema)
	if err != nil {
		t.Fatal(err)
	}

	// Find the message descriptor for SerdeTest
	fd, err := files.FindFileByPath("serde_test.proto")
	if err != nil {
		t.Fatal(err)
	}
	md := fd.Messages().ByName("SerdeTest")
	if md == nil {
		t.Fatal("SerdeTest message not found")
	}

	return md, types
}

// BenchmarkProtobufToMessage benchmarks the complete pipeline of decoding protobuf
// and converting to a Benthos message, testing the matrix of:
// - Decoding: dynamicpb vs hyperpb (with PGO)
// - Conversion: Fast (SetStructuredMut) vs Slow (SetBytes)
func BenchmarkProtobufToMessage(b *testing.B) {
	md, types := loadTestFileDescriptorSet(b)

	testCases := []struct {
		name      string
		textproto string
	}{
		{
			name: "simple",
			textproto: `
				name: "test"
				count: 42
				active: true
			`,
		},
		{
			name: "complex",
			textproto: `
				name: "test"
				count: 42
				active: true
				price: 19.99
				tags: "tag1"
				tags: "tag2"
				tags: "tag3"
				metadata: {
					key: "key1"
					value: "value1"
				}
				metadata: {
					key: "key2"
					value: "value2"
				}
				nested: {
					inner_field: "nested_value"
					inner_count: 100
				}
			`,
		},
		{
			name: "with_timestamp",
			textproto: `
				name: "test"
				created_at: {
					seconds: 1234567890
					nanos: 123456789
				}
			`,
		},
	}

	b.StopTimer()
	// Profile-guided optimization settings for hyperpb
	pgoOpts := ProfilingOptions{
		Rate:              0.01,   // Profile every message during priming
		RecompileInterval: 100000, // Recompile after 1000 messages
	}

	// Create decoders
	dynamicpbDecoder := NewDynamicPbDecoder(md, ProfilingOptions{})
	hyperpbDecoder := NewHyperPbDecoder(md, pgoOpts)

	b.StartTimer()

	marshalOpts := protojson.MarshalOptions{Resolver: types}

	for _, tc := range testCases {
		b.StopTimer()
		// Parse and marshal to protobuf bytes once per test case
		pbMsg := dynamicpb.NewMessage(md)
		unmarshalOpts := prototext.UnmarshalOptions{Resolver: types}
		if err := unmarshalOpts.Unmarshal([]byte(tc.textproto), pbMsg); err != nil {
			b.Fatal(err)
		}
		pbBytes, err := proto.Marshal(pbMsg)
		if err != nil {
			b.Fatal(err)
		}

		// Prime the hyperpb decoder with sample data to build profile
		// Run with enough iterations to trigger at least one recompilation
		for range pgoOpts.RecompileInterval * 2 {
			err := hyperpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()

		// Benchmark: dynamicpb decode + fast conversion + read
		b.Run(tc.name+"/dynamicpb/fast", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := dynamicpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageFast(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
				_, err = msg.AsStructured()
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// Benchmark: dynamicpb decode + slow conversion + read
		b.Run(tc.name+"/dynamicpb/slow", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := dynamicpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageSlow(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
				_, err = msg.AsStructured()
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// Benchmark: hyperpb decode + fast conversion + read
		b.Run(tc.name+"/hyperpb/fast", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := hyperpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageFast(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
				_, err = msg.AsStructured()
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// Benchmark: hyperpb decode + slow conversion + read
		b.Run(tc.name+"/hyperpb/slow", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := hyperpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageSlow(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
				_, err = msg.AsStructured()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
