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
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// loadTestFileDescriptorSet loads test proto descriptors as a FileDescriptorSet
func loadTestFileDescriptorSet(t testing.TB) (*descriptorpb.FileDescriptorSet, protoreflect.MessageDescriptor, *protoregistry.Types) {
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

	return schema, md, types
}

// BenchmarkProtobufToMessage benchmarks the complete pipeline of decoding protobuf
// and converting to a Benthos message, testing the matrix of:
// - Decoding: dynamicpb vs hyperpb
// - Conversion: Fast (SetStructuredMut) vs Slow (SetBytes)
// - Read pattern: convert only vs convert + read structured data
func BenchmarkProtobufToMessage(b *testing.B) {
	schema, md, types := loadTestFileDescriptorSet(b)

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

	messageName := md.FullName()

	// Create decoders
	dynamicpbDecoder, err := NewDynamicPbDecoder(schema, messageName)
	if err != nil {
		b.Fatal(err)
	}

	hyperpbDecoder, err := NewHyperPbDecoder(schema, messageName)
	if err != nil {
		b.Fatal(err)
	}

	marshalOpts := protojson.MarshalOptions{Resolver: types}

	for _, tc := range testCases {
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

		// Benchmark: dynamicpb decode + fast conversion
		b.Run(tc.name+"/dynamicpb/fast/convert_only", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := dynamicpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageFast(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/dynamicpb/fast/convert_and_read", func(b *testing.B) {
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

		// Benchmark: dynamicpb decode + slow conversion
		b.Run(tc.name+"/dynamicpb/slow/convert_only", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := dynamicpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageSlow(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/dynamicpb/slow/convert_and_read", func(b *testing.B) {
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

		// Benchmark: hyperpb decode + fast conversion
		b.Run(tc.name+"/hyperpb/fast/convert_only", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := hyperpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageFast(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/hyperpb/fast/convert_and_read", func(b *testing.B) {
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

		// Benchmark: hyperpb decode + slow conversion
		b.Run(tc.name+"/hyperpb/slow/convert_only", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				err := hyperpbDecoder.WithDecoded(pbBytes, func(decoded proto.Message) error {
					return ToMessageSlow(decoded.(protoreflect.Message), marshalOpts, msg)
				})
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/hyperpb/slow/convert_and_read", func(b *testing.B) {
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
