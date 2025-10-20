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
	"google.golang.org/protobuf/types/dynamicpb"
)

func BenchmarkToMessageFast(b *testing.B) {
	_, md, types := loadTestDescriptors(&testing.T{})

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

	for _, tc := range testCases {
		// Parse the message once
		pbMsg := dynamicpb.NewMessage(md)
		unmarshalOpts := prototext.UnmarshalOptions{Resolver: types}
		if err := unmarshalOpts.Unmarshal([]byte(tc.textproto), pbMsg); err != nil {
			b.Fatal(err)
		}

		marshalOpts := protojson.MarshalOptions{Resolver: types}

		b.Run(tc.name+"/fast/parse_only", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				if err := ToMessageFast(pbMsg, marshalOpts, msg); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/slow/parse_only", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				if err := ToMessageSlow(pbMsg, marshalOpts, msg); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/fast/parse_and_read", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				if err := ToMessageFast(pbMsg, marshalOpts, msg); err != nil {
					b.Fatal(err)
				}
				_, err := msg.AsStructured()
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(tc.name+"/slow/parse_and_read", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msg := service.NewMessage(nil)
				if err := ToMessageSlow(pbMsg, marshalOpts, msg); err != nil {
					b.Fatal(err)
				}
				_, err := msg.AsStructured()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
