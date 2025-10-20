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
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// NewDynamicPbParser returns a new ProtobufDecoder based on standard proto reflection
// in the offical protobuf library.
func NewDynamicPbDecoder(
	md protoreflect.MessageDescriptor,
	_ ProfilingOptions,
) ProtobufDecoder {
	return &dynamicPbParser{dynamicpb.NewMessageType(md)}
}

type dynamicPbParser struct {
	msgType protoreflect.MessageType
}

var _ ProtobufDecoder = (*dynamicPbParser)(nil)

// WithDecoded implements ProtobufParser.
func (p *dynamicPbParser) WithDecoded(buf []byte, cb func(msg proto.Message) error) error {
	dynMsg := p.msgType.New().Interface()
	if err := proto.Unmarshal(buf, dynMsg); err != nil {
		return fmt.Errorf("failed to unmarshal protobuf message: '%v': %w", p.msgType.Descriptor().FullName(), err)
	}
	return cb(dynMsg)
}
