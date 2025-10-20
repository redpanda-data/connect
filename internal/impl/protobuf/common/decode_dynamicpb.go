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
	"google.golang.org/protobuf/types/descriptorpb"
)

// NewDynamicPbParser returns a new ProtobufDecoder based on standard proto reflection
// in the offical protobuf library.
func NewDynamicPbDecoder(
	schema *descriptorpb.FileDescriptorSet,
	messageName protoreflect.FullName,
) (ProtobufDecoder, error) {
	_, types, err := BuildRegistries(schema)
	if err != nil {
		return nil, fmt.Errorf("unable to build protobuf registry for '%v': %w", messageName, err)
	}
	msgType, err := types.FindMessageByName(messageName)
	if err != nil {
		return nil, fmt.Errorf("unable to find message '%v' definition", messageName)
	}
	return &dynamicPbParser{msgType}, nil
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
