// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build arm64 || amd64

package common

import (
	"buf.build/go/hyperpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// NewHyperPbParser returns a new ProtobufDecoder based on hyperpb.
func NewHyperPbDecoder(
	schema *descriptorpb.FileDescriptorSet,
	messageName protoreflect.FullName,
) (ProtobufDecoder, error) {
	msgType, err := hyperpb.CompileFileDescriptorSet(schema, messageName)
	if err != nil {
		return nil, err
	}
	return &hyperPbParser{msgType}, nil
}

type hyperPbParser struct {
	msgType *hyperpb.MessageType
}

var _ ProtobufDecoder = (*hyperPbParser)(nil)

// WithDecoded implements ProtobufParser.
func (p *hyperPbParser) WithDecoded(buf []byte, cb func(msg proto.Message) error) error {
	// TODO: reuse allocs with hyperpb.Shared
	msg := hyperpb.NewMessage(p.msgType)
	if err := msg.Unmarshal(buf); err != nil {
		return err
	}
	return cb(msg)
}
