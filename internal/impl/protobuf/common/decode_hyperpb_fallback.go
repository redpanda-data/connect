// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !(arm64 || amd64)

package common

import (
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NewHyperPbDecoder falls back to using NewDynamicPbDecoder
// on platforms where hyperpb is not supported.
func NewHyperPbDecoder(
	md protoreflect.MessageDescriptor,
	opts ProfilingOptions,
) ProtobufDecoder {
	return NewDynamicPbDecoder(md, opts)
}
