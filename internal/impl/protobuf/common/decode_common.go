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

import "google.golang.org/protobuf/proto"

// ProtobufDecoder is an interface for different methods to parse protobuf
// (the binary format) in a dynamic and reflective way.
//
// Currently, there are two supported approaches: dynamicpb and hyperpb
type ProtobufDecoder interface {
	// Decode the buffer into a proto message that is passed into the callback.
	//
	// The callback allows for optimizations such as re-using allocations in high
	// performance situations, so the passed in msg should never be used outside
	// the provided callback.
	WithDecoded(buf []byte, cb func(msg proto.Message) error) error
}

// ProfilingOptions specifies the profiling rate and how often we recompile
// for ProtobufDecoders that support profile-guided optimizations in flight (PGO)
type ProfilingOptions struct {
	Rate              float64
	RecompileInterval int64
}
