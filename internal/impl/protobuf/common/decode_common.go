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

// DefaultProfilingOptions are the standard profiling settings used across all
// hyperpb decoder call sites.
//
// Profile-guided recompilation is currently disabled (RecompileInterval: 0)
// because hyperpb v0.1.3 leaks profile data across recompiles: MessageType.Recompile
// clones the previous type's compile options and appends another WithProfile, and
// CompileMessageDescriptor stores that growing slice in the new type's
// Library.Metadata. The live type therefore pins every prior generation's profile
// recorder (a 32 KB ring buffer per field), so memory grows unbounded under
// sustained throughput. Only the final profile is ever used during compilation, so
// the retained generations serve no purpose. With recompilation off there is nothing
// to record into, so Rate is 0 as well to avoid the per-message recording overhead.
// Re-enable once the upstream leak is fixed.
var DefaultProfilingOptions = ProfilingOptions{
	Rate:              0,
	RecompileInterval: 0,
}
