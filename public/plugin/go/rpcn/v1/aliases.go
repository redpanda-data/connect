// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package rpcn is the v1 Redpanda Connect plugin SDK for Go.
//
// Plugin authors write code that's byte-identical to in-tree benthos
// components with one mechanical translation — change
// `github.com/redpanda-data/benthos/v4/public/service` imports to
// `github.com/redpanda-data/connect/v4/public/plugin/go/rpcn/v1`, and
// `service.NewConfigSpec()` becomes `rpcn.NewConfigSpec()`.
// Implementation interfaces (BatchProcessor, BatchInput, BatchOutput,
// Message, ParsedConfig, etc.) are type aliases to the underlying
// benthos types, so ProcessBatch / Connect / Close method signatures
// remain literally identical to in-tree code.
//
// A plugin's main() builds an *Environment with the desired
// components and calls Serve(env). One binary registers any number of
// components; the host multiplexes instances across the single gRPC
// connection.
package rpcn

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// BatchProcessor is an alias for benthos's batch-processor interface.
// Plugin code implementing a ProcessBatch / Close method gets the
// literal benthos signatures.
type BatchProcessor = service.BatchProcessor

// BatchInput is an alias for benthos's batch-input interface.
type BatchInput = service.BatchInput

// BatchOutput is an alias for benthos's batch-output interface.
type BatchOutput = service.BatchOutput

// Message is an alias for benthos's per-message handle.
type Message = service.Message

// MessageBatch is an alias for benthos's batch type.
type MessageBatch = service.MessageBatch

// AckFunc is an alias for benthos's input-side acknowledgement
// callback.
type AckFunc = service.AckFunc

// ParsedConfig is an alias for benthos's parsed component
// configuration.
type ParsedConfig = service.ParsedConfig

// Resources is an alias for benthos's per-component resource bundle.
type Resources = service.Resources

// BatchPolicy is an alias for benthos's batch-policy type.
type BatchPolicy = service.BatchPolicy

// BatchProcessorCtor is the constructor signature for a registered
// batch-processor component.
type BatchProcessorCtor = func(conf *ParsedConfig, mgr *Resources) (BatchProcessor, error)

// BatchInputCtor is the constructor signature for a registered
// batch-input component.
type BatchInputCtor = func(conf *ParsedConfig, mgr *Resources) (BatchInput, error)

// BatchOutputCtor is the constructor signature for a registered
// batch-output component.
type BatchOutputCtor = func(conf *ParsedConfig, mgr *Resources) (BatchOutput, BatchPolicy, int, error)

// Re-exported error sentinels matching the benthos ones. Plugin code
// returning these from Connect / ReadBatch / Send round-trips through
// the gRPC layer back to the host's framework.
var (
	ErrNotConnected = service.ErrNotConnected
	ErrEndOfInput   = service.ErrEndOfInput
)
