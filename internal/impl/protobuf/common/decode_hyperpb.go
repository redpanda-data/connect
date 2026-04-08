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
	"sync"
	"sync/atomic"

	"buf.build/go/hyperpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NewHyperPbDecoder returns a new ProtobufDecoder based on hyperpb.
func NewHyperPbDecoder(
	md protoreflect.MessageDescriptor,
	opts ProfilingOptions,
) ProtobufDecoder {
	msgType := hyperpb.CompileMessageDescriptor(md)
	parser := &hyperPbParser{opts: opts}
	parser.state.Store(newHyperPbParserState(msgType, msgType.NewProfile()))
	return parser
}

// hyperPbParserState bundles a compiled MessageType, its active Profile, and
// the Shared pool that goroutines use while this state is current.
//
// Keeping the pool inside the state means that when the state is atomically
// replaced after a profile-guided recompile, the old pool travels with the old
// state. Once every goroutine that loaded the old state has finished its call
// (returning its Shared back to the old pool), the old state — together with
// its pool and all its Shards — becomes unreachable and can be collected by
// the GC as a unit.
type hyperPbParserState struct {
	msgType *hyperpb.MessageType
	profile *hyperpb.Profile
	pool    *sync.Pool
}

func newHyperPbParserState(msgType *hyperpb.MessageType, profile *hyperpb.Profile) *hyperPbParserState {
	return &hyperPbParserState{
		msgType: msgType,
		profile: profile,
		pool:    &sync.Pool{New: func() any { return new(hyperpb.Shared) }},
	}
}

type hyperPbParser struct {
	state atomic.Pointer[hyperPbParserState]
	opts  ProfilingOptions
	seen  atomic.Int64
}

var _ ProtobufDecoder = (*hyperPbParser)(nil)

// WithDecoded implements ProtobufDecoder.
func (p *hyperPbParser) WithDecoded(buf []byte, cb func(msg proto.Message) error) error {
	// Load state once so that the Shared obtained below is always returned to
	// the same pool it came from, even if the state is swapped mid-call.
	state := p.state.Load()
	shared := state.pool.Get().(*hyperpb.Shared)
	defer func() {
		shared.Free()
		state.pool.Put(shared)
	}()
	msg := shared.NewMessage(state.msgType)
	if err := msg.Unmarshal(buf, hyperpb.WithRecordProfile(state.profile, p.opts.Rate)); err != nil {
		return err
	}
	if state.profile != nil && p.seen.Add(1)%p.opts.RecompileInterval == 0 {
		// Temporarily disable profiling while we recompile (to prevent races
		// where multiple goroutines trigger simultaneous recompiles).
		temp := newHyperPbParserState(state.msgType, nil)
		if p.state.CompareAndSwap(state, temp) {
			// Do recompilation in the background — it can be slow.
			go func() {
				recompiled := state.msgType.Recompile(state.profile)
				next := newHyperPbParserState(recompiled, recompiled.NewProfile())
				p.state.Store(next)
			}()
		}
	}
	return cb(msg)
}
