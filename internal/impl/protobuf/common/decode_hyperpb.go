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

// NewHyperPbParser returns a new ProtobufDecoder based on hyperpb.
func NewHyperPbDecoder(
	md protoreflect.MessageDescriptor,
	opts ProfilingOptions,
) ProtobufDecoder {
	msgType := hyperpb.CompileMessageDescriptor(md)
	parser := &hyperPbParser{}
	parser.state.Store(&hyperPbParserState{
		msgType: msgType,
		profile: msgType.NewProfile(),
	})
	parser.pool.New = func() any {
		return new(hyperpb.Shared)
	}
	parser.opts = opts
	return parser
}

type hyperPbParserState struct {
	msgType *hyperpb.MessageType
	profile *hyperpb.Profile
}

type hyperPbParser struct {
	state atomic.Pointer[hyperPbParserState]
	pool  sync.Pool
	opts  ProfilingOptions
	seen  atomic.Int64
}

var _ ProtobufDecoder = (*hyperPbParser)(nil)

// WithDecoded implements ProtobufParser.
func (p *hyperPbParser) WithDecoded(buf []byte, cb func(msg proto.Message) error) error {
	shared := p.pool.Get().(*hyperpb.Shared)
	defer func() {
		shared.Free()
		p.pool.Put(shared)
	}()
	state := p.state.Load()
	msg := shared.NewMessage(state.msgType)
	if err := msg.Unmarshal(buf, hyperpb.WithRecordProfile(state.profile, p.opts.Rate)); err != nil {
		return err
	}
	if state.profile != nil && p.seen.Add(1)%p.opts.RecompileInterval == 0 {
		// Temporarily disable profiling while we recompile (to prevent races where we recompile multiple at once)
		temp := &hyperPbParserState{msgType: state.msgType}
		success := p.state.CompareAndSwap(state, temp)
		if success {
			// Do recompilation in the background as it can be slow
			go func() {
				recompiled := state.msgType.Recompile(state.profile)
				p.state.Store(&hyperPbParserState{
					msgType: recompiled,
					profile: recompiled.NewProfile(),
				})
			}()

		}
	}
	return cb(msg)
}
