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

// catshout is a v1-protocol PoC plugin binary that registers two
// processors in a single subprocess:
//
//   - catshout: appends a configurable suffix and uppercases each
//     message in the batch.
//   - reverser: reverses the bytes of each message in the batch.
//
// Compared to an in-tree benthos component, only the import + the
// `service.` ↔ `rpcn.` package prefix change. The ProcessBatch / Close
// method signatures are byte-identical thanks to the rpcn type
// aliases.
package main

import (
	"bytes"
	"context"
	"log"
	"slices"

	rpcn "github.com/redpanda-data/connect/v4/public/plugin/go/rpcn/v1"
)

func main() {
	env := rpcn.NewEnvironment()

	if err := env.RegisterBatchProcessor(
		"catshout",
		rpcn.NewConfigSpec().
			Summary("Uppercases each message and appends a configurable suffix.").
			Field(rpcn.NewStringField("suffix").
				Description("Bytes appended to every message after uppercasing.").
				Default("")),
		newCatshout,
	); err != nil {
		log.Fatalf("registering catshout: %v", err)
	}

	if err := env.RegisterBatchProcessor(
		"reverser",
		rpcn.NewConfigSpec().
			Summary("Reverses the bytes of each message in the batch."),
		newReverser,
	); err != nil {
		log.Fatalf("registering reverser: %v", err)
	}

	if err := rpcn.Serve(env); err != nil {
		log.Fatalf("serving plugin runtime: %v", err)
	}
}

// ----------------------------------------------------------------------
// catshout
// ----------------------------------------------------------------------

func newCatshout(conf *rpcn.ParsedConfig, _ *rpcn.Resources) (rpcn.BatchProcessor, error) {
	suffix, err := conf.FieldString("suffix")
	if err != nil {
		return nil, err
	}
	return &catshoutProcessor{suffix: []byte(suffix)}, nil
}

type catshoutProcessor struct {
	suffix []byte
}

var _ rpcn.BatchProcessor = (*catshoutProcessor)(nil)

func (p *catshoutProcessor) ProcessBatch(_ context.Context, batch rpcn.MessageBatch) ([]rpcn.MessageBatch, error) {
	for _, m := range batch {
		mBytes, err := m.AsBytes()
		if err != nil {
			return nil, err
		}
		m.SetBytes(slices.Concat(
			[]byte("MEOW! "),
			bytes.ToUpper(mBytes),
			p.suffix,
		))
	}
	return []rpcn.MessageBatch{batch}, nil
}

func (*catshoutProcessor) Close(context.Context) error { return nil }

// ----------------------------------------------------------------------
// reverser
// ----------------------------------------------------------------------

func newReverser(_ *rpcn.ParsedConfig, _ *rpcn.Resources) (rpcn.BatchProcessor, error) {
	return &reverserProcessor{}, nil
}

type reverserProcessor struct{}

var _ rpcn.BatchProcessor = (*reverserProcessor)(nil)

func (*reverserProcessor) ProcessBatch(_ context.Context, batch rpcn.MessageBatch) ([]rpcn.MessageBatch, error) {
	for _, m := range batch {
		mBytes, err := m.AsBytes()
		if err != nil {
			return nil, err
		}
		reversed := make([]byte, len(mBytes))
		for i, b := range mBytes {
			reversed[len(mBytes)-1-i] = b
		}
		m.SetBytes(reversed)
	}
	return []rpcn.MessageBatch{batch}, nil
}

func (*reverserProcessor) Close(context.Context) error { return nil }
