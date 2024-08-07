// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package couchbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocb/v2"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase/client"
)

const (
	// MetaCASKey hold CAS of entry.
	MetaCASKey = "couchbase_cas"
)

var (
	// ErrInvalidOperation specified operation is not supported.
	ErrInvalidOperation = errors.New("invalid operation")
	// ErrContentRequired content field is required.
	ErrContentRequired = errors.New("content required")
)

// ProcessorConfig export couchbase processor specification.
func ProcessorConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		// TODO Stable().
		Version("4.11.0").
		Categories("Integration").
		Summary("Performs operations against Couchbase for each message, allowing you to store or retrieve data within message payloads.").
		Description("When inserting, replacing or upserting documents, each must have the `content` property set. CAS value is stored in meta `couchbase_cas`. It prevent read/write conflict by only allowing write if not modified by other. You can clear the value with `meta couchbase_cas = deleted()` to disable this check.").
		Field(service.NewInterpolatedStringField("id").Description("Document id.").Example(`${! json("id") }`)).
		Field(service.NewBloblangField("content").Description("Document content.").Optional()).
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(client.OperationGet):     "fetch a document.",
			string(client.OperationInsert):  "insert a new document.",
			string(client.OperationRemove):  "delete a document.",
			string(client.OperationReplace): "replace the contents of a document.",
			string(client.OperationUpsert):  "creates a new document if it does not exist, if it does exist then it updates it.",
		}).Description("Couchbase operation to perform.").Default(string(client.OperationGet))).
		LintRule(`root = if ((this.operation == "insert" || this.operation == "replace" || this.operation == "upsert") && !this.exists("content")) { [ "content must be set for insert, replace and upsert operations." ] }`)
}

func init() {
	err := service.RegisterBatchProcessor("couchbase", ProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// Processor stores or retrieves data from couchbase for each message of a
// batch.
type Processor struct {
	*couchbaseClient
	id      *service.InterpolatedString
	content *bloblang.Executor
	op      func(key string, data []byte, cas gocb.Cas) gocb.BulkOp
}

// NewProcessor returns a Couchbase processor.
func NewProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*Processor, error) {
	cl, err := getClient(conf, mgr)
	if err != nil {
		return nil, err
	}
	p := &Processor{
		couchbaseClient: cl,
	}

	if p.id, err = conf.FieldInterpolatedString("id"); err != nil {
		return nil, err
	}

	if conf.Contains("content") {
		if p.content, err = conf.FieldBloblang("content"); err != nil {
			return nil, err
		}
	}

	op, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}
	switch client.Operation(op) {
	case client.OperationGet:
		p.op = get
	case client.OperationRemove:
		p.op = remove
	case client.OperationInsert:
		if p.content == nil {
			return nil, ErrContentRequired
		}
		p.op = insert
	case client.OperationReplace:
		if p.content == nil {
			return nil, ErrContentRequired
		}
		p.op = replace
	case client.OperationUpsert:
		if p.content == nil {
			return nil, ErrContentRequired
		}
		p.op = upsert
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, op)
	}

	return p, nil
}

// ProcessBatch applies the processor to a message batch, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Processor) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	newMsg := inBatch.Copy()
	ops := make([]gocb.BulkOp, len(inBatch))

	var contentExec *service.MessageBatchBloblangExecutor
	if p.content != nil {
		contentExec = inBatch.BloblangExecutor(p.content)
	}

	// generate query
	for index, msg := range newMsg {
		// generate id
		k, err := inBatch.TryInterpolatedString(index, p.id)
		if err != nil {
			return nil, fmt.Errorf("id interpolation error: %w", err)
		}

		// generate content
		var content []byte
		if contentExec != nil {
			res, err := contentExec.Query(index)
			if err != nil {
				return nil, err
			}
			content, err = res.AsBytes()
			if err != nil {
				return nil, err
			}
		}

		var cas gocb.Cas // retrieve cas if set
		if val, ok := msg.MetaGetMut(MetaCASKey); ok {
			if v, ok := val.(gocb.Cas); ok {
				cas = v
			}
		}

		ops[index] = p.op(k, content, cas)
	}

	// execute
	err := p.collection.Do(ops, &gocb.BulkOpOptions{})
	if err != nil {
		return nil, err
	}

	// set results
	for index, part := range newMsg {
		out, cas, err := valueFromOp(ops[index])
		if err != nil {
			part.SetError(fmt.Errorf("couchbase operator failed: %w", err))
		}

		if data, ok := out.([]byte); ok {
			part.SetBytes(data)
		} else if out != nil {
			part.SetStructured(out)
		}

		part.MetaSetMut(MetaCASKey, cas)
	}

	return []service.MessageBatch{newMsg}, nil
}
