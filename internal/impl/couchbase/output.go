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
	"fmt"

	"github.com/couchbase/gocb/v2"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase/client"
)

func outputConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		Version("4.37.0").
		Categories("Integration").
		Summary("Performs operations against Couchbase for each message, allowing you to store or delete data.").
		Description("When inserting, replacing or upserting documents, each must have the `content` property set.\n" + service.OutputPerformanceDocs(true, true)).
		Field(service.NewInterpolatedStringField("id").Description("Document id.").Example(`${! json("id") }`)).
		Field(service.NewBloblangField("content").Description("Document content.").Optional()).
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(client.OperationInsert):  "insert a new document.",
			string(client.OperationRemove):  "delete a document.",
			string(client.OperationReplace): "replace the contents of a document.",
			string(client.OperationUpsert):  "creates a new document if it does not exist, if it does exist then it updates it.",
		}).Description("Couchbase operation to perform.").Default(string(client.OperationUpsert))).
		LintRule(`root = if ((this.operation == "insert" || this.operation == "replace" || this.operation == "upsert") && !this.exists("content")) { [ "content must be set for insert, replace and upsert operations." ] }`).
		Field(service.NewOutputMaxInFlightField()).
		Field(service.NewBatchPolicyField("batching"))
}

func init() {
	service.MustRegisterBatchOutput(
		"couchbase",
		outputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = NewOutput(conf, mgr)
			return
		},
	)
}

// Output is a sink for Couchbase
type Output struct {
	cfg     *couchbaseConfig
	client  *couchbaseClient
	id      *service.InterpolatedString
	content *bloblang.Executor
	op      func(key string, data []byte) gocb.BulkOp
}

// NewOutput returns a new couchbase output based on the provided config
func NewOutput(conf *service.ParsedConfig, mgr *service.Resources) (*Output, error) {
	cl, err := getClientConfig(conf)
	if err != nil {
		return nil, err
	}
	o := &Output{
		cfg: cl,
	}

	if o.id, err = conf.FieldInterpolatedString("id"); err != nil {
		return nil, err
	}

	if conf.Contains("content") {
		if o.content, err = conf.FieldBloblang("content"); err != nil {
			return nil, err
		}
	}

	op, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}
	switch client.Operation(op) {
	case client.OperationRemove:
		o.op = remove
	case client.OperationInsert:
		if o.content == nil {
			return nil, ErrContentRequired
		}
		o.op = insert
	case client.OperationReplace:
		if o.content == nil {
			return nil, ErrContentRequired
		}
		o.op = replace
	case client.OperationUpsert:
		if o.content == nil {
			return nil, ErrContentRequired
		}
		o.op = upsert
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidOperation, op)
	}

	return o, nil
}

// Connect connects to the couchbase cluster
func (o *Output) Connect(ctx context.Context) error {
	client, err := makeClient(o.cfg)
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

// WriteBatch writes out to the couchbase cluster
func (o *Output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	ops := make([]gocb.BulkOp, len(batch))

	var contentExec *service.MessageBatchBloblangExecutor
	if o.content != nil {
		contentExec = batch.BloblangExecutor(o.content)
	}

	// generate query
	for index := range batch {
		// generate id
		k, err := batch.TryInterpolatedString(index, o.id)
		if err != nil {
			return fmt.Errorf("id interpolation error: %w", err)
		}

		// generate content
		var content []byte
		if contentExec != nil {
			res, err := contentExec.Query(index)
			if err != nil {
				return err
			}
			content, err = res.AsBytes()
			if err != nil {
				return err
			}
		}

		ops[index] = o.op(k, content)
	}

	return o.client.collection.Do(ops, &gocb.BulkOpOptions{})
}

// Close closes the connection to the cluster if Connect was successful
func (o *Output) Close(ctx context.Context) error {
	if o.client == nil {
		return nil
	}
	return o.client.Close(ctx)
}
