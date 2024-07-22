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

package pinecone

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	poFieldBatching        = "batching"
	poFieldHost            = "host"
	poFieldAPIKey          = "api_key"
	poFieldNamespace       = "namespace"
	poFieldID              = "id"
	poFieldOp              = "operation"
	poFieldVectorMapping   = "vector_mapping"
	poFieldMetadataMapping = "metadata_mapping"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.31.0").
		Categories("AI").
		Summary("Inserts items into a Pinecone index.").
		Description(service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(poFieldBatching),
			service.NewStringField(poFieldHost).
				Description("The host for the Pinecone index."),
			service.NewStringField(poFieldAPIKey).
				Secret().
				Description("The Pinecone api key."),
			service.NewStringEnumField(poFieldOp, string(operationUpdate), string(operationUpsert), string(operationDelete)).
				Default(string(operationUpsert)).
				Description("The operation to perform against the Pinecone index."),
			service.NewInterpolatedStringField(poFieldNamespace).
				Default("").
				Advanced().
				Description("The namespace to write to - writes to the default namespace by default."),
			service.NewInterpolatedStringField(poFieldID).
				Description("The ID for the index entry in Pinecone."),
			service.NewBloblangField(poFieldVectorMapping).
				Optional().
				Description("The mapping to extract out the vector from the document. The result must be a floating point array. Required if not a delete operation."),
			service.NewBloblangField(poFieldMetadataMapping).
				Optional().
				Description("An optional mapping of message to metadata in the Pinecone index entry."),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"pinecone",
		outputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(poFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newOutputWriter(conf, mgr); err != nil {
				return
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

type operation string

const (
	operationUpdate operation = "update-vector"
	operationUpsert operation = "upsert-vectors"
	operationDelete operation = "delete-vectors"
)

type outputWriter struct {
	client client
	host   string
	op     operation
	logger *service.Logger

	namespace       *service.InterpolatedString
	id              *service.InterpolatedString
	vectorMapping   *bloblang.Executor
	metadataMapping *bloblang.Executor

	pool sync.Pool
}

func newOutputWriter(conf *service.ParsedConfig, mgr *service.Resources) (*outputWriter, error) {
	k, err := conf.FieldString(poFieldAPIKey)
	if err != nil {
		return nil, err
	}
	pc, err := pinecone.NewClient(pinecone.NewClientParams{
		ApiKey: k,
	})
	if err != nil {
		return nil, err
	}
	rawOp, err := conf.FieldString(poFieldOp)
	if err != nil {
		return nil, err
	}
	var op operation
	switch rawOp {
	case string(operationUpsert):
		op = operationUpsert
	case string(operationUpdate):
		op = operationUpdate
	case string(operationDelete):
		op = operationDelete
	default:
		return nil, fmt.Errorf("invalid operation: %s", rawOp)
	}
	host, err := conf.FieldString(poFieldHost)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(host, "https://") {
		return nil, fmt.Errorf("host field must be a FQDN not a URL: %q (remove the https:// prefix)", host)
	}
	id, err := conf.FieldInterpolatedString(poFieldID)
	if err != nil {
		return nil, err
	}
	ns, err := conf.FieldInterpolatedString(poFieldNamespace)
	if err != nil {
		return nil, err
	}
	var vectorMapping *bloblang.Executor
	var metadataMapping *bloblang.Executor
	if op != operationDelete {
		vectorMapping, err = conf.FieldBloblang(poFieldVectorMapping)
		if err != nil {
			return nil, err
		}
		if conf.Contains(poFieldMetadataMapping) {
			metadataMapping, err = conf.FieldBloblang(poFieldMetadataMapping)
			if err != nil {
				return nil, err
			}
		}
	}
	w := outputWriter{
		client:          &realClient{pc},
		host:            host,
		op:              op,
		logger:          mgr.Logger(),
		namespace:       ns,
		id:              id,
		vectorMapping:   vectorMapping,
		metadataMapping: metadataMapping,
	}
	return &w, nil
}

func (w *outputWriter) Connect(ctx context.Context) error {
	w.logger.Tracef("Connecting to %s", w.host)
	c, err := w.client.Index(w.host)
	if err != nil {
		w.logger.Tracef("error connecting to %s: %w", w.host, err)
		return err
	}
	w.logger.Tracef("Connected to %s", w.host)
	w.pool.Put(c)
	return nil
}

func (w *outputWriter) acquireClient() (indexClient, error) {
	if i := w.pool.Get(); i != nil {
		return i.(indexClient), nil
	} else {
		return w.client.Index(w.host)
	}
}

func (w *outputWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) (err error) {
	var c indexClient
	c, err = w.acquireClient()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			w.pool.Put(c)
		} else {
			_ = c.Close()
		}
	}()
	switch w.op {
	case operationUpdate:
		err = w.UpdateBatch(ctx, c, batch)
	case operationUpsert:
		err = w.UpsertBatch(ctx, c, batch)
	case operationDelete:
		err = w.DeleteBatch(ctx, c, batch)
	default:
		err = fmt.Errorf("unknown operation: %s", w.op)
	}
	return
}

func (w *outputWriter) UpdateBatch(ctx context.Context, ic indexClient, batch service.MessageBatch) error {
	batches, err := w.computeBatchedVectors(batch)
	if err != nil {
		return err
	}
	for ns, batch := range batches {
		ic.SetNamespace(ns)
		for _, msg := range batch {
			var req pinecone.UpdateVectorRequest
			req.Id = msg.Id
			req.Values = msg.Values
			req.SparseValues = msg.SparseValues
			req.Metadata = msg.Metadata
			if err := ic.UpdateVector(ctx, &req); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *outputWriter) UpsertBatch(ctx context.Context, ic indexClient, batch service.MessageBatch) error {
	batches, err := w.computeBatchedVectors(batch)
	if err != nil {
		return err
	}
	for ns, batch := range batches {
		ic.SetNamespace(ns)
		if err := ic.UpsertVectors(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (w *outputWriter) computeBatchedVectors(batch service.MessageBatch) (map[string][]*pinecone.Vector, error) {
	nsExec := batch.InterpolationExecutor(w.namespace)
	idExec := batch.InterpolationExecutor(w.id)
	vectorExec := batch.BloblangExecutor(w.vectorMapping)
	var metaExec *service.MessageBatchBloblangExecutor
	if w.metadataMapping != nil {
		metaExec = batch.BloblangExecutor(w.metadataMapping)
	}
	batches := map[string][]*pinecone.Vector{}
	for i := 0; i < len(batch); i++ {
		ns, err := nsExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", poFieldNamespace, err)
		}
		id, err := idExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", poFieldID, err)
		}
		rawVec, err := vectorExec.Query(i)
		if err != nil {
			return nil, fmt.Errorf("failed to execute %s: %w", poFieldVectorMapping, err)
		}
		if rawVec == nil {
			continue
		}
		maybeVec, err := rawVec.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("%s extraction failed: %w", poFieldVectorMapping, err)
		}
		var values []float32
		switch vec := maybeVec.(type) {
		case []float32:
			values = vec
		case []float64:
			values = make([]float32, len(vec))
			for i, v := range vec {
				values[i] = float32(v)
			}
		case []any:
			values = make([]float32, len(vec))
			for i, v := range vec {
				values[i], err = bloblang.ValueAsFloat32(v)
				if err != nil {
					return nil, fmt.Errorf("unable to coerce vector output type: %w", err)
				}
			}
		default:
			return nil, fmt.Errorf("unable to coerce vector output type from %T", vec)
		}
		var rawMeta *service.Message
		if metaExec != nil {
			rawMeta, err = metaExec.Query(i)
			if err != nil {
				return nil, fmt.Errorf("failed to execute %s: %w", poFieldMetadataMapping, err)
			}
		}
		var meta *pinecone.Metadata
		if rawMeta != nil {
			b, err := rawMeta.AsBytes()
			if err != nil {
				return nil, fmt.Errorf("failed to extract %s bytes: %w", poFieldMetadataMapping, err)
			}
			var m pinecone.Metadata
			if err := m.UnmarshalJSON(b); err != nil {
				return nil, fmt.Errorf("failed to convert %s to Pinecone metadata: %w", poFieldMetadataMapping, err)
			}
			meta = &m
		}
		vectors := batches[ns]
		vectors = append(vectors, &pinecone.Vector{
			Id:       id,
			Values:   values,
			Metadata: meta,
		})
		batches[ns] = vectors
	}
	return batches, nil
}

func (w *outputWriter) DeleteBatch(ctx context.Context, ic indexClient, batch service.MessageBatch) error {
	nsExec := batch.InterpolationExecutor(w.namespace)
	idExec := batch.InterpolationExecutor(w.id)
	batches := map[string][]string{}
	for i := 0; i < len(batch); i++ {
		ns, err := nsExec.TryString(i)
		if err != nil {
			return fmt.Errorf("%s interpolation error: %w", poFieldNamespace, err)
		}
		id, err := idExec.TryString(i)
		if err != nil {
			return fmt.Errorf("%s interpolation error: %w", poFieldID, err)
		}
		ids := batches[ns]
		ids = append(ids, id)
		batches[ns] = ids
	}
	for ns, ids := range batches {
		ic.SetNamespace(ns)
		if err := ic.DeleteVectorsByID(ctx, ids); err != nil {
			return err
		}
	}
	return nil
}

func (w *outputWriter) Close(ctx context.Context) error {
	for {
		item := w.pool.Get()
		if item == nil {
			return nil
		}
		c := item.(indexClient)
		if err := c.Close(); err != nil {
			return err
		}
	}
}
