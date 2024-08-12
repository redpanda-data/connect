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

package qdrant

import (
	"context"
	"fmt"

	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	poFieldBatching       = "batching"
	poFieldGrpcHost       = "grpc_host"
	poFieldAPIToken       = "api_token"
	poFieldUseTLS         = "tls"
	poFieldCollectionName = "collection_name"
	poFieldID             = "id"
	poFieldVectorMapping  = "vector_mapping"
	poFieldPayloadMapping = "payload_mapping"
)

func outputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.32.2").
		Categories("AI").
		Summary("Adds items to a [Qdrant](https://qdrant.tech/) collection").
		Description(service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(poFieldBatching),
			service.NewStringField(poFieldGrpcHost).
				Description("The gRPC host of the Qdrant collection. Defaults to 'localhost:6334'.").Default("localhost:6334"),
			service.NewStringField(poFieldAPIToken).
				Secret().
				Description("The Qdrant API token for authentication. Defaults to an empty string.").Default(""),
			service.NewTLSToggledField(poFieldUseTLS).Default("TLS(HTTPS) config to use when connecting"),
			service.NewInterpolatedStringField(poFieldCollectionName).
				Description("The name of the collection in Qdrant. REQUIRED"),
			service.NewBloblangField(poFieldID).
				Description("The ID of the point to insert. REQUIRED").
				Example(`root = this.id`).
				Example(`root = 832`).
				Example(`root = "dc88c126-679f-49f5-ab85-04b77e8c2791"`),
			service.NewBloblangField(poFieldVectorMapping).
				Description("The mapping to extract the vector from the document. REQUIRED").
				Example(`root = this.vector`).
				Example(`root = [1.2, 0.5, 0.76]`).
				Example(`root = [[0.352,0.532,0.532,0.234],[0.352,0.532,0.532,0.234]]`).
				Example(`root = {"some_sparse": {"indices":[23,325,532],"values":[0.352,0.532,0.532]}}`).
				Example(`root = {"some_multi": [[0.352,0.532,0.532,0.234],[0.352,0.532,0.532,0.234]]}`).
				Example(`root = {"some_dense": [0.352,0.532,0.532,0.234],"some_sparse": {"indices": [23,325,532],"values": [0.352,0.532,0.532]}}`),
			service.NewBloblangField(poFieldPayloadMapping).
				Optional().
				Description("An optional mapping of message to payload associated with the point.").
				Example(`root = metadata()`).
				Example(`root = {"field": this.value, "field_2": 987}`),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"qdrant",
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

type outputWriter struct {
	client *qdrantClient

	collectionName *service.InterpolatedString
	id             *bloblang.Executor
	vectorMapping  *bloblang.Executor
	payloadMapping *bloblang.Executor
}

func newOutputWriter(conf *service.ParsedConfig, mgr *service.Resources) (*outputWriter, error) {
	collectionName, err := conf.FieldInterpolatedString(poFieldCollectionName)
	if err != nil {
		return nil, err
	}

	host, err := conf.FieldString(poFieldGrpcHost)
	if err != nil {
		return nil, err
	}

	apiToken, err := conf.FieldString(poFieldAPIToken)
	if err != nil {
		return nil, err
	}

	config, enabled, err := conf.FieldTLSToggled(poFieldUseTLS)
	if err != nil {
		return nil, err
	}
	id, err := conf.FieldBloblang(poFieldID)
	if err != nil {
		return nil, err
	}

	vectorMapping, err := conf.FieldBloblang(poFieldVectorMapping)
	if err != nil {
		return nil, err
	}

	payloadMapping, err := conf.FieldBloblang(poFieldPayloadMapping)
	if err != nil {
		return nil, err
	}

	client, err := newQdrantClient(host, apiToken, enabled, config, mgr.Logger())
	if err != nil {
		return nil, err
	}

	w := outputWriter{
		client: client,

		collectionName: collectionName,
		id:             id,
		vectorMapping:  vectorMapping,
		payloadMapping: payloadMapping,
	}
	return &w, nil
}

func (w *outputWriter) Connect(ctx context.Context) error {
	return w.client.Connect(ctx)
}

func (w *outputWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) (err error) {
	batches, err := w.batchPointsByCollection(batch)
	if err != nil {
		return err
	}
	for cn, batch := range batches {
		if err := w.client.Upsert(ctx, cn, batch); err != nil {
			return err
		}
	}
	return nil
}

func (w *outputWriter) batchPointsByCollection(batch service.MessageBatch) (map[string][]*pb.PointStruct, error) {
	cnExec := batch.InterpolationExecutor(w.collectionName)
	idExec := batch.BloblangExecutor(w.id)
	vectorExec := batch.BloblangExecutor(w.vectorMapping)
	payloadExec := batch.BloblangExecutor(w.payloadMapping)
	batches := map[string][]*pb.PointStruct{}
	for i := 0; i < len(batch); i++ {
		collectionName, err := cnExec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("%s interpolation error: %w", poFieldCollectionName, err)
		}
		rawID, err := idExec.Query(i)
		if err != nil {
			return nil, fmt.Errorf("failed to execute %s: %w", poFieldID, err)
		}

		maybeID, err := rawID.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("%s extraction failed: %w", poFieldID, err)
		}

		id, err := newPointID(maybeID)
		if err != nil {
			return nil, fmt.Errorf("failed to coerce point Id type: %w", err)
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
		vec, err := newVectors(maybeVec)
		if err != nil {
			return nil, fmt.Errorf("unable to coerce vector output type: %w", err)
		}

		rawMeta, err := payloadExec.Query(i)
		if err != nil {
			return nil, fmt.Errorf("failed to execute %s: %w", poFieldPayloadMapping, err)
		}

		maybePayload, err := rawMeta.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("%s extraction failed: %w", poFieldPayloadMapping, err)
		}
		payload, err := newValueMap(maybePayload)
		if err != nil {
			return nil, fmt.Errorf("unable to coerce payload output type: %w", err)
		}

		batches[collectionName] = append(batches[collectionName], &pb.PointStruct{
			Id:      id,
			Vectors: vec,
			Payload: payload,
		})
	}
	return batches, nil
}

func (w *outputWriter) Close(ctx context.Context) error {
	if w.client != nil {
		return w.client.Close()
	}
	return nil
}
