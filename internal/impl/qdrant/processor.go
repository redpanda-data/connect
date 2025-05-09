// Copyright 2025 Redpanda Data, Inc.
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

package qdrant

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/qdrant/go-client/qdrant"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	qpFieldGrpcHost       = "grpc_host"
	qpFieldAPIToken       = "api_token"
	qpFieldTLS            = "tls"
	qpFieldCollectionName = "collection_name"
	qpFieldVectorMapping  = "vector_mapping"
	qpFieldFilter         = "filter"
	qpFieldPayloadFields  = "payload_fields"
	qpFieldPayloadFilter  = "payload_filter"
	qpFieldLimit          = "limit"
)

func processorSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("AI").
		Summary("Query items within a https://qdrant.tech/[Qdrant^] collection.").
		Fields(
			service.NewStringField(qpFieldGrpcHost).
				Description("The gRPC host of the Qdrant server.").
				Example("localhost:6334").
				Example("xyz-example.eu-central.aws.cloud.qdrant.io:6334"),
			service.NewStringField(qpFieldAPIToken).
				Secret().
				Description("The Qdrant API token for authentication. Defaults to an empty string.").Default(""),
			service.NewTLSToggledField(qpFieldTLS).Description("TLS(HTTPS) config to use when connecting"),
			service.NewInterpolatedStringField(qpFieldCollectionName).
				Description("The name of the collection in Qdrant."),
			service.NewBloblangField(qpFieldVectorMapping).
				Description("The mapping to extract the search vector from the document.").
				Example(`root = [1.2, 0.5, 0.76]`).
				Example(`root = this.vector`).
				Example(`root = [[0.352,0.532,0.532,0.234],[0.352,0.532,0.532,0.234]]`).
				Example(`root = {"some_sparse": {"indices":[23,325,532],"values":[0.352,0.532,0.532]}}`).
				Example(`root = {"some_multi": [[0.352,0.532,0.532,0.234],[0.352,0.532,0.532,0.234]]}`).
				Example(`root = {"some_dense": [0.352,0.532,0.532,0.234]}`),
			service.NewBloblangField(qpFieldFilter).
				Optional().
				Description("Additional filtering to perform on the results. The mapping should return a valid filter (using the proto3 encoded form) in qdrant. See the https://qdrant.tech/documentation/concepts/filtering/[^Qdrant documentation] for examples.").
				Example(`
root.must = [
	{"has_id":{"has_id":[{"num": 8}, { "uuid":"1234-5678-90ab-cdef" }]}},
	{"field":{"key": "city", "match": {"text": "London"}}},
]
`).Example(`
root.must = [
	{"field":{"key": "city", "match": {"text": "London"}}},
]
root.must_not = [
	{"field":{"color": "city", "match": {"text": "red"}}},
]
`),
			service.NewStringListField(qpFieldPayloadFields).
				Default([]any{}).
				Description("The fields to include or exclude in returned result based on the `payload_filter`."),
			service.NewStringAnnotatedEnumField(qpFieldPayloadFilter, map[string]string{
				"include": "Include the payload fields specified in `payload_fields`.",
				"exclude": "Exclude the payload fields specified in `payload_fields`.",
			}).
				Default("include").
				Description("The way the fields in `payload_fields` are filtered in the result."),
			service.NewIntField(qpFieldLimit).
				Default(10).
				Description("The maximum number of points to return."),
		)
}

func init() {
	err := service.RegisterProcessor(
		"qdrant",
		processorSpec(),
		newProcessor,
	)
	if err != nil {
		panic(err)
	}
}

func newProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	collectionName, err := conf.FieldInterpolatedString(qpFieldCollectionName)
	if err != nil {
		return nil, err
	}

	vectorMapping, err := conf.FieldBloblang(qpFieldVectorMapping)
	if err != nil {
		return nil, err
	}

	var filter *bloblang.Executor
	if conf.Contains(qpFieldFilter) {
		filter, err = conf.FieldBloblang(qpFieldFilter)
		if err != nil {
			return nil, err
		}
	}

	payloadFields, err := conf.FieldStringList(qpFieldPayloadFields)
	if err != nil {
		return nil, err
	}

	payloadFilter, err := conf.FieldString(qpFieldPayloadFilter)
	if err != nil {
		return nil, err
	}

	var payloadSelector *qdrant.WithPayloadSelector
	if payloadFilter == "include" {
		if len(payloadFields) > 0 {
			payloadSelector = qdrant.NewWithPayloadInclude(payloadFields...)
		} else {
			payloadSelector = qdrant.NewWithPayloadEnable(false)
		}
	} else {
		if len(payloadFields) > 0 {
			payloadSelector = qdrant.NewWithPayloadExclude(payloadFields...)
		} else {
			payloadSelector = qdrant.NewWithPayloadEnable(true)
		}
	}

	limit, err := conf.FieldInt(qpFieldLimit)
	if err != nil {
		return nil, err
	}

	host, err := conf.FieldString(qpFieldGrpcHost)
	if err != nil {
		return nil, err
	}

	apiToken, err := conf.FieldString(qpFieldAPIToken)
	if err != nil {
		return nil, err
	}

	tlsConfig, enabled, err := conf.FieldTLSToggled(qpFieldTLS)
	if err != nil {
		return nil, err
	}

	client, err := newQdrantClient(host, apiToken, enabled, tlsConfig, mgr.Logger())
	if err != nil {
		return nil, err
	}
	return &processor{
		client:         client,
		filter:         filter,
		collectionName: collectionName,
		vectorMapping:  vectorMapping,
		payload:        payloadSelector,
		limit:          uint64(limit),
	}, nil
}

type processor struct {
	client *qdrantClient

	collectionName *service.InterpolatedString
	payload        *qdrant.WithPayloadSelector
	vectorMapping  *bloblang.Executor
	filter         *bloblang.Executor
	limit          uint64
}

var _ service.Processor = (*processor)(nil)

// Process implements service.Processor.
func (p *processor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	collection, err := p.collectionName.TryString(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to interpolate `%s`: %w", qpFieldCollectionName, err)
	}
	var filter qdrant.Filter
	if p.filter != nil {
		rawFilter, err := msg.BloblangQuery(p.filter)
		if err != nil {
			return nil, fmt.Errorf("failed to execute `%s`: %w", qpFieldFilter, err)
		}
		b, err := rawFilter.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("%s extraction failed: %w", qpFieldFilter, err)
		}
		if string(b) != `null` {
			if err = protojson.Unmarshal(b, &filter); err != nil {
				return nil, fmt.Errorf("invalid filter, filters should result in JSON data that is parsable into a qdrant Filter proto3 message. Error: %w", err)
			}
		}
	}
	rawVec, err := msg.BloblangQuery(p.vectorMapping)
	if err != nil {
		return nil, fmt.Errorf("failed to execute `%s`: %w", qpFieldVectorMapping, err)
	}
	maybeVec, err := rawVec.AsStructured()
	if err != nil {
		return nil, fmt.Errorf("%s extraction failed: %w", qpFieldVectorMapping, err)
	}
	vec, err := newVectors(maybeVec)
	if err != nil {
		return nil, fmt.Errorf("unable to coerce vector output type: %w", err)
	}
	if len(vec) != 1 {
		return nil, fmt.Errorf("expected only a single vector to search on, got: %d", len(vec))
	}
	var vectorName *string
	var vector *qdrant.VectorInput
	for k, v := range vec {
		if k != "" {
			vectorName = &k
		}
		if v.GetVectorsCount() > 0 {
			var vecs [][]float32
			for chunk := range slices.Chunk(v.Data, int(v.GetVectorsCount())) {
				vecs = append(vecs, chunk)
			}
			vector = qdrant.NewVectorInputMulti(vecs)
		} else if v.Indices != nil {
			vector = qdrant.NewVectorInputSparse(v.Indices.Data, v.Data)
		} else {
			vector = qdrant.NewVectorInputDense(v.Data)
		}
	}
	results, err := p.client.Query(
		ctx,
		collection,
		vectorName,
		vector,
		p.payload,
		&filter,
		p.limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query qdrant: %w", err)
	}
	points := []json.RawMessage{}
	for _, result := range results {
		b, err := protojson.Marshal(result)
		if err != nil {
			return nil, err
		}
		points = append(points, json.RawMessage(b))
	}
	b, err := json.Marshal(points)
	if err != nil {
		return nil, err
	}
	msg = msg.Copy()
	msg.SetBytes(b)
	return service.MessageBatch{msg}, nil
}

// Close implements service.Processor.
func (p *processor) Close(ctx context.Context) error {
	return p.client.Close()
}
