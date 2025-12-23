// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	elasticsearch_v8 "github.com/elastic/go-elasticsearch/v8"
	elasticsearch_v8_bulk "github.com/elastic/go-elasticsearch/v8/typedapi/core/bulk"
	elasticsearch_v8_types "github.com/elastic/go-elasticsearch/v8/typedapi/types"

	"github.com/redpanda-data/benthos/v4/public/service"
	elasticsearch_common "github.com/redpanda-data/connect/v4/internal/impl/elasticsearch/common"
)

func init() {
	service.MustRegisterBatchOutput("elasticsearch_v8",
		elasticsearch_common.ElasticsearchConfigSpecFromTagName("elasticsearch_v8"),
		elasticsearch_common.BuildBatchOutputConstructor(newBulkWriterBuilderV8))
}

var (
	_ elasticsearch_common.BulkWriterBuilder   = (*bulkWriterBuilderV8)(nil)
	_ elasticsearch_common.BulkWriter          = (*bulkWriterV8)(nil)
	_ elasticsearch_common.BulkWriterConnector = newBulkWriterBuilderV8
)

type bulkWriterBuilderV8 struct {
	client *elasticsearch_v8.TypedClient
}

func newBulkWriterBuilderV8(
	clientOpts elasticsearch_common.ElasticsearchConfig,
) (elasticsearch_common.BulkWriterBuilder, error) {
	client, err := elasticsearch_v8.NewTypedClient(clientOpts.ToV8Configuration())
	if err != nil {
		return nil, err
	}

	return &bulkWriterBuilderV8{
		client: client,
	}, nil
}

func (b *bulkWriterBuilderV8) Bulk() elasticsearch_common.BulkWriter {
	return newBulkWriterV8(b.client)
}

type bulkWriterV8 struct {
	bulkWriter *elasticsearch_v8_bulk.Bulk
}

func newBulkWriterV8(client *elasticsearch_v8.TypedClient) *bulkWriterV8 {
	return &bulkWriterV8{
		bulkWriter: client.Bulk(),
	}
}

func (bw *bulkWriterV8) AddOpToBatch(batch service.MessageBatch,
	batchInterpolator *elasticsearch_common.BatchInterpolator,
	batchIndex int,
	retryOnConflict int,
) error {
	bulkWriter := bw.bulkWriter

	msg := batch[batchIndex]
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("reading raw message data: %w", err)
	}

	action, err := batchInterpolator.Action.TryString(batchIndex)
	if err != nil {
		return fmt.Errorf("interpolating action: %w", err)
	}
	index, err := batchInterpolator.Index.TryString(batchIndex)
	if err != nil {
		return fmt.Errorf("interpolating index: %w", err)
	}
	routing, err := batchInterpolator.Routing.TryString(batchIndex)
	if err != nil {
		return fmt.Errorf("interpolating routing: %w", err)
	}
	id, err := batchInterpolator.Id.TryString(batchIndex)
	if err != nil {
		return fmt.Errorf("interpolating id: %w", err)
	}
	pipeline, err := batchInterpolator.Pipeline.TryString(batchIndex)
	if err != nil {
		return fmt.Errorf("interpolating pipeline: %w", err)
	}

	switch action {
	case "index", "upsert":
		op := elasticsearch_v8_types.IndexOperation{
			Index_:   &index,
			Id_:      optionalStr(id),
			Pipeline: optionalStr(pipeline),
			Routing:  optionalStr(routing),
		}
		if err := bulkWriter.IndexOp(op, msgBytes); err != nil {
			return err
		}
	case "create":
		op := elasticsearch_v8_types.CreateOperation{
			Index_:   &index,
			Id_:      optionalStr(id),
			Pipeline: optionalStr(pipeline),
			Routing:  optionalStr(routing),
		}
		if err := bulkWriter.CreateOp(op, msgBytes); err != nil {
			return err
		}
	case "update":
		op := elasticsearch_v8_types.UpdateOperation{
			Id_:     &id,
			Index_:  &index,
			Routing: optionalStr(routing),
		}
		if retryOnConflict != 0 {
			op.RetryOnConflict = &retryOnConflict
		}
		// We use our own struct here so that users can't specify, intentionally or
		// not, other fields that may alter behavior we depend on internally.
		var update updateAction
		if err := json.Unmarshal(msgBytes, &update); err != nil {
			return fmt.Errorf("unmarshalling update action: %w", err)
		}
		err := bulkWriter.UpdateOp(op, nil, &elasticsearch_v8_types.UpdateAction{
			Doc:    update.Doc,
			Script: update.Script,
			Upsert: update.Upsert,
		})
		if err != nil {
			return err
		}
	case "delete":
		op := elasticsearch_v8_types.DeleteOperation{
			Id_:     &id,
			Index_:  &index,
			Routing: optionalStr(routing),
		}
		if err := bulkWriter.DeleteOp(op); err != nil {
			return err
		}
	}
	return nil
}

func (bw *bulkWriterV8) Do(ctx context.Context) (result *elasticsearch_common.Result, err error) {
	response, err := bw.bulkWriter.Do(ctx)
	if err != nil {
		return nil, err
	}

	results := make([]error, len(response.Items))

	for index, item := range response.Items {
		var errs []error
		for _, responseItem := range item {
			if responseItem.Error != nil {
				errs = append(errs, errors.New(*responseItem.Error.Reason))
			}
		}

		results[index] = errors.Join(errs...)
	}

	return &elasticsearch_common.Result{
		Errors:  response.Errors,
		Results: results,
		// response.Took is an int64 counting milliseconds
		Took: time.Duration(response.Took) * time.Millisecond,
	}, nil
}

type updateAction struct {
	Doc    json.RawMessage                `json:"doc"`
	Script *elasticsearch_v8_types.Script `json:"script"`
	Upsert json.RawMessage                `json:"upsert"`
}

func optionalStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
