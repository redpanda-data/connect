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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/bulk"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	esFieldURLs            = "urls"
	esFieldID              = "id"
	esFieldAction          = "action"
	esFieldIndex           = "index"
	esFieldPipeline        = "pipeline"
	esFieldRouting         = "routing"
	esFieldRetryOnConflict = "retry_on_conflict"
	esFieldTLS             = "tls"
	esFieldAuth            = "basic_auth"
	esFieldAuthEnabled     = "enabled"
	esFieldAuthUsername    = "username"
	esFieldAuthPassword    = "password"
	esFieldBatching        = "batching"
)

type esConfig struct {
	clientOpts elasticsearch.Config

	action          *service.InterpolatedString
	id              *service.InterpolatedString
	index           *service.InterpolatedString
	pipeline        *service.InterpolatedString
	routing         *service.InterpolatedString
	retryOnConflict int
}

func esConfigFromParsed(pConf *service.ParsedConfig) (*esConfig, error) {
	conf := &esConfig{}

	if os.Getenv("REDPANDA_CONNECT_ELASTICSEARCH_DEBUG") != "" {
		conf.clientOpts.Logger = &elastictransport.CurlLogger{
			Output:             os.Stdout,
			EnableRequestBody:  true,
			EnableResponseBody: true,
		}
	}

	urlStrs, err := pConf.FieldStringList(esFieldURLs)
	if err != nil {
		return nil, err
	}
	for _, u := range urlStrs {
		for _, urlStr := range strings.Split(u, ",") {
			if urlStr != "" {
				conf.clientOpts.Addresses = append(conf.clientOpts.Addresses, urlStr)
			}
		}
	}

	authConf := pConf.Namespace(esFieldAuth)
	if enabled, _ := authConf.FieldBool(esFieldAuthEnabled); enabled {
		if conf.clientOpts.Username, err = authConf.FieldString(esFieldAuthUsername); err != nil {
			return nil, err
		}
		if conf.clientOpts.Password, err = authConf.FieldString(esFieldAuthPassword); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(esFieldTLS)
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		conf.clientOpts.Transport = &http.Transport{
			TLSClientConfig: tlsConf,
		}
	}

	if conf.action, err = pConf.FieldInterpolatedString(esFieldAction); err != nil {
		return nil, err
	}
	if conf.id, err = pConf.FieldInterpolatedString(esFieldID); err != nil {
		return nil, err
	}
	if conf.index, err = pConf.FieldInterpolatedString(esFieldIndex); err != nil {
		return nil, err
	}
	if conf.pipeline, err = pConf.FieldInterpolatedString(esFieldPipeline); err != nil {
		return nil, err
	}
	if conf.routing, err = pConf.FieldInterpolatedString(esFieldRouting); err != nil {
		return nil, err
	}
	if conf.retryOnConflict, err = pConf.FieldInt(esFieldRetryOnConflict); err != nil {
		return nil, err
	}

	return conf, nil
}

func elasticsearchConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Publishes messages into an Elasticsearch index. If the index does not exist then it is created with a dynamic mapping.`).
		Description(`
Both the `+"`id` and `index`"+` fields can be dynamically set using function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here]. When sending batched messages these interpolations are performed per message part.`+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewStringListField(esFieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"http://localhost:9200"}),
			service.NewInterpolatedStringField(esFieldIndex).
				Description("The index to place messages."),
			service.NewInterpolatedStringField(esFieldAction).
				Description("The action to take on the document. This field must resolve to one of the following action types: `index`, `update` or `delete`. See the `Updating Documents` example for more on how the `update` action works."),
			service.NewInterpolatedStringField(esFieldID).
				Description("The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").
				Example(`${!counter()}-${!timestamp_unix()}`),
			service.NewInterpolatedStringField(esFieldPipeline).
				Description("An optional pipeline id to preprocess incoming documents.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(esFieldRouting).
				Description("The routing key to use for the document.").
				Advanced().
				Default(""),
			service.NewIntField(esFieldRetryOnConflict).
				Description("Specify how many times should an update operation be retried when a conflict occurs").
				Advanced().
				Default(0),
			service.NewTLSToggledField(esFieldTLS),
			service.NewOutputMaxInFlightField(),
		).
		Fields(
			service.NewObjectField(esFieldAuth,
				service.NewBoolField(esFieldAuthEnabled).
					Description("Whether to use basic authentication in requests.").
					Default(false),
				service.NewStringField(esFieldAuthUsername).
					Description("A username to authenticate as.").
					Default(""),
				service.NewStringField(esFieldAuthPassword).
					Description("A password to authenticate with.").
					Default("").Secret(),
			).Description("Allows you to specify basic authentication.").
				Advanced().
				Optional(),
			service.NewBatchPolicyField(esFieldBatching),
		).
		Example("Updating Documents", "When updating documents, the request body should contain a combination of a `doc`, `upsert`, and/or `script` fields at the top level, this should be done via mapping processors. `doc` updates using a partial document, `script` performs an update using a scripting language such as the built in Painless language, and `upsert` updates an existing document or inserts a new one if it doesn’t exist. For more information on the structures and behaviors of these fields, please see the https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html[Elasticsearch Update API^]", `
# Partial document update
output:
  processors:
    - mapping: |
        meta id = this.id
        # Performs a partial update ont he document.
        root.doc = this
  elasticsearch_v8:
    urls: [localhost:9200]
    index: foo
    id: ${! @id }
    action: update

# Scripted update
output:
  processors:
    - mapping: |
        meta id = this.id
        # Increments the field "counter" by 1.
        root.script.source = "ctx._source.counter += 1"
  elasticsearch_v8:
    urls: [localhost:9200]
    index: foo
    id: ${! @id }
    action: update

# Upsert
output:
  processors:
    - mapping: |
        meta id = this.id
        # If the product with the ID exists, its price will be updated to 100.
        # If the product does not exist, a new document with ID 1 and a price
        # of 50 will be inserted.
        root.doc.product_price = 50
        root.upsert.product_price = 100
  elasticsearch_v8:
    urls: [localhost:9200]
    index: foo
    id: ${! @id }
    action: update
`).
		Example("Indexing documents from Redpanda", "Here we read messages from a Redpanda cluster and write them to an Elasticsearch index using a field from the message as the ID for the Elasticsearch document.", `
input:
  redpanda:
    seed_brokers: [localhost:19092]
    topics: ["things"]
    consumer_group: "rpcn3"
  processors:
    - mapping: |
        meta id = this.id
        root = this
output:
  elasticsearch_v8:
    urls: ['http://localhost:9200']
    index: "things"
    action: "index"
    id: ${! meta("id") }
`).
		Example("Indexing documents from S3", "Here we read messages from a AWS S3 bucket and write them to an Elasticsearch index using the S3 key as the ID for the Elasticsearch document.", `
input:
  aws_s3:
    bucket: "my-cool-bucket"
    prefix: "bug-facts/"
    scanner:
      to_the_end: {}
output:
  elasticsearch_v8:
    urls: ['http://localhost:9200']
    index: "cool-bug-facts"
    action: "index"
    id: ${! meta("s3_key") }
`)

}

func init() {
	err := service.RegisterBatchOutput("elasticsearch_v8", elasticsearchConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(esFieldBatching); err != nil {
				return
			}
			out, err = outputFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

func outputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*esOutput, error) {
	conf, err := esConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	return &esOutput{
		log:  mgr.Logger(),
		conf: conf,
	}, nil
}

type esOutput struct {
	log  *service.Logger
	conf *esConfig

	client *elasticsearch.TypedClient
}

func (e *esOutput) Connect(ctx context.Context) error {
	if e.client != nil {
		return nil
	}

	client, err := elasticsearch.NewTypedClient(e.conf.clientOpts)
	if err != nil {
		return err
	}

	e.client = client
	return nil
}

func (e *esOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	bulkWriter := e.client.Bulk()
	batchInterpolator := e.newBatchInterpolator(batch)

	for i := range batch {
		if err := e.addOpToBatch(bulkWriter, batch, batchInterpolator, i); err != nil {
			return fmt.Errorf("adding operation to batch: %w", err)
		}
	}

	result, err := bulkWriter.Do(ctx)
	if err != nil {
		return fmt.Errorf("sending bulk request: %w", err)
	}

	if result.Errors {
		var batchErr *service.BatchError
		for i, item := range result.Items {
			for _, responseItem := range item {
				if responseItem.Error != nil {
					err := errors.New(*responseItem.Error.Reason)
					if batchErr == nil {
						batchErr = service.NewBatchError(batch, err)
					}
					batchErr.Failed(i, err)
				}
			}
		}
		return batchErr
	}

	// result.Took is an int64 counting milliseconds
	tookDuration := time.Duration(result.Took) * time.Millisecond

	e.log.Debugf(
		"Successfully dispatched [%s] documents in %s (%s docs/sec)",
		len(result.Items),
		tookDuration,
		float64(len(result.Items))/tookDuration.Seconds(),
	)

	return nil
}

func (e *esOutput) newBatchInterpolator(batch service.MessageBatch) *batchInterpolator {
	return &batchInterpolator{
		action:   batch.InterpolationExecutor(e.conf.action),
		index:    batch.InterpolationExecutor(e.conf.index),
		routing:  batch.InterpolationExecutor(e.conf.routing),
		id:       batch.InterpolationExecutor(e.conf.id),
		pipeline: batch.InterpolationExecutor(e.conf.pipeline),
	}
}

type batchInterpolator struct {
	action   *service.MessageBatchInterpolationExecutor
	index    *service.MessageBatchInterpolationExecutor
	routing  *service.MessageBatchInterpolationExecutor
	id       *service.MessageBatchInterpolationExecutor
	pipeline *service.MessageBatchInterpolationExecutor
}

func (e *esOutput) addOpToBatch(bulkWriter *bulk.Bulk, batch service.MessageBatch, batchInterpolator *batchInterpolator, i int) error {
	msg := batch[i]
	msgBytes, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("reading raw message data: %w", err)
	}

	action, err := batchInterpolator.action.TryString(i)
	if err != nil {
		return fmt.Errorf("interpolating action: %w", err)
	}
	index, err := batchInterpolator.index.TryString(i)
	if err != nil {
		return fmt.Errorf("interpolating index: %w", err)
	}
	routing, err := batchInterpolator.routing.TryString(i)
	if err != nil {
		return fmt.Errorf("interpolating routing: %w", err)
	}
	id, err := batchInterpolator.id.TryString(i)
	if err != nil {
		return fmt.Errorf("interpolating id: %w", err)
	}
	pipeline, err := batchInterpolator.pipeline.TryString(i)
	if err != nil {
		return fmt.Errorf("interpolating pipeline: %w", err)
	}

	switch action {
	case "index":
		op := types.IndexOperation{
			Index_:   &index,
			Id_:      optionalStr(id),
			Pipeline: optionalStr(pipeline),
			Routing:  optionalStr(routing),
		}
		if err := bulkWriter.IndexOp(op, msgBytes); err != nil {
			return err
		}
	case "update":
		op := types.UpdateOperation{
			Id_:     &id,
			Index_:  &index,
			Routing: optionalStr(routing),
		}
		if e.conf.retryOnConflict != 0 {
			op.RetryOnConflict = &e.conf.retryOnConflict
		}
		// We use our own struct here so that users can't specify, intentionally or
		// not, other fields that may alter behavior we depend on internally.
		var update updateAction
		if err := json.Unmarshal(msgBytes, &update); err != nil {
			return fmt.Errorf("unmarshalling update action: %w", err)
		}
		err := bulkWriter.UpdateOp(op, nil, &types.UpdateAction{
			Doc:    update.Doc,
			Script: update.Script,
			Upsert: update.Upsert,
		})
		if err != nil {
			return err
		}
	case "delete":
		op := types.DeleteOperation{
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

type updateAction struct {
	Doc    json.RawMessage `json:"doc"`
	Script *types.Script   `json:"script"`
	Upsert json.RawMessage `json:"upsert"`
}

func optionalStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (e *esOutput) Close(context.Context) error {
	// The client does not need to be closed, as it interacts with Elasticsearch
	// over short lived HTTP connections.
	return nil
}
