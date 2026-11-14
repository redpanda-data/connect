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

package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v3/opensearchutil"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/impl/elasticsearch/esoutput"
)

const (
	esoFieldURLs                = esoutput.FieldURLs
	esoFieldID                  = esoutput.FieldID
	esoFieldAction              = "action"
	esoFieldIndex               = esoutput.FieldIndex
	esoFieldPipeline            = esoutput.FieldPipeline
	esoFieldRouting             = esoutput.FieldRouting
	esoFieldTLS                 = "tls"
	esoFieldAuth                = esoutput.FieldBasicAuth
	esoFieldAuthEnabled         = esoutput.FieldBasicAuthEnabled
	esoFieldAuthUsername        = esoutput.FieldBasicAuthUsername
	esoFieldAuthPassword        = esoutput.FieldBasicAuthPassword
	esoFieldBatching            = "batching"
	esoFieldAWS                 = "aws"
	esoFieldCompressRequestBody = "compress_request_body"
	esoFieldPerMessageErrors    = "per_message_errors"
	// ESOFieldAWSEnabled enabled field.
	ESOFieldAWSEnabled = "enabled"
)

func notImportedAWSOptFn(conf *service.ParsedConfig, _ *opensearchapi.Config) error {
	if enabled, _ := conf.FieldBool(ESOFieldAWSEnabled); !enabled {
		return nil
	}
	return errors.New("unable to configure AWS authentication as this binary does not import components/aws")
}

// AWSOptFn is populated with the child `aws` package when imported.
var AWSOptFn = notImportedAWSOptFn

// AWSField represents the aws block within an elasticsearch field. This is
// exported in order to make unit testing easier within the aws subpackage.
func AWSField() *service.ConfigField {
	return service.NewObjectField(esoFieldAWS,
		append([]*service.ConfigField{
			service.NewBoolField(ESOFieldAWSEnabled).
				Description("Whether to connect to Amazon Elastic Service.").
				Default(false),
		}, config.SessionFields()...)...).
		Description("Enables and customises connectivity to Amazon Elastic Service.").
		Advanced()
}

type esoConfig struct {
	clientOpts opensearchapi.Config

	actionStr   *service.InterpolatedString
	idStr       *service.InterpolatedString
	indexStr    *service.InterpolatedString
	pipelineStr *service.InterpolatedString
	routingStr  *service.InterpolatedString

	perMessageErrors bool
}

func esoConfigFromParsed(pConf *service.ParsedConfig) (conf esoConfig, err error) {
	conf.clientOpts = opensearchapi.Config{}

	var tmpURLs []string
	if tmpURLs, err = pConf.FieldStringList(esoFieldURLs); err != nil {
		return
	}
	for _, u := range tmpURLs {
		for splitURL := range strings.SplitSeq(u, ",") {
			if splitURL != "" {
				conf.clientOpts.Client.Addresses = append(conf.clientOpts.Client.Addresses, splitURL)
			}
		}
	}

	{
		authConf := pConf.Namespace(esoFieldAuth)
		if enabled, _ := authConf.FieldBool(esoFieldAuthEnabled); enabled {
			if conf.clientOpts.Client.Username, err = authConf.FieldString(esoFieldAuthUsername); err != nil {
				return
			}
			if conf.clientOpts.Client.Password, err = authConf.FieldString(esoFieldAuthPassword); err != nil {
				return
			}
		}
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(esoFieldTLS); err != nil {
		return
	} else if tlsEnabled {
		conf.clientOpts.Client.Transport = &http.Transport{
			TLSClientConfig: tlsConf,
		}
	}

	if conf.clientOpts.Client.CompressRequestBody, err = pConf.FieldBool(esoFieldCompressRequestBody); err != nil {
		return
	}
	if conf.perMessageErrors, err = pConf.FieldBool(esoFieldPerMessageErrors); err != nil {
		return
	}

	if conf.actionStr, err = pConf.FieldInterpolatedString(esoFieldAction); err != nil {
		return
	}
	if conf.idStr, err = pConf.FieldInterpolatedString(esoFieldID); err != nil {
		return
	}
	if conf.indexStr, err = pConf.FieldInterpolatedString(esoFieldIndex); err != nil {
		return
	}
	if conf.pipelineStr, err = pConf.FieldInterpolatedString(esoFieldPipeline); err != nil {
		return
	}
	if conf.routingStr, err = pConf.FieldInterpolatedString(esoFieldRouting); err != nil {
		return
	}

	if err = AWSOptFn(pConf.Namespace(esoFieldAWS), &conf.clientOpts); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

// OutputSpec returns the config spec for an elasticsearch output writer.
func OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Publishes messages into an Elasticsearch index. If the index does not exist then it is created with a dynamic mapping.`).
		Description(`
Both the `+"`id` and `index`"+` fields can be dynamically set using function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here]. When sending batched messages these interpolations are performed per message part.`+service.OutputPerformanceDocs(true, true)).Version("4.25.0").
		Fields(
			esoutput.URLsField(),
			esoutput.IndexField("OpenSearch"),
			service.NewInterpolatedStringField(esoFieldAction).
				Description(`The action to take on the document. This field must resolve to one of the following action types: `+"`"+`index`+"`"+`, `+"`"+`update`+"`"+` or `+"`"+`delete`+"`"+`.`).
				ShortDescription("The action to take on the document: index, update or delete."),
			esoutput.IDField(),
			esoutput.PipelineField(),
			esoutput.RoutingField(),
			service.NewTLSToggledField(esoFieldTLS),
			service.NewBoolField(esoFieldCompressRequestBody).
				Description("Enable gzip compression of HTTP request bodies on the OpenSearch client. OpenSearch 2.x servers accept `Content-Encoding: gzip` natively; this is useful to reduce bandwidth on cross-cloud or cross-region bulk writes.").
				Default(false).
				Advanced(),
			service.NewBoolField(esoFieldPerMessageErrors).
				Description("Whether to report as failed only the messages of a batch that were not written. The output can split each batch into several bulk requests. By default, when one of those requests fails as a whole, for example with a `429 Too Many Requests` response, every message of the batch is reported as failed, including the messages that the other requests wrote, so they are retried and written again. When enabled, the output tracks the result of each message and reports as failed only the messages that OpenSearch rejected, the messages of the requests that failed, and any message that the bulk response has no result for. Inputs with `auto_replay_nacks` enabled, and other components that handle errors per message, then retry only those messages. Tracking the results adds a little work for each message.").
				ShortDescription("Report as failed only the messages of a batch that were not written, rather than the whole batch.").
				Default(false).
				Advanced().
				Version("4.119.0"),
			service.NewOutputMaxInFlightField(),
		).
		Fields(
			esoutput.BasicAuthField("OpenSearch"),
			service.NewBatchPolicyField(esoFieldBatching),
			AWSField(),
		).
		Example("Updating Documents", "When https://opensearch.org/docs/latest/api-reference/document-apis/update-document/[updating documents^] the request body should contain a combination of a `doc`, `upsert`, and/or `script` fields at the top level, this should be done via mapping processors.", `
output:
  processors:
    - mapping: |
        meta id = this.id
        root.doc = this
  opensearch:
    urls: [ TODO ]
    index: foo
    id: ${! @id }
    action: update
`)
}

func init() {
	service.MustRegisterBatchOutput("opensearch", OutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(esoFieldBatching); err != nil {
				return
			}
			out, err = OutputFromParsed(conf, mgr)
			return
		})
}

// Output implements service.BatchOutput for elasticsearch.
type Output struct {
	log  *service.Logger
	conf esoConfig

	// Zero values keep the opensearch-go defaults. Tests set them to make the
	// split of a batch into bulk requests deterministic.
	bulkNumWorkers int
	bulkFlushBytes int

	client *opensearchapi.Client
}

// OutputFromParsed returns an elasticsearch output writer from a parsed config.
func OutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*Output, error) {
	conf, err := esoConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	return &Output{
		log:  mgr.Logger(),
		conf: conf,
	}, nil
}

//------------------------------------------------------------------------------

// Connect attempts to connect to the server.
func (e *Output) Connect(context.Context) error {
	if e.client != nil {
		return nil
	}

	client, err := opensearchapi.NewClient(e.conf.clientOpts)
	if err != nil {
		return err
	}

	e.client = client
	return nil
}

type pendingBulkIndex struct {
	Action   string
	Index    string
	Pipeline string
	Routing  string
	Payload  []byte
	ID       string
}

// errNoBulkResult fails a message that the bulk response has no result for,
// so that it is retried rather than acknowledged without a confirmation.
var errNoBulkResult = errors.New("bulk response has no result for the message")

// bulkResults collects what the bulk indexer reports for the messages of a
// batch. The indexer calls back from its worker goroutines and from Close.
type bulkResults struct {
	mu         sync.Mutex
	batch      service.MessageBatch
	requestErr error
	// The indexer reports each failed request twice, so the different
	// request errors are counted by their text.
	requestErrTexts map[string]struct{} // only with per_message_errors
	itemsErr        *service.BatchError // only without per_message_errors
	items           []bulkItemResult    // only with per_message_errors
}

type bulkItemResult struct {
	written bool
	err     error
}

func newBulkResults(batch service.MessageBatch, perMessageErrors bool) *bulkResults {
	r := &bulkResults{batch: batch}
	if perMessageErrors {
		r.items = make([]bulkItemResult, len(batch))
	}
	return r
}

func (r *bulkResults) requestFailed(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestErr = err
	if r.items == nil {
		return
	}
	if r.requestErrTexts == nil {
		r.requestErrTexts = map[string]struct{}{}
	}
	r.requestErrTexts[err.Error()] = struct{}{}
}

func (r *bulkResults) itemWritten(i int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.items == nil {
		return
	}
	r.items[i].written = true
}

func (r *bulkResults) itemFailed(i int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.items != nil {
		r.items[i].err = err
		return
	}
	if r.itemsErr == nil {
		r.itemsErr = service.NewBatchError(r.batch, err)
	}
	r.itemsErr.Failed(i, err)
}

// batchErr returns nil when no message failed. The indexer reports a failed
// request without the items it carried, so without per_message_errors that
// fails the whole batch. With it, a message is only acknowledged when the
// bulk response confirms it. A message without a result cannot be traced to
// its request, so it fails with the request error when every failed request
// reported the same one, with an error that counts the different ones and
// wraps the last otherwise, or with errNoBulkResult when no request failed.
func (r *bulkResults) batchErr() *service.BatchError {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.items == nil {
		if r.requestErr != nil {
			return service.NewBatchError(r.batch, r.requestErr)
		}
		return r.itemsErr
	}

	noResultErr := r.requestErr
	switch n := len(r.requestErrTexts); {
	case n == 0:
		noResultErr = errNoBulkResult
	case n > 1:
		noResultErr = fmt.Errorf("bulk requests failed with %d different errors, last: %w", n, r.requestErr)
	}

	var bErr *service.BatchError
	for i, item := range r.items {
		err := item.err
		if err == nil && !item.written {
			err = noResultErr
		}
		if err == nil {
			continue
		}
		if bErr == nil {
			headline := err
			if r.requestErr != nil {
				headline = noResultErr
			}
			bErr = service.NewBatchError(r.batch, headline)
		}
		bErr.Failed(i, err)
	}
	return bErr
}

// WriteBatch writes a message batch to the output.
func (e *Output) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	if e.client == nil {
		return service.ErrNotConnected
	}

	requests := make([]*pendingBulkIndex, len(msg))

	for i := range msg {
		rawBytes, ierr := msg[i].AsBytes()
		if ierr != nil {
			e.log.Errorf("Failed to obtain message raw data: %v\n", ierr)
			return fmt.Errorf("obtaining message raw data: %w", ierr)
		}

		pbi := &pendingBulkIndex{Payload: rawBytes}
		if pbi.Action, ierr = msg.TryInterpolatedString(i, e.conf.actionStr); ierr != nil {
			return fmt.Errorf("action interpolation error: %w", ierr)
		}
		if pbi.Index, ierr = msg.TryInterpolatedString(i, e.conf.indexStr); ierr != nil {
			return fmt.Errorf("index interpolation error: %w", ierr)
		}
		if pbi.Pipeline, ierr = msg.TryInterpolatedString(i, e.conf.pipelineStr); ierr != nil {
			return fmt.Errorf("pipeline interpolation error: %w", ierr)
		}
		if pbi.Routing, ierr = msg.TryInterpolatedString(i, e.conf.routingStr); ierr != nil {
			return fmt.Errorf("routing interpolation error: %w", ierr)
		}
		if pbi.ID, ierr = msg.TryInterpolatedString(i, e.conf.idStr); ierr != nil {
			return fmt.Errorf("id interpolation error: %w", ierr)
		}
		requests[i] = pbi
	}

	results := newBulkResults(msg, e.conf.perMessageErrors)

	start := time.Now()
	b, _ := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client:     e.client,
		NumWorkers: e.bulkNumWorkers,
		FlushBytes: e.bulkFlushBytes,
		OnError: func(_ context.Context, err error) {
			results.requestFailed(err)
		},
	})

	for i, v := range requests {
		bulkReq, err := buildBulkableRequest(v, func(err error) {
			results.itemFailed(i, err)
		})
		if err != nil {
			return err
		}
		if e.conf.perMessageErrors {
			bulkReq.OnSuccess = func(context.Context, opensearchutil.BulkIndexerItem, opensearchapi.BulkRespItem) {
				results.itemWritten(i)
			}
		}
		if err = b.Add(ctx, *bulkReq); err != nil {
			return err
		}
	}

	if err := b.Close(ctx); err != nil {
		return err
	}

	if bErr := results.batchErr(); bErr != nil {
		if e.conf.perMessageErrors {
			e.log.Debugf("Reporting %d of %d messages as failed", bErr.IndexedErrors(), len(msg))
		}
		return bErr
	}

	biStats := b.Stats()
	dur := time.Since(start)

	e.log.Debugf(
		"Successfully dispatched [%d] documents in %s (%.2f docs/sec)",
		biStats.NumFlushed,
		dur.Truncate(time.Millisecond),
		1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed),
	)
	return nil
}

// Close closes the output.
func (*Output) Close(context.Context) error {
	return nil
}

// Build a bulkable request for a given pending bulk index item.
func buildBulkableRequest(p *pendingBulkIndex, onError func(err error)) (r *opensearchutil.BulkIndexerItem, err error) {
	switch p.Action {
	case "update":
		r = &opensearchutil.BulkIndexerItem{
			Index:  p.Index,
			Action: "update",
			Body:   bytes.NewReader(p.Payload),
		}
		if p.ID != "" {
			r.DocumentID = p.ID
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}
	case "delete":
		r = &opensearchutil.BulkIndexerItem{
			Index:      p.Index,
			DocumentID: p.ID,
			Action:     "delete",
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}
	case "index":
		r = &opensearchutil.BulkIndexerItem{
			Index:  p.Index,
			Action: "index",
			Body:   bytes.NewReader(p.Payload),
		}
		if p.ID != "" {
			r.DocumentID = p.ID
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}
	default:
		return nil, fmt.Errorf("opensearch action '%s' is not allowed", p.Action)
	}

	r.OnFailure = func(
		_ context.Context,
		_ opensearchutil.BulkIndexerItem,
		biri opensearchapi.BulkRespItem,
		err error,
	) {
		if err == nil {
			if biri.Error.Type == "" {
				biri.Error.Type = fmt.Sprintf("status %v", biri.Status)
			}
			err = fmt.Errorf("%v: %v", biri.Error.Type, biri.Error.Reason)
		}
		onError(err)
	}
	return
}
