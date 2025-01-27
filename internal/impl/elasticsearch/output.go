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

package elasticsearch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/olivere/elastic/v7"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/retries"
)

const (
	esoFieldURLs            = "urls"
	esoFieldSniff           = "sniff"
	esoFieldHealthcheck     = "healthcheck"
	esoFieldID              = "id"
	esoFieldAction          = "action"
	esoFieldIndex           = "index"
	esoFieldPipeline        = "pipeline"
	esoFieldRouting         = "routing"
	esoFieldRetryOnConflict = "retry_on_conflict"
	esoFieldType            = "type"
	esoFieldTimeout         = "timeout"
	esoFieldTLS             = "tls"
	esoFieldAuth            = "basic_auth"
	esoFieldAuthEnabled     = "enabled"
	esoFieldAuthUsername    = "username"
	esoFieldAuthPassword    = "password"
	esoFieldAPIKey          = "api_key"
	esoFieldAWS             = "aws"
	// ESOFieldAWSEnabled enabled field.
	ESOFieldAWSEnabled      = "enabled"
	esoFieldGzipCompression = "gzip_compression"
	esoFieldBatching        = "batching"
)

type esoConfig struct {
	urls []string

	clientOpts  []elastic.ClientOptionFunc
	backoffCtor func() backoff.BackOff

	actionStr   *service.InterpolatedString
	idStr       *service.InterpolatedString
	indexStr    *service.InterpolatedString
	pipelineStr *service.InterpolatedString
	routingStr  *service.InterpolatedString
	typeStr     *service.InterpolatedString
}

func esoConfigFromParsed(pConf *service.ParsedConfig) (conf esoConfig, err error) {
	var tmpURLs []string
	if tmpURLs, err = pConf.FieldStringList(esoFieldURLs); err != nil {
		return
	}
	for _, u := range tmpURLs {
		for _, splitURL := range strings.Split(u, ",") {
			if splitURL != "" {
				conf.urls = append(conf.urls, splitURL)
			}
		}
	}

	var sniff, healthCheck bool
	if sniff, err = pConf.FieldBool(esoFieldSniff); err != nil {
		return
	}
	if healthCheck, err = pConf.FieldBool(esoFieldHealthcheck); err != nil {
		return
	}
	conf.clientOpts = []elastic.ClientOptionFunc{
		elastic.SetURL(conf.urls...),
		elastic.SetSniff(sniff),
		elastic.SetHealthcheck(healthCheck),
	}

	{
		authConf := pConf.Namespace(esoFieldAuth)
		if enabled, _ := authConf.FieldBool(esoFieldAuthEnabled); enabled {
			var username, password string
			if username, err = authConf.FieldString(esoFieldAuthUsername); err != nil {
				return
			}
			if password, err = authConf.FieldString(esoFieldAuthPassword); err != nil {
				return
			}
			conf.clientOpts = append(conf.clientOpts, elastic.SetBasicAuth(username, password))
		}
	}

	if pConf.Contains(esoFieldAPIKey) {
		var apiKey string
		apiKey, err = pConf.FieldString(esoFieldAPIKey)
		if err != nil {
			return
		}
		header := http.Header{}
		header.Set("Authorization", "ApiKey "+apiKey)
		conf.clientOpts = append(conf.clientOpts, elastic.SetHeaders(header))
	}

	var timeout time.Duration
	if timeout, err = pConf.FieldDuration(esoFieldTimeout); err != nil {
		return
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(esoFieldTLS); err != nil {
		return
	} else if tlsEnabled {
		conf.clientOpts = append(conf.clientOpts, elastic.SetHttpClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConf,
			},
			Timeout: timeout,
		}))
	} else {
		conf.clientOpts = append(conf.clientOpts, elastic.SetHttpClient(&http.Client{
			Timeout: timeout,
		}))
	}

	var awsOpts []elastic.ClientOptionFunc
	if awsOpts, err = AWSOptFn(pConf.Namespace(esoFieldAWS)); err != nil {
		return
	}
	conf.clientOpts = append(conf.clientOpts, awsOpts...)

	var gzipCompression bool
	if gzipCompression, err = pConf.FieldBool(esoFieldGzipCompression); err != nil {
		return
	}
	if gzipCompression {
		conf.clientOpts = append(conf.clientOpts, elastic.SetGzip(true))
	}

	if conf.backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
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
	if conf.typeStr, err = pConf.FieldInterpolatedString(esoFieldType); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

func notImportedAWSOptFn(conf *service.ParsedConfig) ([]elastic.ClientOptionFunc, error) {
	if enabled, _ := conf.FieldBool(ESOFieldAWSEnabled); !enabled {
		return nil, nil
	}
	return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
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

//------------------------------------------------------------------------------

// OutputSpec returns the config spec for an elasticsearch output writer.
func OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Publishes messages into an Elasticsearch index. If the index does not exist then it is created with a dynamic mapping.`).
		Description(`
Both the `+"`id` and `index`"+` fields can be dynamically set using function interpolations described in xref:configuration:interpolation.adoc#bloblang-queries[Bloblang queries]. When sending batched messages these interpolations are performed per message part.

== AWS

It's possible to enable AWS connectivity with this output using the `+"`aws`"+` fields. However, you may need to set `+"`sniff` and `healthcheck`"+` to false for connections to succeed.`+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewStringListField(esoFieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"http://localhost:9200"}),
			service.NewInterpolatedStringField(esoFieldIndex).
				Description("The index to place messages."),
			service.NewInterpolatedStringField(esoFieldAction).
				Description("The action to take on the document. This field must resolve to one of the following action types: `create`, `index`, `update`, `upsert` or `delete`.").
				Default("index").
				Advanced(),
			service.NewInterpolatedStringField(esoFieldPipeline).
				Description("An optional pipeline id to preprocess incoming documents.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(esoFieldID).
				Description("The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").
				Default(`${!counter()}-${!timestamp_unix()}`),
			service.NewInterpolatedStringField(esoFieldType).
				Description("The document mapping type. This field is required for versions of elasticsearch earlier than 6.0.0, but are invalid for versions 7.0.0 or later.").
				Default(""),
			service.NewInterpolatedStringField(esoFieldRouting).
				Description("The routing key to use for the document.").
				Advanced().
				Default(""),
			service.NewIntField(esoFieldRetryOnConflict).
				Description("When using the update or upsert action, retry_on_conflict can be used to specify how many times an update should be retried in the case of a version conflict.").
				Advanced().
				Default(0),
			service.NewBoolField(esoFieldSniff).
				Description("Prompts Redpanda Connect to sniff for brokers to connect to when establishing a connection.").
				Advanced().
				Default(true),
			service.NewBoolField(esoFieldHealthcheck).
				Description("Whether to enable healthchecks.").
				Advanced().
				Default(true),
			service.NewDurationField(esoFieldTimeout).
				Description("The maximum time to wait before abandoning a request (and trying again).").
				Advanced().
				Default("5s"),
			service.NewStringField(esoFieldAPIKey).
				Description("The key to set in the Authorization header if using API keys for authentication.").
				Optional().
				Secret(),
			service.NewTLSToggledField(esoFieldTLS),
			service.NewOutputMaxInFlightField(),
		).
		Fields(retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...).
		Fields(
			service.NewObjectField(esoFieldAuth,
				service.NewBoolField(esoFieldAuthEnabled).
					Description("Whether to use basic authentication in requests.").
					Default(false),
				service.NewStringField(esoFieldAuthUsername).
					Description("A username to authenticate as.").
					Default(""),
				service.NewStringField(esoFieldAuthPassword).
					Description("A password to authenticate with.").
					Default("").Secret(),
			).Description("Allows you to specify basic authentication.").
				Advanced().
				Optional(),
			service.NewBatchPolicyField(esoFieldBatching),
			AWSField(),
			service.NewBoolField(esoFieldGzipCompression).
				Description("Enable gzip compression on the request side.").
				Advanced().
				Default(false),
		).
		Example(
			"Elastic Cloud Serverless",
			"This is an example of writing data to https://www.elastic.co/docs/current/serverless[Elastic Cloud serverless^].",
			`
output:
  elasticsearch:
    urls: ["https://${ELASTIC_CLOUD_CLUSTER_ID}.es.us-east-1.aws.elastic.cloud:443"]
    sniff: false
    healthcheck: false
    index: "my-elasticsearch-index"
    id: my-document-id-${!counter()}-${!timestamp_unix()}
    api_key: "${ELASTIC_CLOUD_API_KEY}"
`)
}

func init() {
	err := service.RegisterBatchOutput("elasticsearch", OutputSpec(),
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
	if err != nil {
		panic(err)
	}
}

// Output implements service.BatchOutput for elasticsearch.
type Output struct {
	log  *service.Logger
	conf esoConfig

	client *elastic.Client
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
func (e *Output) Connect(ctx context.Context) error {
	if e.client != nil {
		return nil
	}

	client, err := elastic.NewClient(e.conf.clientOpts...)
	if err != nil {
		return err
	}

	e.client = client
	return nil
}

func shouldRetry(s int) bool {
	// Retry if the status code is 429 (Too Many Requests) or any 5xx server error.
	// HTTP 429 indicates the elasticsearch cluster is rate-limiting the client and expects the client to backoff
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-indexing-speed.html#multiple-workers-threads
	if s == 429 || (s >= 500 && s <= 599) {
		return true
	}
	return false
}

type pendingBulkIndex struct {
	Action          string
	Index           string
	Pipeline        string
	Routing         string
	RetryOnConflict int
	Type            string
	Doc             any
	ID              string
}

// WriteBatch writes a message batch to the output.
func (e *Output) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	if e.client == nil {
		return service.ErrNotConnected
	}

	boff := e.conf.backoffCtor()

	requests := make([]*pendingBulkIndex, len(msg))

	for i := 0; i < len(msg); i++ {
		jObj, ierr := msg[i].AsStructured()
		if ierr != nil {
			e.log.Errorf("Failed to marshal message into JSON document: %v\n", ierr)
			return fmt.Errorf("failed to marshal message into JSON document: %w", ierr)
		}

		pbi := &pendingBulkIndex{Doc: jObj}
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
		if pbi.Type, ierr = msg.TryInterpolatedString(i, e.conf.typeStr); ierr != nil {
			return fmt.Errorf("type interpolation error: %w", ierr)
		}
		if pbi.ID, ierr = msg.TryInterpolatedString(i, e.conf.idStr); ierr != nil {
			return fmt.Errorf("id interpolation error: %w", ierr)
		}
		requests[i] = pbi
	}

	b := e.client.Bulk()
	for _, v := range requests {
		bulkReq, err := e.buildBulkableRequest(v)
		if err != nil {
			return err
		}
		b.Add(bulkReq)
	}

	lastErrReason := "no reason given"
	for b.NumberOfActions() != 0 {
		result, err := b.Do(ctx)
		if err != nil {
			return err
		}
		if !result.Errors {
			return nil
		}

		var newRequests []*pendingBulkIndex
		for i, resp := range result.Items {
			for _, item := range resp {
				if item.Status >= 200 && item.Status <= 299 {
					continue
				}

				reason := "no reason given"
				if item.Error != nil {
					reason = item.Error.Reason
					lastErrReason = fmt.Sprintf("status [%v]: %v", item.Status, reason)
				}

				e.log.Errorf("Elasticsearch message '%v' rejected with status [%v]: %v\n", item.Id, item.Status, reason)
				if !shouldRetry(item.Status) {
					return fmt.Errorf("failed to send message '%v': %v", item.Id, reason)
				}

				// IMPORTANT: i exactly matches the index of our source requests
				// and when we re-run our bulk request with errored requests
				// that must remain true.
				sourceReq := requests[i]
				bulkReq, err := e.buildBulkableRequest(sourceReq)
				if err != nil {
					return err
				}
				b.Add(bulkReq)
				newRequests = append(newRequests, sourceReq)
			}
		}
		requests = newRequests

		wait := boff.NextBackOff()
		if wait == backoff.Stop {
			return fmt.Errorf("retries exhausted for messages, aborting with last error reported as: %v", lastErrReason)
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// Close closes the output.
func (e *Output) Close(context.Context) error {
	return nil
}

// Build a bulkable request for a given pending bulk index item.
func (e *Output) buildBulkableRequest(p *pendingBulkIndex) (elastic.BulkableRequest, error) {
	switch p.Action {
	case "update":
		r := elastic.NewBulkUpdateRequest().
			Index(p.Index).
			Routing(p.Routing).
			RetryOnConflict(p.RetryOnConflict).
			Id(p.ID).
			Doc(p.Doc)
		if p.Type != "" {
			r = r.Type(p.Type)
		}
		return r, nil
	case "upsert":
		r := elastic.NewBulkUpdateRequest().
			Index(p.Index).
			Routing(p.Routing).
			RetryOnConflict(p.RetryOnConflict).
			Id(p.ID).
			DocAsUpsert(true).
			Doc(p.Doc)
		if p.Type != "" {
			r = r.Type(p.Type)
		}
		return r, nil
	case "delete":
		r := elastic.NewBulkDeleteRequest().
			Index(p.Index).
			Routing(p.Routing).
			Id(p.ID)
		if p.Type != "" {
			r = r.Type(p.Type)
		}
		return r, nil
	case "index":
		r := elastic.NewBulkIndexRequest().
			Index(p.Index).
			Pipeline(p.Pipeline).
			Routing(p.Routing).
			Id(p.ID).
			Doc(p.Doc)
		if p.Type != "" {
			r = r.Type(p.Type)
		}
		return r, nil
	case "create":
		r := elastic.NewBulkCreateRequest().
			Index(p.Index).
			Pipeline(p.Pipeline).
			Routing(p.Routing).
			Id(p.ID).
			Doc(p.Doc)
		if p.Type != "" {
			r = r.Type(p.Type)
		}
		return r, nil
	default:
		return nil, fmt.Errorf("elasticsearch action '%s' is not allowed", p.Action)
	}
}
