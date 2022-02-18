package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/cenkalti/backoff/v4"
	"github.com/olivere/elastic/v7"
	aws "github.com/olivere/elastic/v7/aws/v4"
)

//------------------------------------------------------------------------------

// OptionalAWSConfig contains config fields for AWS authentication with an
// enable flag.
type OptionalAWSConfig struct {
	Enabled     bool `json:"enabled" yaml:"enabled"`
	sess.Config `json:",inline" yaml:",inline"`
}

//------------------------------------------------------------------------------

// ElasticsearchConfig contains configuration fields for the Elasticsearch
// output type.
type ElasticsearchConfig struct {
	URLs            []string             `json:"urls" yaml:"urls"`
	Sniff           bool                 `json:"sniff" yaml:"sniff"`
	Healthcheck     bool                 `json:"healthcheck" yaml:"healthcheck"`
	ID              string               `json:"id" yaml:"id"`
	Action          string               `json:"action" yaml:"action"`
	Index           string               `json:"index" yaml:"index"`
	Pipeline        string               `json:"pipeline" yaml:"pipeline"`
	Routing         string               `json:"routing" yaml:"routing"`
	Type            string               `json:"type" yaml:"type"`
	Timeout         string               `json:"timeout" yaml:"timeout"`
	TLS             btls.Config          `json:"tls" yaml:"tls"`
	Auth            auth.BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
	AWS             OptionalAWSConfig    `json:"aws" yaml:"aws"`
	GzipCompression bool                 `json:"gzip_compression" yaml:"gzip_compression"`
	MaxInFlight     int                  `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config  `json:",inline" yaml:",inline"`
	Batching        batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewElasticsearchConfig creates a new ElasticsearchConfig with default values.
func NewElasticsearchConfig() ElasticsearchConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return ElasticsearchConfig{
		URLs:        []string{"http://localhost:9200"},
		Sniff:       true,
		Healthcheck: true,
		Action:      "index",
		ID:          `${!count("elastic_ids")}-${!timestamp_unix()}`,
		Index:       "benthos_index",
		Pipeline:    "",
		Type:        "doc",
		Routing:     "",
		Timeout:     "5s",
		TLS:         btls.NewConfig(),
		Auth:        auth.NewBasicAuthConfig(),
		AWS: OptionalAWSConfig{
			Enabled: false,
			Config:  sess.NewConfig(),
		},
		GzipCompression: false,
		MaxInFlight:     1,
		Config:          rConf,
		Batching:        batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// Elasticsearch is a writer type that writes messages into elasticsearch.
type Elasticsearch struct {
	log   log.Modular
	stats metrics.Type

	urls        []string
	sniff       bool
	healthcheck bool
	conf        ElasticsearchConfig

	backoffCtor func() backoff.BackOff
	timeout     time.Duration
	tlsConf     *tls.Config

	actionStr   *field.Expression
	idStr       *field.Expression
	indexStr    *field.Expression
	pipelineStr *field.Expression
	routingStr  *field.Expression

	client *elastic.Client
}

// NewElasticsearchV2 creates a new Elasticsearch writer type.
func NewElasticsearchV2(conf ElasticsearchConfig, mgr interop.Manager, log log.Modular, stats metrics.Type) (*Elasticsearch, error) {
	e := Elasticsearch{
		log:         log,
		stats:       stats,
		conf:        conf,
		sniff:       conf.Sniff,
		healthcheck: conf.Healthcheck,
	}

	var err error
	if e.actionStr, err = mgr.BloblEnvironment().NewField(conf.Action); err != nil {
		return nil, fmt.Errorf("failed to parse action expression: %v", err)
	}
	if e.idStr, err = mgr.BloblEnvironment().NewField(conf.ID); err != nil {
		return nil, fmt.Errorf("failed to parse id expression: %v", err)
	}
	if e.indexStr, err = mgr.BloblEnvironment().NewField(conf.Index); err != nil {
		return nil, fmt.Errorf("failed to parse index expression: %v", err)
	}
	if e.pipelineStr, err = mgr.BloblEnvironment().NewField(conf.Pipeline); err != nil {
		return nil, fmt.Errorf("failed to parse pipeline expression: %v", err)
	}
	if e.routingStr, err = mgr.BloblEnvironment().NewField(conf.Routing); err != nil {
		return nil, fmt.Errorf("failed to parse routing key expression: %v", err)
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if len(splitURL) > 0 {
				e.urls = append(e.urls, splitURL)
			}
		}
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if e.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if e.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}

	if conf.TLS.Enabled {
		var err error
		if e.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return &e, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext attempts to establish a connection to a Elasticsearch
// broker.
func (e *Elasticsearch) ConnectWithContext(ctx context.Context) error {
	return e.Connect()
}

// Connect attempts to establish a connection to a Elasticsearch broker.
func (e *Elasticsearch) Connect() error {
	if e.client != nil {
		return nil
	}

	opts := []elastic.ClientOptionFunc{
		elastic.SetURL(e.urls...),
		elastic.SetSniff(e.sniff),
		elastic.SetHealthcheck(e.healthcheck),
	}

	if e.conf.Auth.Enabled {
		opts = append(opts, elastic.SetBasicAuth(
			e.conf.Auth.Username, e.conf.Auth.Password,
		))
	}

	if e.conf.TLS.Enabled {
		opts = append(opts, elastic.SetHttpClient(&http.Client{
			Transport: &http.Transport{
				TLSClientConfig: e.tlsConf,
			},
			Timeout: e.timeout,
		}))

	} else {
		opts = append(opts, elastic.SetHttpClient(&http.Client{
			Timeout: e.timeout,
		}))
	}

	if e.conf.AWS.Enabled {
		tsess, err := e.conf.AWS.GetSession()
		if err != nil {
			return err
		}
		signingClient := aws.NewV4SigningClient(tsess.Config.Credentials, e.conf.AWS.Region)
		opts = append(opts, elastic.SetHttpClient(signingClient))
	}

	if e.conf.GzipCompression {
		opts = append(opts, elastic.SetGzip(true))
	}

	client, err := elastic.NewClient(opts...)
	if err != nil {
		return err
	}

	e.client = client
	e.log.Infof("Sending messages to Elasticsearch index at urls: %s\n", e.urls)
	return nil
}

func shouldRetry(s int) bool {
	if s >= 500 && s <= 599 {
		return true
	}
	return false
}

type pendingBulkIndex struct {
	Action   string
	Index    string
	Pipeline string
	Routing  string
	Type     string
	Doc      interface{}
	ID       string
}

// WriteWithContext will attempt to write a message to Elasticsearch, wait for
// acknowledgement, and returns an error if applicable.
func (e *Elasticsearch) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	return e.Write(msg)
}

// Write will attempt to write a message to Elasticsearch, wait for
// acknowledgement, and returns an error if applicable.
func (e *Elasticsearch) Write(msg *message.Batch) error {
	if e.client == nil {
		return component.ErrNotConnected
	}

	boff := e.backoffCtor()

	requests := make([]*pendingBulkIndex, msg.Len())
	if err := msg.Iter(func(i int, part *message.Part) error {
		jObj, ierr := part.JSON()
		if ierr != nil {
			e.log.Errorf("Failed to marshal message into JSON document: %v\n", ierr)
			return fmt.Errorf("failed to marshal message into JSON document: %w", ierr)
		}
		requests[i] = &pendingBulkIndex{
			Action:   e.actionStr.String(i, msg),
			Index:    e.indexStr.String(i, msg),
			Pipeline: e.pipelineStr.String(i, msg),
			Routing:  e.routingStr.String(i, msg),
			Type:     e.conf.Type,
			Doc:      jObj,
			ID:       e.idStr.String(i, msg),
		}
		return nil
	}); err != nil {
		return err
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
		result, err := b.Do(context.Background())
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
		time.Sleep(wait)
	}

	return nil
}

// CloseAsync shuts down the Elasticsearch writer and stops processing messages.
func (e *Elasticsearch) CloseAsync() {
}

// WaitForClose blocks until the Elasticsearch writer has closed down.
func (e *Elasticsearch) WaitForClose(timeout time.Duration) error {
	return nil
}

// Build a bulkable request for a given pending bulk index item.
func (e *Elasticsearch) buildBulkableRequest(p *pendingBulkIndex) (elastic.BulkableRequest, error) {
	// TODO: V4 the type field should be optional and not used
	switch p.Action {
	case "update":
		return elastic.NewBulkUpdateRequest().
			Index(p.Index).
			Routing(p.Routing).
			Type(p.Type).
			Id(p.ID).
			Doc(p.Doc), nil
	case "delete":
		return elastic.NewBulkDeleteRequest().
			Index(p.Index).
			Routing(p.Routing).
			Id(p.ID).
			Type(p.Type), nil
	case "index":
		return elastic.NewBulkIndexRequest().
			Index(p.Index).
			Pipeline(p.Pipeline).
			Routing(p.Routing).
			Type(p.Type).
			Id(p.ID).
			Doc(p.Doc), nil
	default:
		return nil, fmt.Errorf("elasticsearch action '%s' is not allowed", p.Action)
	}
}

//------------------------------------------------------------------------------
