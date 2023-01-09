package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	os "github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchutil"

	"github.com/benthosdev/benthos/v4/internal/httpclient"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"

	"github.com/tidwall/sjson"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	itls "github.com/benthosdev/benthos/v4/internal/tls"
)

// Transport is a RoundTripper that will sign requests with AWS V4 Signing
type Transport struct {
	Transport http.RoundTripper
	Creds     *credentials.Credentials
	Signer    *v4.Signer
	Region    string
}

func notImportedAWSOptFn(roundtripper http.RoundTripper, conf output.OpenSearchConfig) (*Transport, error) {
	if !conf.AWS.Enabled {
		return nil, nil
	}
	return nil, errors.New("unable to configure AWS authentication as this binary does not import components/aws")
}

// AWSOptFn is populated with the child `aws` package when imported.
var AWSOptFn = notImportedAWSOptFn

func init() {
	//TODO: use public service API, refer:output_pusher.go
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return NewOpenSearch(c.OpenSearch, nm)
	}), docs.ComponentSpec{
		Name: "opensearch",
		Summary: `
Publishes messages into an OpenSearch index. If the index does not exist then
it is created with a dynamic mapping.`,
		Description: output.Description(true, true, `
Both the `+"`id` and `index`"+` fields can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When
sending batched messages these interpolations are performed per message part.

### AWS

It's possible to enable AWS connectivity with this output using the `+"`aws`"+`
fields. However, you may need to set `+"`sniff` and `healthcheck`"+` to
false for connections to succeed.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"http://localhost:9200"}).Array(),
			docs.FieldString("index", "The index to place messages.").IsInterpolated(),
			docs.FieldString("action", "The action to take on the document. This field must resolve to one of the following action types: `create`, `index`, `update`, `upsert` or `delete`.").IsInterpolated().Advanced(),
			docs.FieldString("pipeline", "An optional pipeline id to preprocess incoming documents.").IsInterpolated().Advanced(),
			docs.FieldString("id", "The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").IsInterpolated(),
			docs.FieldString("type", "The document mapping type. This field is required for versions of elasticsearch earlier than 6.0.0, but are invalid for versions 7.0.0 or later.").Optional().IsInterpolated(),
			docs.FieldString("routing", "The routing key to use for the document.").IsInterpolated().Advanced(),
			docs.FieldString("timeout", "The maximum time to wait before abandoning a request (and trying again).").Advanced(),
			docs.FieldString("flush_interval", "The maximum time to wait before flushing a request (and trying again).").Advanced(),
			docs.FieldString("flush_bytes", "The maximum size in buffer before flushing a request.").Advanced(),
			itls.FieldSpec(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).WithChildren(retries.FieldSpecs()...).WithChildren(
			httpclient.OldBasicAuthFieldSpec(),
			policy.FieldSpec(),
			docs.FieldObject("aws", "Enables and customises connectivity to Amazon OpenSearch Service.").WithChildren(
				docs.FieldSpecs{
					docs.FieldBool("enabled", "Whether to connect to Amazon OpenSearch Service."),
				}.Merge(sess.FieldSpecs())...,
			).Advanced(),
			docs.FieldBool("gzip_compression", "Enable gzip compression on the request side.").Advanced(),
		).ChildDefaultAndTypesFromStruct(output.NewOpenSearchConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

// NewOpenSearch creates a new OpenSearch output type.
func NewOpenSearch(conf output.OpenSearchConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	openSearchWriter, err := NewOpenSearchV2(conf, mgr)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("opensearch", conf.MaxInFlight, openSearchWriter, mgr)
	if err != nil {
		return w, err
	}
	return batcher.NewFromConfig(conf.Batching, w, mgr)
}

// OpenSearch is a writer type that writes messages into OpenSearch.
type OpenSearch struct {
	log   log.Modular
	stats metrics.Type

	urls []string
	conf output.OpenSearchConfig

	backoffCtor   func() backoff.BackOff
	timeout       time.Duration
	flushInterval time.Duration
	flushBytes    int
	tlsConf       *tls.Config

	actionStr   *field.Expression
	idStr       *field.Expression
	indexStr    *field.Expression
	pipelineStr *field.Expression
	routingStr  *field.Expression
	typeStr     *field.Expression

	client *os.Client
}

// NewOpenSearchV2 creates a new OpenSearch writer type.
func NewOpenSearchV2(conf output.OpenSearchConfig, mgr bundle.NewManagement) (*OpenSearch, error) {
	e := OpenSearch{
		log:   mgr.Logger(),
		stats: mgr.Metrics(),
		conf:  conf,
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
	if e.typeStr, err = mgr.BloblEnvironment().NewField(conf.Type); err != nil {
		return nil, fmt.Errorf("failed to parse type field expression: %v", err)
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
		if e.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	return &e, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to a OpenSearch broker.
func (e *OpenSearch) Connect(ctx context.Context) error {
	if e.client != nil {
		return nil
	}
	boff := e.backoffCtor()
	opts := os.Config{
		Addresses:     e.urls,
		RetryOnStatus: []int{502, 503, 504, 429},
		RetryBackoff: func(attempt int) time.Duration {
			if attempt == 1 {
				boff.Reset()
			}
			return boff.NextBackOff()
		},
		MaxRetries: int(e.conf.MaxRetries),
	}
	if e.conf.Auth.Enabled {
		opts.Username = e.conf.Auth.Username
		opts.Password = e.conf.Auth.Password
	}

	opts.Transport = &http.Transport{
		ResponseHeaderTimeout: e.timeout,
	}
	if e.conf.TLS.Enabled {
		opts.Transport = &http.Transport{
			TLSClientConfig:       e.tlsConf,
			ResponseHeaderTimeout: e.timeout,
		}

	}

	awsOpts, err := AWSOptFn(opts.Transport, e.conf)
	if err != nil {
		return err
	}
	if awsOpts != nil {
		opts.Transport = awsOpts

	}

	if e.conf.GzipCompression {
		opts.CompressRequestBody = true
	}

	client, err := os.NewClient(opts)
	if err != nil {
		return err
	}

	e.client = client
	e.log.Infof("Sending messages to OpenSearch index at urls: %s\n", e.urls)
	return nil
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

// WriteBatch will attempt to write a message to OpenSearch, wait for
// acknowledgement, and returns an error if applicable.
func (e *OpenSearch) WriteBatch(ctx context.Context, msg message.Batch) error {
	return e.Write(ctx, msg)
}

// Write will attempt to write a message to OpenSearch, wait for
// acknowledgement, and returns an error if applicable.
func (e *OpenSearch) Write(ctx context.Context, msg message.Batch) error {
	if e.client == nil {
		return component.ErrNotConnected
	}

	requests := make([]*pendingBulkIndex, msg.Len())
	if err := msg.Iter(func(i int, part *message.Part) error {
		jObj, ierr := part.AsStructured()
		if ierr != nil {
			e.log.Errorf("Failed to marshal message into JSON document: %v\n", ierr)
			return fmt.Errorf("failed to marshal message into JSON document: %w", ierr)
		}
		pbi := &pendingBulkIndex{
			Doc: jObj,
		}
		if pbi.Action, ierr = e.actionStr.String(i, msg); ierr != nil {
			return fmt.Errorf("action interpolation error: %w", ierr)
		}
		if pbi.Index, ierr = e.indexStr.String(i, msg); ierr != nil {
			return fmt.Errorf("index interpolation error: %w", ierr)
		}
		if pbi.Pipeline, ierr = e.pipelineStr.String(i, msg); ierr != nil {
			return fmt.Errorf("pipeline interpolation error: %w", ierr)
		}
		if pbi.Routing, ierr = e.routingStr.String(i, msg); ierr != nil {
			return fmt.Errorf("routing interpolation error: %w", ierr)
		}
		if pbi.Type, ierr = e.typeStr.String(i, msg); ierr != nil {
			return fmt.Errorf("type interpolation error: %w", ierr)
		}
		if pbi.ID, ierr = e.idStr.String(i, msg); ierr != nil {
			return fmt.Errorf("id interpolation error: %w", ierr)
		}

		requests[i] = pbi
		return nil
	}); err != nil {
		return err
	}
	start := time.Now()
	b, _ := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		FlushInterval: e.flushInterval,
		FlushBytes:    e.flushBytes,
		Client:        e.client,
	})

	for _, v := range requests {
		bulkReq, err := e.buildBulkableRequest(v)
		if err != nil {
			return err
		}
		_ = b.Add(ctx, *bulkReq)
	}

	_ = b.Close(ctx)

	biStats := b.Stats()
	dur := time.Since(start)

	if biStats.NumFailed > 0 {
		e.log.Errorf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			humanize.Comma(int64(biStats.NumFailed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	} else {
		e.log.Debugf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	}

	return nil
}

// Close shuts down the OpenSearch writer and stops processing messages.
func (e *OpenSearch) Close(context.Context) error {
	return nil
}

// Build a bulkable request for a given pending bulk index item.
func (e *OpenSearch) buildBulkableRequest(p *pendingBulkIndex) (r *opensearchutil.BulkIndexerItem, err error) {
	switch p.Action {
	case "update":
		// opensearch util needs the body root element should be "doc"
		var doc string
		doc, err = sjson.SetOptions("", "doc", p.Doc, &sjson.Options{ReplaceInPlace: true})
		if err != nil {
			return
		}
		r = &opensearchutil.BulkIndexerItem{
			Index:  p.Index,
			Action: "update",
			Body:   strings.NewReader(doc),
		}
		if p.ID != "" {
			r.DocumentID = p.ID
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}

		return
	case "delete":
		r = &opensearchutil.BulkIndexerItem{
			Index:      p.Index,
			DocumentID: p.ID,
			Action:     "delete",
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}
		return
	case "index":
		var jsonData []byte
		jsonData, err = json.Marshal(p.Doc)
		if err != nil {
			return
		}
		r = &opensearchutil.BulkIndexerItem{
			Index:  p.Index,
			Action: "index",
			Body:   bytes.NewReader(jsonData),
		}
		if p.ID != "" {
			r.DocumentID = p.ID
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}
		return
	case "create":
		var jsonData []byte
		jsonData, err = json.Marshal(p.Doc)
		if err != nil {
			return
		}
		r = &opensearchutil.BulkIndexerItem{
			Index:  p.Index,
			Action: "create",
			Body:   bytes.NewReader(jsonData),
		}
		if p.ID != "" {
			r.DocumentID = p.ID
		}
		if p.Routing != "" {
			r.Routing = &p.Routing
		}
		return
	default:
		return nil, fmt.Errorf("opensearch action '%s' is not allowed", p.Action)
	}
}

// RoundTrip uses the underlying RoundTripper transport, but signs request first with AWS V4 Signing
func (st Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if h, ok := req.Header["Authorization"]; ok && len(h) > 0 && strings.HasPrefix(h[0], "AWS4") {
		// Received a signed request, just pass it on.
		return st.Transport.RoundTrip(req)
	}

	if strings.Contains(req.URL.RawPath, "%2C") {
		// Escaping path
		req.URL.RawPath = url.PathEscape(req.URL.RawPath)
	}

	now := time.Now().UTC()
	req.Header.Set("Date", now.Format(time.RFC3339))

	var err error
	switch req.Body {
	case nil:
		_, err = st.Signer.Sign(req, nil, "es", st.Region, now)
	default:
		switch body := req.Body.(type) {
		case io.ReadSeeker:
			_, err = st.Signer.Sign(req, body, "es", st.Region, now)
		default:
			var buf []byte
			buf, err = io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			req.Body = io.NopCloser(bytes.NewReader(buf))
			_, err = st.Signer.Sign(req, bytes.NewReader(buf), "es", st.Region, time.Now().UTC())
		}
	}
	if err != nil {
		return nil, err
	}
	return st.Transport.RoundTrip(req)
}
