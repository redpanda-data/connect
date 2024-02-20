package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	os "github.com/opensearch-project/opensearch-go/v2"
	"github.com/opensearch-project/opensearch-go/v2/opensearchutil"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	esoFieldURLs         = "urls"
	esoFieldID           = "id"
	esoFieldAction       = "action"
	esoFieldIndex        = "index"
	esoFieldPipeline     = "pipeline"
	esoFieldRouting      = "routing"
	esoFieldTLS          = "tls"
	esoFieldAuth         = "basic_auth"
	esoFieldAuthEnabled  = "enabled"
	esoFieldAuthUsername = "username"
	esoFieldAuthPassword = "password"
	esoFieldBatching     = "batching"
)

type esoConfig struct {
	clientOpts os.Config

	actionStr   *service.InterpolatedString
	idStr       *service.InterpolatedString
	indexStr    *service.InterpolatedString
	pipelineStr *service.InterpolatedString
	routingStr  *service.InterpolatedString
}

func esoConfigFromParsed(pConf *service.ParsedConfig) (conf esoConfig, err error) {
	conf.clientOpts = os.Config{}

	var tmpURLs []string
	if tmpURLs, err = pConf.FieldStringList(esoFieldURLs); err != nil {
		return
	}
	for _, u := range tmpURLs {
		for _, splitURL := range strings.Split(u, ",") {
			if len(splitURL) > 0 {
				conf.clientOpts.Addresses = append(conf.clientOpts.Addresses, splitURL)
			}
		}
	}

	{
		authConf := pConf.Namespace(esoFieldAuth)
		if enabled, _ := authConf.FieldBool(esoFieldAuthEnabled); enabled {
			if conf.clientOpts.Username, err = authConf.FieldString(esoFieldAuthUsername); err != nil {
				return
			}
			if conf.clientOpts.Password, err = authConf.FieldString(esoFieldAuthPassword); err != nil {
				return
			}
		}
	}

	var tlsConf *tls.Config
	var tlsEnabled bool
	if tlsConf, tlsEnabled, err = pConf.FieldTLSToggled(esoFieldTLS); err != nil {
		return
	} else if tlsEnabled {
		conf.clientOpts.Transport = &http.Transport{
			TLSClientConfig: tlsConf,
		}
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
	return
}

//------------------------------------------------------------------------------

// OutputSpec returns the config spec for an elasticsearch output writer.
func OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Publishes messages into an Elasticsearch index. If the index does not exist then it is created with a dynamic mapping.`).
		Description(output.Description(true, true, `
Both the `+"`id` and `index`"+` fields can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages these interpolations are performed per message part.`)).
		Fields(
			service.NewStringListField(esoFieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"http://localhost:9200"}),
			service.NewInterpolatedStringField(esoFieldIndex).
				Description("The index to place messages."),
			service.NewInterpolatedStringField(esoFieldAction).
				Description("The action to take on the document. This field must resolve to one of the following action types: `create`, `index`, `update`, `upsert` or `delete`."),
			service.NewInterpolatedStringField(esoFieldPipeline).
				Description("An optional pipeline id to preprocess incoming documents.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(esoFieldID).
				Description("The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").
				Default(`${!count("elastic_ids")}-${!timestamp_unix()}`),
			service.NewInterpolatedStringField(esoFieldRouting).
				Description("The routing key to use for the document.").
				Advanced().
				Default(""),
			service.NewTLSToggledField(esoFieldTLS),
			service.NewOutputMaxInFlightField(),
		).
		Fields(
			httpclient.BasicAuthField(),
			service.NewBatchPolicyField(esoFieldBatching),
		)

}

func init() {
	err := service.RegisterBatchOutput("opensearch", OutputSpec(),
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

	client *os.Client
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

func (e *Output) Connect(ctx context.Context) error {
	if e.client != nil {
		return nil
	}

	client, err := os.NewClient(e.conf.clientOpts)
	if err != nil {
		return err
	}

	e.client = client
	e.log.Infof("Sending messages to Elasticsearch index at urls: %s\n", e.conf.clientOpts.Addresses)
	return nil
}

type pendingBulkIndex struct {
	Action   string
	Index    string
	Pipeline string
	Routing  string
	Doc      any
	ID       string
}

func (e *Output) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	if e.client == nil {
		return component.ErrNotConnected
	}

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
		if pbi.ID, ierr = msg.TryInterpolatedString(i, e.conf.idStr); ierr != nil {
			return fmt.Errorf("id interpolation error: %w", ierr)
		}
		requests[i] = pbi
	}

	start := time.Now()
	b, _ := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client: e.client,
	})

	for _, v := range requests {
		bulkReq, err := e.buildBulkableRequest(v)
		if err != nil {
			return err
		}
		if err = b.Add(ctx, *bulkReq); err != nil {
			return err
		}
	}

	if err := b.Close(ctx); err != nil {
		return err
	}

	biStats := b.Stats()
	dur := time.Since(start)

	// TODO: Proper error handling mate???
	if biStats.NumFailed > 0 {
		e.log.Errorf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			biStats.NumFlushed,
			biStats.NumFailed,
			dur.Truncate(time.Millisecond),
			int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
		)
	} else {
		e.log.Debugf(
			"Successfully indexed [%s] documents in %s (%s docs/sec)",
			biStats.NumFlushed,
			dur.Truncate(time.Millisecond),
			int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
		)
	}
	return nil
}

func (e *Output) Close(context.Context) error {
	return nil
}

// Build a bulkable request for a given pending bulk index item.
func (e *Output) buildBulkableRequest(p *pendingBulkIndex) (r *opensearchutil.BulkIndexerItem, err error) {
	switch p.Action {
	case "update":
		var jsonData []byte
		jsonData, err = json.Marshal(p.Doc)
		if err != nil {
			return
		}
		r = &opensearchutil.BulkIndexerItem{
			Index:  p.Index,
			Action: "update",
			Body:   bytes.NewReader(jsonData),
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
