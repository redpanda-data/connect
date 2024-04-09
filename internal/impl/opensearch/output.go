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

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
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
	esoFieldAWS          = "aws"
	ESOFieldAWSEnabled   = "enabled"
)

func notImportedAWSOptFn(conf *service.ParsedConfig, osconf *opensearchapi.Config) error {
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
}

func esoConfigFromParsed(pConf *service.ParsedConfig) (conf esoConfig, err error) {
	conf.clientOpts = opensearchapi.Config{}

	var tmpURLs []string
	if tmpURLs, err = pConf.FieldStringList(esoFieldURLs); err != nil {
		return
	}
	for _, u := range tmpURLs {
		for _, splitURL := range strings.Split(u, ",") {
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
		Description(output.Description(true, true, `
Both the `+"`id` and `index`"+` fields can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages these interpolations are performed per message part.`)).
		Fields(
			service.NewStringListField(esoFieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"http://localhost:9200"}),
			service.NewInterpolatedStringField(esoFieldIndex).
				Description("The index to place messages."),
			service.NewInterpolatedStringField(esoFieldAction).
				Description("The action to take on the document. This field must resolve to one of the following action types: `index`, `update` or `delete`."),
			service.NewInterpolatedStringField(esoFieldID).
				Description("The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").
				Example(`${!counter()}-${!timestamp_unix()}`),
			service.NewInterpolatedStringField(esoFieldPipeline).
				Description("An optional pipeline id to preprocess incoming documents.").
				Advanced().
				Default(""),
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
			AWSField(),
		).
		Example("Updating Documents", "When [updating documents](https://opensearch.org/docs/latest/api-reference/document-apis/update-document/) the request body should contain a combination of a `doc`, `upsert`, and/or `script` fields at the top level, this should be done via mapping processors.", `
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

func (e *Output) Connect(ctx context.Context) error {
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

func (e *Output) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	if e.client == nil {
		return component.ErrNotConnected
	}

	requests := make([]*pendingBulkIndex, len(msg))

	for i := 0; i < len(msg); i++ {
		rawBytes, ierr := msg[i].AsBytes()
		if ierr != nil {
			e.log.Errorf("Failed to obtain message raw data: %v\n", ierr)
			return fmt.Errorf("failed to obtain message raw data: %w", ierr)
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

	start := time.Now()
	b, _ := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client: e.client,
	})

	var bErrMut sync.Mutex
	var bErr *service.BatchError

	for i, v := range requests {
		i := i
		bulkReq, err := e.buildBulkableRequest(v, func(err error) {
			bErrMut.Lock()
			defer bErrMut.Unlock()

			if bErr == nil {
				bErr = service.NewBatchError(msg, err)
			}
			bErr = bErr.Failed(i, err)
		})
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

	if bErr != nil {
		return bErr
	}

	biStats := b.Stats()
	dur := time.Since(start)

	e.log.Debugf(
		"Successfully dispatched [%s] documents in %s (%s docs/sec)",
		biStats.NumFlushed,
		dur.Truncate(time.Millisecond),
		int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed)),
	)
	return nil
}

func (e *Output) Close(context.Context) error {
	return nil
}

// Build a bulkable request for a given pending bulk index item.
func (e *Output) buildBulkableRequest(p *pendingBulkIndex, onError func(err error)) (r *opensearchutil.BulkIndexerItem, err error) {
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
		ctx context.Context,
		bii opensearchutil.BulkIndexerItem,
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
