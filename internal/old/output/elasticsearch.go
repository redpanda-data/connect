package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/http/docs/auth"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeElasticsearch] = TypeSpec{
		constructor: fromSimpleConstructor(NewElasticsearch),
		Summary: `
Publishes messages into an Elasticsearch index. If the index does not exist then
it is created with a dynamic mapping.`,
		Description: `
Both the ` + "`id` and `index`" + ` fields can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When
sending batched messages these interpolations are performed per message part.

### AWS

It's possible to enable AWS connectivity with this output using the ` + "`aws`" + `
fields. However, you may need to set ` + "`sniff` and `healthcheck`" + ` to
false for connections to succeed.`,
		Async:   true,
		Batches: true,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"http://localhost:9200"}).Array(),
			docs.FieldString("index", "The index to place messages.").IsInterpolated(),
			docs.FieldString("action", "The action to take on the document.").IsInterpolated().HasOptions("index", "update", "delete").Advanced(),
			docs.FieldString("pipeline", "An optional pipeline id to preprocess incoming documents.").IsInterpolated().Advanced(),
			docs.FieldString("id", "The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").IsInterpolated(),
			docs.FieldString("type", "The document type.").Deprecated(),
			docs.FieldString("routing", "The routing key to use for the document.").IsInterpolated().Advanced(),
			docs.FieldBool("sniff", "Prompts Benthos to sniff for brokers to connect to when establishing a connection.").Advanced(),
			docs.FieldBool("healthcheck", "Whether to enable healthchecks.").Advanced(),
			docs.FieldString("timeout", "The maximum time to wait before abandoning a request (and trying again).").Advanced(),
			tls.FieldSpec(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).WithChildren(retries.FieldSpecs()...).WithChildren(
			auth.BasicAuthFieldSpec(),
			policy.FieldSpec(),
			docs.FieldObject("aws", "Enables and customises connectivity to Amazon Elastic Service.").WithChildren(
				docs.FieldSpecs{
					docs.FieldBool("enabled", "Whether to connect to Amazon Elastic Service."),
				}.Merge(sess.FieldSpecs())...,
			).Advanced(),
			docs.FieldBool("gzip_compression", "Enable gzip compression on the request side.").Advanced(),
		),
		Categories: []string{
			"Services",
		},
	}
}

//------------------------------------------------------------------------------

// NewElasticsearch creates a new Elasticsearch output type.
func NewElasticsearch(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	elasticWriter, err := writer.NewElasticsearchV2(conf.Elasticsearch, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(
		TypeElasticsearch, conf.Elasticsearch.MaxInFlight, elasticWriter, log, stats,
	)
	if err != nil {
		return w, err
	}
	return NewBatcherFromConfig(conf.Elasticsearch.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
