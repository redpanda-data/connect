package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeElasticsearch] = TypeSpec{
		constructor: NewElasticsearch,
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
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Elasticsearch, conf.Elasticsearch.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"http://localhost:9200"}),
			docs.FieldCommon("index", "The index to place messages.").SupportsInterpolation(false),
			docs.FieldAdvanced("pipeline", "An optional pipeline id to preprocess incoming documents.").SupportsInterpolation(false),
			docs.FieldCommon("id", "The ID for indexed messages. Interpolation should be used in order to create a unique ID for each message.").SupportsInterpolation(false),
			docs.FieldCommon("type", "The document type."),
			docs.FieldAdvanced("sniff", "Prompts Benthos to sniff for brokers to connect to when establishing a connection."),
			docs.FieldAdvanced("healthcheck", "Whether to enable healthchecks."),
			docs.FieldAdvanced("timeout", "The maximum time to wait before abandoning a request (and trying again)."),
			tls.FieldSpec(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		}.Merge(retries.FieldSpecs()).Add(
			auth.BasicAuthFieldSpec(),
			batch.FieldSpec(),
			docs.FieldAdvanced("aws", "Enables and customises connectivity to Amazon Elastic Service.").WithChildren(
				docs.FieldSpecs{
					docs.FieldCommon("enabled", "Whether to connect to Amazon Elastic Service."),
				}.Merge(sess.FieldSpecs())...,
			),
		),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewElasticsearch creates a new Elasticsearch output type.
func NewElasticsearch(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	elasticWriter, err := writer.NewElasticsearch(conf.Elasticsearch, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.Elasticsearch.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeElasticsearch, elasticWriter, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeElasticsearch, conf.Elasticsearch.MaxInFlight, elasticWriter, log, stats,
		)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.Elasticsearch.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
