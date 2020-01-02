package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeElasticsearch] = TypeSpec{
		constructor: NewElasticsearch,
		Description: `
Publishes messages into an Elasticsearch index. If the index does not exist then
it is created with a dynamic mapping.

Both the ` + "`id` and `index`" + ` fields can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions). When
sending batched messages these interpolations are performed per message part.

### AWS Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

If the configured target is a managed AWS Elasticsearch cluster, you may need
to set ` + "`sniff` and `healthcheck`" + ` to false for connections to succeed.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Elasticsearch, conf.Elasticsearch.Batching)
		},
		Async:   true,
		Batches: true,
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
	if bconf := conf.Elasticsearch.Batching; err == nil && !bconf.IsNoop() {
		policy, err := batch.NewPolicy(bconf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		w = NewBatcher(policy, w, log, stats)
	}
	return w, err
}

//------------------------------------------------------------------------------
