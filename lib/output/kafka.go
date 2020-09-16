package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafka] = TypeSpec{
		constructor: NewKafka,
		Summary: `
The kafka output type writes a batch of messages to Kafka brokers and waits for
acknowledgement before propagating it back to the input.`,
		Description: `
The config field ` + "`ack_replicas`" + ` determines whether we wait for
acknowledgement from all replicas or just a single broker.

Both the ` + "`key` and `topic`" + ` fields can be dynamically set using
function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).
When sending batched messages these interpolations are performed per message
part.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Kafka, conf.Kafka.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldDeprecated("round_robin_partitions"),
			docs.FieldCommon("addresses", "A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.", []string{"localhost:9092"}, []string{"localhost:9041,localhost:9042"}, []string{"localhost:9041", "localhost:9042"}),
			tls.FieldSpec(),
			sasl.FieldSpec(),
			docs.FieldCommon("topic", "The topic to publish messages to.").SupportsInterpolation(false),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldCommon("key", "The key to publish messages with.").SupportsInterpolation(false),
			docs.FieldCommon("partitioner", "The partitioning algorithm to use.").HasOptions("fnv1a_hash", "murmur2_hash", "random", "round_robin"),
			docs.FieldCommon("compression", "The compression algorithm to use.").HasOptions("none", "snappy", "lz4", "gzip"),
			docs.FieldCommon("static_headers", "An optional map of static headers that should be added to messages in addition to metadata.", map[string]string{"first-static-header": "value-1", "second-static-header": "value-2"}),
			docs.FieldCommon("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			docs.FieldAdvanced("ack_replicas", "Ensure that messages have been copied across all replicas before acknowledging receipt."),
			docs.FieldAdvanced("max_msg_bytes", "The maximum size in bytes of messages sent to the target topic."),
			docs.FieldAdvanced("timeout", "The maximum period of time to wait for message sends before abandoning the request and retrying."),
			docs.FieldAdvanced("target_version", "The version of the Kafka protocol to use."),
			batch.FieldSpec(),
		}, retries.FieldSpecs()...),
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewKafka creates a new Kafka output type.
func NewKafka(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	k, err := writer.NewKafka(conf.Kafka, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.Kafka.MaxInFlight == 1 {
		w, err = NewWriter(
			TypeKafka, k, log, stats,
		)
	} else {
		w, err = NewAsyncWriter(
			TypeKafka, conf.Kafka.MaxInFlight, k, log, stats,
		)
	}
	if err != nil {
		return nil, err
	}
	return newBatcherFromConf(conf.Kafka.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
