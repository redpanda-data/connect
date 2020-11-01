package input

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafkaBalanced] = TypeSpec{
		constructor:                  NewKafkaBalanced,
		constructorHasBatchProcessor: newKafkaBalancedHasBatchProcessor,
		Status:                       docs.StatusDeprecated,
		Summary: `
Connects to Kafka brokers and consumes topics by automatically sharing
partitions across other consumers of the same consumer group.`,
		Description: `
Offsets are managed within Kafka as per the consumer group (set via config), and
partitions are automatically balanced across any members of the consumer group.

Partitions consumed by this input can be processed in parallel allowing it to
utilise <= N pipeline processing threads and parallel outputs where N is the
number of partitions allocated to this consumer.

The ` + "`batching`" + ` fields allow you to configure a
[batching policy](/docs/configuration/batching#batch-policy) which will be
applied per partition. Any other batching mechanism will stall with this input
due its sequential transaction model.

## Alternatives

The functionality of this input is now covered by the general ` + "[`kafka` input](/docs/components/inputs/kafka)" + `.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- All existing message headers (version 0.11+)
` + "```" + `

The field ` + "`kafka_lag`" + ` is the calculated difference between the high
water mark offset of the partition at the time of ingestion and the current
message offset.

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.KafkaBalanced, conf.KafkaBalanced.Batching)
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldDeprecated("max_batch_count"),
			docs.FieldCommon("addresses", "A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.", []string{"localhost:9092"}, []string{"localhost:9041,localhost:9042"}, []string{"localhost:9041", "localhost:9042"}),
			tls.FieldSpec(),
			sasl.FieldSpec(),
			docs.FieldCommon("topics", "A list of topics to consume from. If an item of the list contains commas it will be expanded into multiple topics."),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldCommon("consumer_group", "An identifier for the consumer group of the connection."),
			docs.FieldAdvanced("start_from_oldest", "If an offset is not found for a topic parition, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset."),
			docs.FieldAdvanced("commit_period", "The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown."),
			docs.FieldAdvanced("max_processing_period", "A maximum estimate for the time taken to process a message, this is used for tuning consumer group synchronization."),
			docs.FieldAdvanced("group", "Tuning parameters for consumer group synchronization.").WithChildren(
				docs.FieldAdvanced("session_timeout", "A period after which a consumer of the group is kicked after no heartbeats."),
				docs.FieldAdvanced("heartbeat_interval", "A period in which heartbeats should be sent out."),
				docs.FieldAdvanced("rebalance_timeout", "A period after which rebalancing is abandoned if unresolved."),
			),
			docs.FieldAdvanced("fetch_buffer_cap", "The maximum number of unprocessed messages to fetch at a given time."),
			docs.FieldAdvanced("target_version", "The version of the Kafka protocol to use."),
			batch.FieldSpec(),
		},
		Categories: []Category{
			CategoryServices,
		},
	}
}

//------------------------------------------------------------------------------

// NewKafkaBalanced creates a new KafkaBalanced input type.
func NewKafkaBalanced(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	// TODO: V4 Remove this.
	if conf.KafkaBalanced.MaxBatchCount > 1 {
		log.Warnf("Field '%v.max_batch_count' is deprecated, use '%v.batching.count' instead.\n", conf.Type, conf.Type)
		conf.KafkaBalanced.Batching.Count = conf.KafkaBalanced.MaxBatchCount
	}
	k, err := reader.NewKafkaCG(conf.KafkaBalanced, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	preserved := reader.NewAsyncPreserver(k)
	return NewAsyncReader("kafka_balanced", true, preserved, log, stats)
}

// DEPRECATED: This is a hack for until the batch processor is removed.
// TODO: V4 Remove this.
func newKafkaBalancedHasBatchProcessor(
	hasBatchProc bool,
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if !hasBatchProc {
		return NewKafkaBalanced(conf, mgr, log, stats)
	}

	log.Warnln("Detected presence of a 'batch' processor, kafka_balanced input falling back to single threaded mode. To fix this use the 'batching' fields instead.")
	k, err := reader.NewKafkaBalanced(conf.KafkaBalanced, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("kafka_balanced", reader.NewPreserver(k), log, stats)
}

//------------------------------------------------------------------------------
