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
	Constructors[TypeKafka] = TypeSpec{
		constructor: NewKafka,
		Summary: `
Connects to a Kafka broker and consumes a topic and partition.`,
		Description: `
Offsets are managed within kafka as per the consumer group. Only one partition
per input is supported, if you wish to balance partitions across a consumer
group look at the ` + "`kafka_balanced`" + ` input type instead.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](/docs/configuration/batching#batch-policy). Any other batching
mechanism will stall with this input due its sequential transaction model.

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
			return sanitiseWithBatch(conf.Kafka, conf.Kafka.Batching)
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldDeprecated("max_batch_count"),
			docs.FieldCommon("addresses", "A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.", []string{"localhost:9092"}, []string{"localhost:9041,localhost:9042"}, []string{"localhost:9041", "localhost:9042"}),
			tls.FieldSpec(),
			sasl.FieldSpec(),
			docs.FieldCommon("topic", "A topic to consume from."),
			docs.FieldCommon("partition", "A partition to consume from."),
			docs.FieldCommon("consumer_group", "An identifier for the consumer group of the connection."),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldAdvanced("start_from_oldest", "If an offset is not found for a topic parition, determines whether to consume from the oldest available offset, otherwise messages are consumed from the latest offset."),
			docs.FieldAdvanced("commit_period", "The period of time between each commit of the current partition offsets. Offsets are always committed during shutdown."),
			docs.FieldAdvanced("max_processing_period", "A maximum estimate for the time taken to process a message, this is used for tuning consumer group synchronization."),
			docs.FieldAdvanced("fetch_buffer_cap", "The maximum number of unprocessed messages to fetch at a given time."),
			docs.FieldAdvanced("target_version", "The version of the Kafka protocol to use."),
			batch.FieldSpec(),
		},
	}
}

//------------------------------------------------------------------------------

// NewKafka creates a new Kafka input type.
func NewKafka(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	// TODO: V4 Remove this.
	if conf.Kafka.MaxBatchCount > 1 {
		log.Warnf("Field '%v.max_batch_count' is deprecated, use '%v.batching.count' instead.\n", conf.Type, conf.Type)
		conf.Kafka.Batching.Count = conf.Kafka.MaxBatchCount
	}
	k, err := reader.NewKafka(conf.Kafka, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	var kb reader.Type = k
	if !conf.Kafka.Batching.IsNoop() {
		if kb, err = reader.NewSyncBatcher(conf.Kafka.Batching, k, mgr, log, stats); err != nil {
			return nil, err
		}
	}
	return NewReader(TypeKafka, reader.NewPreserver(kb), log, stats)
}

//------------------------------------------------------------------------------
