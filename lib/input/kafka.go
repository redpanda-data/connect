package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeKafka] = TypeSpec{
		constructor: NewKafka,
		Description: `
Connects to a kafka (0.8+) server. Offsets are managed within kafka as per the
consumer group (set via config). Only one partition per input is supported, if
you wish to balance partitions across a consumer group look at the
` + "`kafka_balanced`" + ` input type instead.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy). Any other batching mechanism
will stall with this input due its sequential transaction model.

This input currently provides a single continuous feed of data, and therefore
by default will only utilise a single processing thread and parallel output.
Take a look at the
[pipelines documentation](../pipeline.md#single-consumer-without-buffer) for
guides on how to work around this.

The field ` + "`max_processing_period`" + ` should be set above the maximum
estimated time taken to process a message.

The target version by default will be the oldest supported, as it is expected
that the server will be backwards compatible. In order to support newer client
features you should increase this version up to the known version of the target
server.

` + tls.Documentation + `

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
[function interpolation](../config_interpolation.md#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.Kafka, conf.Kafka.Batching)
		},
		FieldSpecs: docs.FieldSpecs{
			"max_batch_count": docs.FieldDeprecated(),
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
	k, err := reader.NewKafka(conf.Kafka, log, stats)
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
