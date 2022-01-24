package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
)

func init() {
	Constructors[TypeKafka] = TypeSpec{
		constructor: fromSimpleConstructor(NewKafka),
		Summary: `
The kafka output type writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.`,
		Description: `
The config field ` + "`ack_replicas`" + ` determines whether we wait for acknowledgement from all replicas or just a single broker.

Both the ` + "`key` and `topic`" + ` fields can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

[Metadata](/docs/configuration/metadata) will be added to each message sent as headers (version 0.11+), but can be restricted using the field ` + "[`metadata`](#metadata)" + `.

### Strict Ordering and Retries

When strict ordering is required for messages written to topic partitions it is important to ensure that both the field ` + "`max_in_flight` is set to `1` and that the field `retry_as_batch` is set to `true`" + `.

You must also ensure that failed batches are never rerouted back to the same output. This can be done by setting the field ` + "`max_retries` to `0` and `backoff.max_elapsed_time`" + ` to empty, which will apply back pressure indefinitely until the batch is sent successfully.

However, this also means that manual intervention will eventually be required in cases where the batch cannot be sent due to configuration problems such as an incorrect ` + "`max_msg_bytes`" + ` estimate. A less strict but automated alternative would be to route failed batches to a dead letter queue using a ` + "[`fallback` broker](/docs/components/outputs/fallback)" + `, but this would allow subsequent batches to be delivered in the meantime whilst those failed batches are dealt with.

### Troubleshooting

If you're seeing issues writing to or reading from Kafka with this component then it's worth trying out the newer ` + "[`kafka_franz` output](/docs/components/outputs/kafka_franz)" + `.

- I'm seeing logs that report ` + "`Failed to connect to kafka: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)`" + `, but the brokers are definitely reachable.

Unfortunately this error message will appear for a wide range of connection problems even when the broker endpoint can be reached. Double check your authentication configuration and also ensure that you have [enabled TLS](#tlsenabled) if applicable.`,
		Async:   true,
		Batches: true,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldCommon("addresses", "A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.", []string{"localhost:9092"}, []string{"localhost:9041,localhost:9042"}, []string{"localhost:9041", "localhost:9042"}).Array(),
			tls.FieldSpec(),
			sasl.FieldSpec(),
			docs.FieldCommon("topic", "The topic to publish messages to.").IsInterpolated(),
			docs.FieldCommon("client_id", "An identifier for the client connection."),
			docs.FieldString("target_version", "The version of the Kafka protocol to use. This limits the capabilities used by the client and should ideally match the version of your brokers."),
			docs.FieldAdvanced("rack_id", "A rack identifier for this client."),
			docs.FieldCommon("key", "The key to publish messages with.").IsInterpolated(),
			docs.FieldCommon("partitioner", "The partitioning algorithm to use.").HasOptions("fnv1a_hash", "murmur2_hash", "random", "round_robin", "manual"),
			docs.FieldAdvanced("partition", "The manually-specified partition to publish messages to, relevant only when the field `partitioner` is set to `manual`. Must be able to parse as a 32-bit integer.").IsInterpolated(),
			docs.FieldCommon("compression", "The compression algorithm to use.").HasOptions("none", "snappy", "lz4", "gzip", "zstd"),
			docs.FieldString("static_headers", "An optional map of static headers that should be added to messages in addition to metadata.", map[string]string{"first-static-header": "value-1", "second-static-header": "value-2"}).Map(),
			docs.FieldCommon("metadata", "Specify criteria for which metadata values are sent with messages as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			output.InjectTracingSpanMappingDocs,
			docs.FieldCommon("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			docs.FieldAdvanced("ack_replicas", "Ensure that messages have been copied across all replicas before acknowledging receipt."),
			docs.FieldAdvanced("max_msg_bytes", "The maximum size in bytes of messages sent to the target topic."),
			docs.FieldAdvanced("timeout", "The maximum period of time to wait for message sends before abandoning the request and retrying."),
			docs.FieldAdvanced("retry_as_batch", "When enabled forces an entire batch of messages to be retried if any individual message fails on a send, otherwise only the individual messages that failed are retried. Disabling this helps to reduce message duplicates during intermittent errors, but also makes it impossible to guarantee strict ordering of messages."),
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
	w, err := NewAsyncWriter(
		TypeKafka, conf.Kafka.MaxInFlight, k, log, stats,
	)
	if err != nil {
		return nil, err
	}

	if conf.Kafka.InjectTracingMap != "" {
		aw, ok := w.(*AsyncWriter)
		if !ok {
			return nil, fmt.Errorf("unable to set an inject_tracing_map due to wrong type: %T", w)
		}
		if err = aw.SetInjectTracingMap(conf.Kafka.InjectTracingMap); err != nil {
			return nil, fmt.Errorf("failed to initialize inject tracing map: %v", err)
		}
	}

	return NewBatcherFromConfig(conf.Kafka.Batching, w, mgr, log, stats)
}
