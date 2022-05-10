package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/impl/kafka/sasl"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

// KafkaConfig contains configuration fields for the Kafka output type.
type KafkaConfig struct {
	Addresses        []string    `json:"addresses" yaml:"addresses"`
	ClientID         string      `json:"client_id" yaml:"client_id"`
	RackID           string      `json:"rack_id" yaml:"rack_id"`
	Key              string      `json:"key" yaml:"key"`
	Partitioner      string      `json:"partitioner" yaml:"partitioner"`
	Partition        string      `json:"partition" yaml:"partition"`
	Topic            string      `json:"topic" yaml:"topic"`
	Compression      string      `json:"compression" yaml:"compression"`
	MaxMsgBytes      int         `json:"max_msg_bytes" yaml:"max_msg_bytes"`
	Timeout          string      `json:"timeout" yaml:"timeout"`
	AckReplicas      bool        `json:"ack_replicas" yaml:"ack_replicas"`
	TargetVersion    string      `json:"target_version" yaml:"target_version"`
	TLS              btls.Config `json:"tls" yaml:"tls"`
	SASL             sasl.Config `json:"sasl" yaml:"sasl"`
	MaxInFlight      int         `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config   `json:",inline" yaml:",inline"`
	RetryAsBatch     bool                         `json:"retry_as_batch" yaml:"retry_as_batch"`
	Batching         batchconfig.Config           `json:"batching" yaml:"batching"`
	StaticHeaders    map[string]string            `json:"static_headers" yaml:"static_headers"`
	Metadata         metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	InjectTracingMap string                       `json:"inject_tracing_map" yaml:"inject_tracing_map"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "3s"
	rConf.Backoff.MaxInterval = "10s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return KafkaConfig{
		Addresses:     []string{},
		ClientID:      "benthos",
		RackID:        "",
		Key:           "",
		Partitioner:   "fnv1a_hash",
		Partition:     "",
		Topic:         "",
		Compression:   "none",
		MaxMsgBytes:   1000000,
		Timeout:       "5s",
		AckReplicas:   false,
		TargetVersion: "2.0.0",
		StaticHeaders: map[string]string{},
		Metadata:      metadata.NewExcludeFilterConfig(),
		TLS:           btls.NewConfig(),
		SASL:          sasl.NewConfig(),
		MaxInFlight:   64,
		Config:        rConf,
		RetryAsBatch:  false,
		Batching:      batchconfig.NewConfig(),
	}
}
