package reader

import (
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Shopify/sarama"
)

// KafkaBalancedGroupConfig contains config fields for Kafka consumer groups.
type KafkaBalancedGroupConfig struct {
	SessionTimeout    string `json:"session_timeout" yaml:"session_timeout"`
	HeartbeatInterval string `json:"heartbeat_interval" yaml:"heartbeat_interval"`
	RebalanceTimeout  string `json:"rebalance_timeout" yaml:"rebalance_timeout"`
}

// NewKafkaBalancedGroupConfig returns a KafkaBalancedGroupConfig with default
// values.
func NewKafkaBalancedGroupConfig() KafkaBalancedGroupConfig {
	return KafkaBalancedGroupConfig{
		SessionTimeout:    "10s",
		HeartbeatInterval: "3s",
		RebalanceTimeout:  "60s",
	}
}

// KafkaConfig contains configuration fields for the Kafka input type.
type KafkaConfig struct {
	Addresses           []string                 `json:"addresses" yaml:"addresses"`
	Topics              []string                 `json:"topics" yaml:"topics"`
	ClientID            string                   `json:"client_id" yaml:"client_id"`
	RackID              string                   `json:"rack_id" yaml:"rack_id"`
	ConsumerGroup       string                   `json:"consumer_group" yaml:"consumer_group"`
	Group               KafkaBalancedGroupConfig `json:"group" yaml:"group"`
	CommitPeriod        string                   `json:"commit_period" yaml:"commit_period"`
	CheckpointLimit     int                      `json:"checkpoint_limit" yaml:"checkpoint_limit"`
	ExtractTracingMap   string                   `json:"extract_tracing_map" yaml:"extract_tracing_map"`
	MaxProcessingPeriod string                   `json:"max_processing_period" yaml:"max_processing_period"`
	FetchBufferCap      int                      `json:"fetch_buffer_cap" yaml:"fetch_buffer_cap"`
	StartFromOldest     bool                     `json:"start_from_oldest" yaml:"start_from_oldest"`
	TargetVersion       string                   `json:"target_version" yaml:"target_version"`
	TLS                 btls.Config              `json:"tls" yaml:"tls"`
	SASL                sasl.Config              `json:"sasl" yaml:"sasl"`
	Batching            batch.PolicyConfig       `json:"batching" yaml:"batching"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:           []string{"localhost:9092"},
		Topics:              []string{},
		ClientID:            "benthos_kafka_input",
		RackID:              "",
		ConsumerGroup:       "benthos_consumer_group",
		Group:               NewKafkaBalancedGroupConfig(),
		CommitPeriod:        "1s",
		CheckpointLimit:     1,
		MaxProcessingPeriod: "100ms",
		FetchBufferCap:      256,
		StartFromOldest:     true,
		TargetVersion:       sarama.V2_0_0_0.String(),
		TLS:                 btls.NewConfig(),
		SASL:                sasl.NewConfig(),
		Batching:            batch.NewPolicyConfig(),
	}
}
