package input

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/impl/kafka/sasl"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
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
	MultiHeader         bool                     `json:"multi_header" yaml:"multi_header"`
	Batching            batchconfig.Config       `json:"batching" yaml:"batching"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:           []string{},
		Topics:              []string{},
		ClientID:            "benthos",
		RackID:              "",
		ConsumerGroup:       "",
		Group:               NewKafkaBalancedGroupConfig(),
		CommitPeriod:        "1s",
		CheckpointLimit:     1024,
		MaxProcessingPeriod: "100ms",
		FetchBufferCap:      256,
		StartFromOldest:     true,
		TargetVersion:       "2.0.0",
		TLS:                 btls.NewConfig(),
		SASL:                sasl.NewConfig(),
		MultiHeader:         false,
		Batching:            batchconfig.NewConfig(),
	}
}
