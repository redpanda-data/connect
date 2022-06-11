package input

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/session"
)

// DynamoDBCheckpointConfig contains configuration parameters for a DynamoDB
// based checkpoint store for Kinesis.
type DynamoDBCheckpointConfig struct {
	Table              string `json:"table" yaml:"table"`
	Create             bool   `json:"create" yaml:"create"`
	ReadCapacityUnits  int64  `json:"read_capacity_units" yaml:"read_capacity_units"`
	WriteCapacityUnits int64  `json:"write_capacity_units" yaml:"write_capacity_units"`
	BillingMode        string `json:"billing_mode" yaml:"billing_mode"`
}

// NewDynamoDBCheckpointConfig returns a DynamoDBCheckpoint config struct with
// default values.
func NewDynamoDBCheckpointConfig() DynamoDBCheckpointConfig {
	return DynamoDBCheckpointConfig{
		Table:              "",
		Create:             false,
		ReadCapacityUnits:  0,
		WriteCapacityUnits: 0,
		BillingMode:        "PAY_PER_REQUEST",
	}
}

// AWSKinesisConfig is configuration values for the input type.
type AWSKinesisConfig struct {
	session.Config  `json:",inline" yaml:",inline"`
	Streams         []string                 `json:"streams" yaml:"streams"`
	DynamoDB        DynamoDBCheckpointConfig `json:"dynamodb" yaml:"dynamodb"`
	CheckpointLimit int                      `json:"checkpoint_limit" yaml:"checkpoint_limit"`
	CommitPeriod    string                   `json:"commit_period" yaml:"commit_period"`
	LeasePeriod     string                   `json:"lease_period" yaml:"lease_period"`
	RebalancePeriod string                   `json:"rebalance_period" yaml:"rebalance_period"`
	StartFromOldest bool                     `json:"start_from_oldest" yaml:"start_from_oldest"`
	Batching        batchconfig.Config       `json:"batching" yaml:"batching"`
}

// NewAWSKinesisConfig creates a new Config with default values.
func NewAWSKinesisConfig() AWSKinesisConfig {
	return AWSKinesisConfig{
		Config:          session.NewConfig(),
		Streams:         []string{},
		DynamoDB:        NewDynamoDBCheckpointConfig(),
		CheckpointLimit: 1024,
		CommitPeriod:    "5s",
		LeasePeriod:     "30s",
		RebalancePeriod: "30s",
		StartFromOldest: true,
		Batching:        batchconfig.NewConfig(),
	}
}
