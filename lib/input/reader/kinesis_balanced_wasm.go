// +build wasm

package reader

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

var errNoWASM = errors.New("component not supported in WASM builds")

// KinesisBalancedConfig is configuration values for the input type.
type KinesisBalancedConfig struct {
	MaxBatchCount int                `json:"max_batch_count" yaml:"max_batch_count"`
	Batching      batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewKinesisBalancedConfig creates a new Config with default values.
func NewKinesisBalancedConfig() KinesisBalancedConfig {
	return KinesisBalancedConfig{}
}

// KinesisBalanced is a benthos reader.Type implementation that reads messages
// from an Amazon Kinesis stream.
type KinesisBalanced struct{}

// NewKinesisBalanced creates a new Amazon Kinesis stream reader.Type.
func NewKinesisBalanced(
	conf KinesisBalancedConfig,
	log log.Modular,
	stats metrics.Type,
) (*KinesisBalanced, error) {
	return nil, errNoWASM
}

// Connect attempts to establish a connection to the target Kinesis stream.
func (k *KinesisBalanced) Connect() error {
	return errNoWASM
}

// ConnectWithContext attempts to establish a connection to the target Kinesis
// stream.
func (k *KinesisBalanced) ConnectWithContext(ctx context.Context) error {
	return errNoWASM
}

// ReadWithContext attempts to read a new message from the target Kinesis
// stream.
func (k *KinesisBalanced) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	return nil, nil, errNoWASM
}

// Read attempts to read a new message from the target Kinesis stream.
func (k *KinesisBalanced) Read() (types.Message, error) {
	return nil, errNoWASM
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (k *KinesisBalanced) Acknowledge(err error) error {
	return errNoWASM
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (k *KinesisBalanced) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (k *KinesisBalanced) WaitForClose(time.Duration) error {
	return errNoWASM
}

//------------------------------------------------------------------------------
