// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// +build wasm

package reader

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

var errNoWASM = errors.New("component not supported in WASM builds")

// KinesisBalancedConfig is configuration values for the input type.
type KinesisBalancedConfig struct{}

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
