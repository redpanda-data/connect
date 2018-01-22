// Copyright (c) 2017 Ashley Jeffs
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

package input

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

//------------------------------------------------------------------------------

func init() {
	constructors["kafka_balanced"] = typeSpec{
		constructor: NewKafkaBalanced,
		description: `
Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.`,
	}
}

//------------------------------------------------------------------------------

// KafkaBalancedConfig is configuration for the KafkaBalanced input type.
type KafkaBalancedConfig struct {
	Addresses       []string `json:"addresses" yaml:"addresses"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	ConsumerGroup   string   `json:"consumer_group" yaml:"consumer_group"`
	Topics          []string `json:"topics" yaml:"topics"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
}

// NewKafkaBalancedConfig creates a new KafkaBalancedConfig with default values.
func NewKafkaBalancedConfig() KafkaBalancedConfig {
	return KafkaBalancedConfig{
		Addresses:       []string{"localhost:9092"},
		ClientID:        "benthos_kafka_input",
		ConsumerGroup:   "benthos_consumer_group",
		Topics:          []string{"benthos_stream"},
		StartFromOldest: true,
	}
}

//------------------------------------------------------------------------------

// KafkaBalanced is an input type that reads from a KafkaBalanced instance.
type KafkaBalanced struct {
	running int32

	consumer *cluster.Consumer

	offset int64

	addresses []string
	conf      Config
	stats     metrics.Type
	log       log.Modular

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewKafkaBalanced creates a new KafkaBalanced input type.
func NewKafkaBalanced(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	k := KafkaBalanced{
		running:    1,
		offset:     0,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.kafka_balanced"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}
	for _, addr := range conf.KafkaBalanced.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				k.addresses = append(k.addresses, splitAddr)
			}
		}
	}
	return &k, nil
}

//------------------------------------------------------------------------------

func (k *KafkaBalanced) connect() error {
	config := cluster.NewConfig()
	config.ClientID = k.conf.KafkaBalanced.ClientID
	config.Net.DialTimeout = time.Second
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	if k.conf.KafkaBalanced.StartFromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	var err error
	defer func() {
		if err != nil {
			k.closeClients()
		}
	}()

	k.consumer, err = cluster.NewConsumer(
		k.addresses,
		k.conf.KafkaBalanced.ConsumerGroup,
		k.conf.KafkaBalanced.Topics,
		config,
	)

	return err
}

// drainConsumer should be called after closeClients.
func (k *KafkaBalanced) drainConsumer() {
	if k.consumer != nil {
		// Drain all channels
		for range k.consumer.Messages() {
		}
		for range k.consumer.Notifications() {
		}
		for range k.consumer.Errors() {
		}
		k.consumer = nil
	}
}

// closeClients closes the kafka clients, this interrupts loop() out of the read
// block.
func (k *KafkaBalanced) closeClients() {
	if k.consumer != nil {
		// NOTE: Needs draining before destroying.
		k.consumer.Close()
	}
}

//------------------------------------------------------------------------------

func (k *KafkaBalanced) errorLoop() {
	for atomic.LoadInt32(&k.running) == 1 {
		select {
		case err := <-k.consumer.Errors():
			if err != nil {
				k.log.Errorf("KafkaBalanced message recv error: %v\n", err)
				k.stats.Incr("input.kafka_balanced.recv.error", 1)
			}
		case <-k.consumer.Notifications():
			k.stats.Incr("input.kafka_balanced.rebalanced", 1)
		case <-k.closeChan:
			return
		}
	}
}

func (k *KafkaBalanced) loop() {
	defer func() {
		if atomic.CompareAndSwapInt32(&k.running, 1, 0) {
			k.closeClients()
		}
		k.stats.Decr("input.kafka_balanced.running", 1)

		k.drainConsumer()
		close(k.messages)
		close(k.closedChan)
	}()
	k.stats.Incr("input.kafka_balanced.running", 1)

	for {
		if err := k.connect(); err != nil {
			k.log.Errorf("Failed to connect to Kafka: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-k.closeChan:
				return
			}
		} else {
			break
		}
	}
	k.log.Infof("Receiving KafkaBalanced messages from addresses: %s\n", k.addresses)

	go k.errorLoop()

	var data *sarama.ConsumerMessage

	for atomic.LoadInt32(&k.running) == 1 {
		// If no bytes then read a message
		if data == nil {
			var open bool
			if data, open = <-k.consumer.Messages(); !open {
				return
			}
			k.stats.Incr("input.kafka_balanced.count", 1)
		}

		// If bytes are read then try and propagate.
		if data != nil {
			select {
			case k.messages <- types.Message{Parts: [][]byte{data.Value}}:
			case <-k.closeChan:
				return
			}
			res, open := <-k.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				k.offset = data.Offset + 1
				if !res.SkipAck() {
					k.consumer.MarkOffset(data, "")
					if err := k.consumer.CommitOffsets(); err != nil {
						k.log.Errorf("Failed to commit offset: %v\n", err)
					}
				}
				k.stats.Incr("input.kafka_balanced.send.success", 1)
				data = nil
			} else {
				k.stats.Incr("input.kafka_balanced.send.error", 1)
			}
		}
	}

}

// StartListening sets the channel used by the input to validate message
// receipt.
func (k *KafkaBalanced) StartListening(responses <-chan types.Response) error {
	if k.responses != nil {
		return types.ErrAlreadyStarted
	}
	k.responses = responses
	go k.loop()
	return nil
}

// MessageChan returns the messages channel.
func (k *KafkaBalanced) MessageChan() <-chan types.Message {
	return k.messages
}

// CloseAsync shuts down the KafkaBalanced input and stops processing requests.
func (k *KafkaBalanced) CloseAsync() {
	if atomic.CompareAndSwapInt32(&k.running, 1, 0) {
		close(k.closeChan)
		k.closeClients()
	}
}

// WaitForClose blocks until the KafkaBalanced input has closed down.
func (k *KafkaBalanced) WaitForClose(timeout time.Duration) error {
	select {
	case <-k.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
