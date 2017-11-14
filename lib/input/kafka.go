// Copyright (c) 2014 Ashley Jeffs
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
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["kafka"] = typeSpec{
		constructor: NewKafka,
		description: `
Connects to a kafka (0.8+) server. Offsets are managed within kafka as per the
consumer group (set via config). Only one partition per input is supported, if
you wish to balance partitions across a consumer group look at the
'kafka_balanced' input type instead.`,
	}
}

//------------------------------------------------------------------------------

// KafkaConfig is configuration for the Kafka input type.
type KafkaConfig struct {
	Addresses       []string `json:"addresses" yaml:"addresses"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	ConsumerGroup   string   `json:"consumer_group" yaml:"consumer_group"`
	Topic           string   `json:"topic" yaml:"topic"`
	Partition       int32    `json:"partition" yam:"partition"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:       []string{"localhost:9092"},
		ClientID:        "benthos_kafka_input",
		ConsumerGroup:   "benthos_consumer_group",
		Topic:           "benthos_stream",
		Partition:       0,
		StartFromOldest: true,
	}
}

//------------------------------------------------------------------------------

// Kafka is an input type that reads from a Kafka instance.
type Kafka struct {
	running int32

	client        sarama.Client
	coordinator   *sarama.Broker
	consumer      sarama.Consumer
	topicConsumer sarama.PartitionConsumer

	offset int64

	conf  Config
	stats metrics.Type
	log   log.Modular

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewKafka creates a new Kafka input type.
func NewKafka(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	k := Kafka{
		running:    1,
		offset:     0,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.kafka"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	config := sarama.NewConfig()
	config.ClientID = conf.Kafka.ClientID
	config.Net.DialTimeout = time.Second
	config.Consumer.Return.Errors = true

	var err error
	defer func() {
		if err != nil {
			k.closeClients()
		}
	}()

	k.client, err = sarama.NewClient(conf.Kafka.Addresses, config)
	if err != nil {
		return nil, err
	}

	k.coordinator, err = k.client.Coordinator(conf.Kafka.ConsumerGroup)
	if err != nil {
		return nil, err
	}

	k.consumer, err = sarama.NewConsumerFromClient(k.client)
	if err != nil {
		return nil, err
	}

	offsetReq := sarama.OffsetFetchRequest{}
	offsetReq.ConsumerGroup = k.conf.Kafka.ConsumerGroup
	offsetReq.AddPartition(k.conf.Kafka.Topic, k.conf.Kafka.Partition)

	if offsetRes, err := k.coordinator.FetchOffset(&offsetReq); err == nil {
		offsetBlock := offsetRes.Blocks[k.conf.Kafka.Topic][k.conf.Kafka.Partition]
		if offsetBlock.Err == sarama.ErrNoError {
			k.offset = offsetBlock.Offset
		}
	}

	k.topicConsumer, err = k.consumer.ConsumePartition(
		k.conf.Kafka.Topic, k.conf.Kafka.Partition, k.offset,
	)
	if err != nil {
		offsetTarget := sarama.OffsetOldest
		if !k.conf.Kafka.StartFromOldest {
			offsetTarget = sarama.OffsetNewest
			k.log.Warnln("Failed to read from stored offset, restarting from newest offset")
		} else {
			k.log.Warnln("Failed to read from stored offset, restarting from oldest offset")
		}

		k.log.Warnf(
			"Attempting to obtain offset for topic %s, partition %v\n",
			k.conf.Kafka.Topic, k.conf.Kafka.Partition,
		)

		// Get the new offset target
		if k.offset, err = k.client.GetOffset(
			k.conf.Kafka.Topic, k.conf.Kafka.Partition, offsetTarget,
		); err == nil {
			k.topicConsumer, err = k.consumer.ConsumePartition(
				k.conf.Kafka.Topic, k.conf.Kafka.Partition, k.offset,
			)
		}
	}

	return &k, err
}

//------------------------------------------------------------------------------

func (k *Kafka) commitOffset() error {
	commitReq := sarama.OffsetCommitRequest{}
	commitReq.ConsumerGroup = k.conf.Kafka.ConsumerGroup
	commitReq.AddBlock(k.conf.Kafka.Topic, k.conf.Kafka.Partition, k.offset, 0, "")

	commitRes, err := k.coordinator.CommitOffset(&commitReq)
	if err != nil {
		return err
	}
	err = commitRes.Errors[k.conf.Kafka.Topic][k.conf.Kafka.Partition]
	if err != sarama.ErrNoError {
		return err
	}
	return nil
}

//------------------------------------------------------------------------------

// drainConsumer should be called after closeClients.
func (k *Kafka) drainConsumer() {
	if k.topicConsumer != nil {
		// Drain both channels
		for range k.topicConsumer.Messages() {
		}
		for range k.topicConsumer.Errors() {
		}

		k.topicConsumer = nil
	}
}

// closeClients closes the kafka clients, this interrupts loop() out of the read
// block.
func (k *Kafka) closeClients() {
	if k.topicConsumer != nil {
		// NOTE: Needs draining before destroying.
		k.topicConsumer.AsyncClose()
	}
	if k.coordinator != nil {
		k.coordinator.Close()
		k.coordinator = nil
	}
	if k.client != nil {
		k.client.Close()
		k.client = nil
	}
}

//------------------------------------------------------------------------------

func (k *Kafka) errorLoop() {
	for atomic.LoadInt32(&k.running) == 1 {
		if err := <-k.topicConsumer.Errors(); err != nil {
			k.log.Errorf("Kafka message recv error: %v\n", err)
			k.stats.Incr("input.kafka.recv.error", 1)
		}
	}
}

func (k *Kafka) loop() {
	k.log.Infof("Receiving Kafka messages from addresses: %s\n", k.conf.Kafka.Addresses)

	defer func() {
		if atomic.CompareAndSwapInt32(&k.running, 1, 0) {
			k.closeClients()
		}

		k.drainConsumer()
		close(k.messages)
		close(k.closedChan)
	}()

	var data *sarama.ConsumerMessage

	for atomic.LoadInt32(&k.running) == 1 {
		// If no bytes then read a message
		if data == nil {
			var open bool
			if data, open = <-k.topicConsumer.Messages(); !open {
				return
			}
			k.stats.Incr("input.kafka.count", 1)
		}

		// If bytes are read then try and propagate.
		if data != nil {
			start := time.Now()
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
				k.stats.Timing("input.kafka.timing", int64(time.Since(start)))
				k.offset = data.Offset + 1
				if !res.SkipAck() {
					if err := k.commitOffset(); err != nil {
						k.log.Errorf("Failed to commit offset: %v\n", err)
					}
				}
				k.stats.Incr("input.kafka.send.success", 1)
				data = nil
			} else {
				k.stats.Incr("input.kafka.send.error", 1)
			}
		}
	}

}

// StartListening sets the channel used by the input to validate message
// receipt.
func (k *Kafka) StartListening(responses <-chan types.Response) error {
	if k.responses != nil {
		return types.ErrAlreadyStarted
	}
	k.responses = responses
	go k.loop()
	go k.errorLoop()
	return nil
}

// MessageChan returns the messages channel.
func (k *Kafka) MessageChan() <-chan types.Message {
	return k.messages
}

// CloseAsync shuts down the Kafka input and stops processing requests.
func (k *Kafka) CloseAsync() {
	if atomic.CompareAndSwapInt32(&k.running, 1, 0) {
		close(k.closeChan)
		k.closeClients()
	}
}

// WaitForClose blocks until the Kafka input has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	select {
	case <-k.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
