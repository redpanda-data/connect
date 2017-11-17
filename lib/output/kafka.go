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

package output

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
The kafka output type writes messages to a kafka broker, these messages are
acknowledged, which is propagated back to the input. The config field
'ack_replicas' determines whether we wait for acknowledgement from all replicas
or just a single broker.`,
	}
}

//------------------------------------------------------------------------------

// KafkaConfig is configuration for the Kafka output type.
type KafkaConfig struct {
	Addresses   []string `json:"addresses" yaml:"addresses"`
	ClientID    string   `json:"client_id" yaml:"client_id"`
	Topic       string   `json:"topic" yaml:"topic"`
	TimeoutMS   int      `json:"timeout_ms" yaml:"timeout_ms"`
	AckReplicas bool     `json:"ack_replicas" yaml:"ack_replicas"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:   []string{"localhost:9092"},
		ClientID:    "benthos_kafka_output",
		Topic:       "benthos_stream",
		TimeoutMS:   5000,
		AckReplicas: true,
	}
}

//------------------------------------------------------------------------------

// Kafka is an output type that writes messages into kafka.
type Kafka struct {
	running int32

	log   log.Modular
	stats metrics.Type

	conf Config

	producer sarama.SyncProducer

	messages     <-chan types.Message
	responseChan chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewKafka creates a new Kafka output type.
func NewKafka(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	k := Kafka{
		running:      1,
		log:          log.NewModule(".output.kafka"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.Kafka.ClientID

	config.Producer.Timeout = time.Duration(conf.Kafka.TimeoutMS) * time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	if conf.Kafka.AckReplicas {
		config.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	var err error
	k.producer, err = sarama.NewSyncProducer(k.conf.Kafka.Addresses, config)

	return &k, err
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to output pipe, does
// not use select.
func (k *Kafka) loop() {
	defer func() {
		atomic.StoreInt32(&k.running, 0)
		if nil != k.producer {
			k.producer.Close()
		}
		close(k.responseChan)
		close(k.closedChan)
	}()

	k.log.Infof("Sending Kafka messages to addresses: %s\n", k.conf.Kafka.Addresses)

	var open bool
	for atomic.LoadInt32(&k.running) == 1 {
		var msg types.Message
		if msg, open = <-k.messages; !open {
			return
		}
		k.stats.Incr("output.kafka.count", 1)
		var err error
		for _, part := range msg.Parts {
			if _, _, err = k.producer.SendMessage(&sarama.ProducerMessage{
				Topic: k.conf.Kafka.Topic,
				Value: sarama.ByteEncoder(part),
			}); err != nil {
				k.stats.Incr("output.kafka.send.error", 1)
				break
			} else {
				k.stats.Incr("output.kafka.send.success", 1)
			}
		}
		select {
		case k.responseChan <- types.NewSimpleResponse(err):
		case <-k.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (k *Kafka) StartReceiving(msgs <-chan types.Message) error {
	if k.messages != nil {
		return types.ErrAlreadyStarted
	}
	k.messages = msgs
	go k.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (k *Kafka) ResponseChan() <-chan types.Response {
	return k.responseChan
}

// CloseAsync shuts down the Kafka output and stops processing messages.
func (k *Kafka) CloseAsync() {
	if atomic.CompareAndSwapInt32(&k.running, 1, 0) {
		close(k.closeChan)
	}
}

// WaitForClose blocks until the Kafka output has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	select {
	case <-k.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
