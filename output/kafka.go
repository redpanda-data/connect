/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package output

import (
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jeffail/benthos/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["kafka"] = NewKafka
}

//--------------------------------------------------------------------------------------------------

// KafkaConfig - Configuration for the Kafka output type.
type KafkaConfig struct {
	Addresses []string `json:"addresses" yaml:"addresses"`
	ClientID  string   `json:"client_id" yaml:"client_id"`
	Topic     string   `json:"topic" yaml:"topic"`
}

// NewKafkaConfig - Creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses: []string{"localhost:9092"},
		ClientID:  "benthos_kafka_output",
		Topic:     "benthos_stream",
	}
}

//--------------------------------------------------------------------------------------------------

// Kafka - An output type that writes messages into kafka.
type Kafka struct {
	running int32

	log   *log.Logger
	stats metrics.Aggregator

	conf Config

	producer sarama.AsyncProducer

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
}

// NewKafka - Create a new Kafka output type.
func NewKafka(conf Config, log *log.Logger, stats metrics.Aggregator) (Type, error) {
	k := Kafka{
		running:      1,
		log:          log.NewModule(".output.kafka"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.Kafka.ClientID

	var err error
	k.producer, err = sarama.NewAsyncProducer(k.conf.Kafka.Addresses, config)

	return &k, err
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe, does not use select.
func (k *Kafka) loop() {
	k.log.Infof("Sending Kafka messages to addresses: %s\n", k.conf.Kafka.Addresses)

	for atomic.LoadInt32(&k.running) == 1 {
		msg, open := <-k.messages
		if !open {
			atomic.StoreInt32(&k.running, 0)
		} else {
			var val []byte
			if len(msg.Parts) == 1 {
				val = msg.Parts[0]
			} else {
				val = msg.Bytes()
			}
			k.producer.Input() <- &sarama.ProducerMessage{
				Topic: k.conf.Kafka.Topic,
				Value: sarama.ByteEncoder(val),
			}
			k.responseChan <- types.NewSimpleResponse(nil)
		}
	}

	if nil != k.producer {
		k.producer.AsyncClose()
	}
	close(k.responseChan)
	close(k.closedChan)
}

// StartReceiving - Assigns a messages channel for the output to read.
func (k *Kafka) StartReceiving(msgs <-chan types.Message) error {
	if k.messages != nil {
		return types.ErrAlreadyStarted
	}
	k.messages = msgs
	go k.loop()
	return nil
}

// ResponseChan - Returns the errors channel.
func (k *Kafka) ResponseChan() <-chan types.Response {
	return k.responseChan
}

// CloseAsync - Shuts down the Kafka output and stops processing messages.
func (k *Kafka) CloseAsync() {
	atomic.StoreInt32(&k.running, 0)
}

// WaitForClose - Blocks until the Kafka output has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	select {
	case <-k.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
