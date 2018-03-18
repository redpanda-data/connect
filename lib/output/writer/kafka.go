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

package writer

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/Shopify/sarama"
)

//------------------------------------------------------------------------------

// KafkaConfig is configuration for the Kafka output type.
type KafkaConfig struct {
	Addresses            []string `json:"addresses" yaml:"addresses"`
	ClientID             string   `json:"client_id" yaml:"client_id"`
	Key                  string   `json:"key" yaml:"key"`
	RoundRobinPartitions bool     `json:"round_robin_partitions" yaml:"round_robin_partitions"`
	Topic                string   `json:"topic" yaml:"topic"`
	MaxMsgBytes          int      `json:"max_msg_bytes" yaml:"max_msg_bytes"`
	TimeoutMS            int      `json:"timeout_ms" yaml:"timeout_ms"`
	AckReplicas          bool     `json:"ack_replicas" yaml:"ack_replicas"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:            []string{"localhost:9092"},
		ClientID:             "benthos_kafka_output",
		Key:                  "",
		RoundRobinPartitions: false,
		Topic:                "benthos_stream",
		MaxMsgBytes:          1000000,
		TimeoutMS:            5000,
		AckReplicas:          true,
	}
}

//------------------------------------------------------------------------------

// Kafka is a writer type that writes messages into kafka.
type Kafka struct {
	log   log.Modular
	stats metrics.Type

	addresses []string
	conf      KafkaConfig

	keyBytes       []byte
	interpolateKey bool

	producer sarama.SyncProducer
}

// NewKafka creates a new Kafka writer type.
func NewKafka(conf KafkaConfig, log log.Modular, stats metrics.Type) (*Kafka, error) {
	keyBytes := []byte(conf.Key)
	interpolateKey := text.ContainsFunctionVariables(keyBytes)

	k := Kafka{
		log:            log.NewModule(".output.kafka"),
		stats:          stats,
		conf:           conf,
		keyBytes:       keyBytes,
		interpolateKey: interpolateKey,
	}

	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				k.addresses = append(k.addresses, splitAddr)
			}
		}
	}

	return &k, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to a Kafka broker.
func (k *Kafka) Connect() error {
	if k.producer != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.ClientID

	config.Producer.MaxMessageBytes = k.conf.MaxMsgBytes
	config.Producer.Timeout = time.Duration(k.conf.TimeoutMS) * time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	if k.conf.RoundRobinPartitions {
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	}

	if k.conf.AckReplicas {
		config.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	var err error
	k.producer, err = sarama.NewSyncProducer(k.addresses, config)

	if err == nil {
		k.log.Infof("Sending Kafka messages to addresses: %s\n", k.addresses)
	}
	return err
}

// Write will attempt to write a message to Kafka, wait for acknowledgement, and
// returns an error if applicable.
func (k *Kafka) Write(msg types.Message) error {
	if k.producer == nil {
		return types.ErrNotConnected
	}

	msgs := []*sarama.ProducerMessage{}
	for _, part := range msg.GetAll() {
		if len(part) > k.conf.MaxMsgBytes {
			k.stats.Incr("output.kafka.send.dropped.max_msg_bytes", 1)
			continue
		}

		key := k.keyBytes
		if k.interpolateKey {
			key = text.ReplaceFunctionVariables(k.keyBytes)
		}
		nextMsg := &sarama.ProducerMessage{
			Topic: k.conf.Topic,
			Value: sarama.ByteEncoder(part),
		}
		if len(key) > 0 {
			nextMsg.Key = sarama.ByteEncoder(key)
		}
		msgs = append(msgs, nextMsg)
	}

	err := k.producer.SendMessages(msgs)
	if err != nil {
		if pErr, ok := err.(sarama.ProducerErrors); ok && len(pErr) > 0 {
			err = fmt.Errorf("failed to send %v parts from message: %v", len(pErr), pErr[0].Err)
		}
	}

	return err
}

// CloseAsync shuts down the Kafka writer and stops processing messages.
func (k *Kafka) CloseAsync() {
}

// WaitForClose blocks until the Kafka writer has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	if nil != k.producer {
		k.producer.Close()
		k.producer = nil
	}
	return nil
}

//------------------------------------------------------------------------------
