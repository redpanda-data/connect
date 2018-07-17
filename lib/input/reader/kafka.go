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

package reader

import (
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Shopify/sarama"
)

//------------------------------------------------------------------------------

// KafkaConfig is configuration for the Kafka input type.
type KafkaConfig struct {
	Addresses       []string `json:"addresses" yaml:"addresses"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	ConsumerGroup   string   `json:"consumer_group" yaml:"consumer_group"`
	TLSEnable       bool     `json:"tls_enable" yaml:"tls_enable"`
	Topic           string   `json:"topic" yaml:"topic"`
	Partition       int32    `json:"partition" yaml:"partition"`
	SkipCertVerify  bool     `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
	TargetVersion   string   `json:"target_version" yaml:"target_version"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:       []string{"localhost:9092"},
		ClientID:        "benthos_kafka_input",
		ConsumerGroup:   "benthos_consumer_group",
		TLSEnable:       false,
		Topic:           "benthos_stream",
		Partition:       0,
		SkipCertVerify:  false,
		StartFromOldest: true,
		TargetVersion:   sarama.V1_0_0_0.String(),
	}
}

//------------------------------------------------------------------------------

// Kafka is an input type that reads from a Kafka instance.
type Kafka struct {
	client       sarama.Client
	coordinator  *sarama.Broker
	partConsumer sarama.PartitionConsumer
	version      sarama.KafkaVersion

	sMut sync.Mutex

	offset int64

	addresses []string
	conf      KafkaConfig
	stats     metrics.Type
	log       log.Modular
}

// NewKafka creates a new Kafka input type.
func NewKafka(
	conf KafkaConfig, log log.Modular, stats metrics.Type,
) (*Kafka, error) {
	k := Kafka{
		offset: 0,
		conf:   conf,
		stats:  stats,
		log:    log.NewModule(".input.kafka"),
	}

	var err error
	if k.version, err = sarama.ParseKafkaVersion(conf.TargetVersion); err != nil {
		return nil, err
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

// closeClients closes the kafka clients, this interrupts loop() out of the read
// block.
func (k *Kafka) closeClients() {
	k.sMut.Lock()
	defer k.sMut.Unlock()

	if k.partConsumer != nil {
		// NOTE: Needs draining before destroying.
		k.partConsumer.AsyncClose()
		defer func() {
			// Drain both channels
			for range k.partConsumer.Messages() {
			}
			for range k.partConsumer.Errors() {
			}

			k.partConsumer = nil
		}()
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

// Connect establishes a Kafka connection.
func (k *Kafka) Connect() error {
	var err error
	defer func() {
		if err != nil {
			k.closeClients()
		}
	}()

	k.sMut.Lock()
	defer k.sMut.Unlock()

	if k.client != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.Version = k.version
	config.ClientID = k.conf.ClientID
	config.Net.DialTimeout = time.Second
	config.Consumer.Return.Errors = true
	config.Net.TLS.Enable = k.conf.TLSEnable
	if k.conf.SkipCertVerify {
		config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
	}

	k.client, err = sarama.NewClient(k.addresses, config)
	if err != nil {
		return err
	}

	k.coordinator, err = k.client.Coordinator(k.conf.ConsumerGroup)
	if err != nil {
		return err
	}

	var consumer sarama.Consumer
	consumer, err = sarama.NewConsumerFromClient(k.client)
	if err != nil {
		return err
	}

	offsetReq := sarama.OffsetFetchRequest{}
	offsetReq.ConsumerGroup = k.conf.ConsumerGroup
	offsetReq.AddPartition(k.conf.Topic, k.conf.Partition)

	if offsetRes, err := k.coordinator.FetchOffset(&offsetReq); err == nil {
		offsetBlock := offsetRes.Blocks[k.conf.Topic][k.conf.Partition]
		if offsetBlock.Err == sarama.ErrNoError {
			k.offset = offsetBlock.Offset
		}
	}

	var partConsumer sarama.PartitionConsumer
	partConsumer, err = consumer.ConsumePartition(
		k.conf.Topic, k.conf.Partition, k.offset,
	)
	if err != nil {
		offsetTarget := sarama.OffsetOldest
		if !k.conf.StartFromOldest {
			offsetTarget = sarama.OffsetNewest
			k.log.Warnln("Failed to read from stored offset, restarting from newest offset")
		} else {
			k.log.Warnln("Failed to read from stored offset, restarting from oldest offset")
		}

		k.log.Warnf(
			"Attempting to obtain offset for topic %s, partition %v\n",
			k.conf.Topic, k.conf.Partition,
		)

		// Get the new offset target
		if k.offset, err = k.client.GetOffset(
			k.conf.Topic, k.conf.Partition, offsetTarget,
		); err == nil {
			partConsumer, err = consumer.ConsumePartition(
				k.conf.Topic, k.conf.Partition, k.offset,
			)
		}
	}
	if err != nil {
		return err
	}

	k.partConsumer = partConsumer
	k.log.Infof("Receiving Kafka messages from addresses: %s\n", k.addresses)

	go func() {
		for err := range partConsumer.Errors() {
			if err != nil {
				k.log.Errorf("Kafka message recv error: %v\n", err)
				k.stats.Incr("input.kafka.recv.error", 1)
			}
		}
	}()

	return err
}

// Read attempts to read a message from a Kafka topic.
func (k *Kafka) Read() (types.Message, error) {
	var partConsumer sarama.PartitionConsumer

	k.sMut.Lock()
	partConsumer = k.partConsumer
	k.sMut.Unlock()

	if partConsumer == nil {
		return nil, types.ErrNotConnected
	}

	data, open := <-partConsumer.Messages()
	if !open {
		return nil, types.ErrTypeClosed
	}
	k.offset = data.Offset + 1
	return types.NewMessage([][]byte{data.Value}), nil
}

// Acknowledge instructs whether the current offset should be committed.
func (k *Kafka) Acknowledge(err error) error {
	if err != nil {
		return nil
	}

	var coordinator *sarama.Broker

	k.sMut.Lock()
	coordinator = k.coordinator
	k.sMut.Unlock()

	if coordinator == nil {
		return types.ErrNotConnected
	}

	commitReq := sarama.OffsetCommitRequest{}
	commitReq.ConsumerGroup = k.conf.ConsumerGroup
	commitReq.AddBlock(k.conf.Topic, k.conf.Partition, k.offset, 0, "")

	commitRes, err := coordinator.CommitOffset(&commitReq)
	if err == nil {
		err = commitRes.Errors[k.conf.Topic][k.conf.Partition]
		if err == sarama.ErrNoError {
			err = nil
		}
	}

	if err != nil {
		k.log.Errorf("Failed to commit offset: %v\n", err)

		k.sMut.Lock()
		defer k.sMut.Unlock()

		// Attempt to reconnect
		if newCoord, err := k.client.Coordinator(k.conf.ConsumerGroup); err != nil {
			k.log.Errorf("Failed to create new coordinator: %v\n", err)
		} else {
			k.coordinator.Close()
			k.coordinator = newCoord
		}
	}

	return nil
}

// CloseAsync shuts down the Kafka input and stops processing requests.
func (k *Kafka) CloseAsync() {
	go k.closeClients()
}

// WaitForClose blocks until the Kafka input has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
