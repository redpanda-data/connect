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
	cluster "github.com/bsm/sarama-cluster"
)

//------------------------------------------------------------------------------

// KafkaBalancedConfig is configuration for the KafkaBalanced input type.
type KafkaBalancedConfig struct {
	Addresses       []string `json:"addresses" yaml:"addresses"`
	ClientID        string   `json:"client_id" yaml:"client_id"`
	ConsumerGroup   string   `json:"consumer_group" yaml:"consumer_group"`
	TLSEnable       bool     `json:"tls_enable" yaml:"tls_enable"`
	Topics          []string `json:"topics" yaml:"topics"`
	SkipCertVerify  bool     `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	StartFromOldest bool     `json:"start_from_oldest" yaml:"start_from_oldest"`
}

// NewKafkaBalancedConfig creates a new KafkaBalancedConfig with default values.
func NewKafkaBalancedConfig() KafkaBalancedConfig {
	return KafkaBalancedConfig{
		Addresses:       []string{"localhost:9092"},
		ClientID:        "benthos_kafka_input",
		ConsumerGroup:   "benthos_consumer_group",
		TLSEnable:       false,
		Topics:          []string{"benthos_stream"},
		SkipCertVerify:  false,
		StartFromOldest: true,
	}
}

//------------------------------------------------------------------------------

// KafkaBalanced is an input type that reads from a KafkaBalanced instance.
type KafkaBalanced struct {
	consumer *cluster.Consumer
	cMut     sync.Mutex

	addresses []string
	topics    []string
	conf      KafkaBalancedConfig
	stats     metrics.Type
	log       log.Modular
}

// NewKafkaBalanced creates a new KafkaBalanced input type.
func NewKafkaBalanced(
	conf KafkaBalancedConfig, log log.Modular, stats metrics.Type,
) (*KafkaBalanced, error) {
	k := KafkaBalanced{
		conf:  conf,
		stats: stats,
		log:   log.NewModule(".input.kafka_balanced"),
	}
	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if len(splitAddr) > 0 {
				k.addresses = append(k.addresses, splitAddr)
			}
		}
	}
	for _, t := range conf.Topics {
		for _, splitTopics := range strings.Split(t, ",") {
			if len(splitTopics) > 0 {
				k.topics = append(k.topics, splitTopics)
			}
		}
	}
	return &k, nil
}

//------------------------------------------------------------------------------

// closeClients closes the kafka clients, this interrupts Read().
func (k *KafkaBalanced) closeClients() {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.consumer != nil {
		k.consumer.Close()

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

//------------------------------------------------------------------------------

// Connect establishes a KafkaBalanced connection.
func (k *KafkaBalanced) Connect() error {
	k.cMut.Lock()
	defer k.cMut.Unlock()

	if k.consumer != nil {
		return nil
	}

	config := cluster.NewConfig()
	config.ClientID = k.conf.ClientID
	config.Net.DialTimeout = time.Second
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Net.TLS.Enable = k.conf.TLSEnable
	if k.conf.SkipCertVerify {
		config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
	}

	if k.conf.StartFromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	var consumer *cluster.Consumer
	var err error

	if consumer, err = cluster.NewConsumer(
		k.addresses,
		k.conf.ConsumerGroup,
		k.topics,
		config,
	); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case err, open := <-consumer.Errors():
				if !open {
					return
				}
				if err != nil {
					k.log.Errorf("KafkaBalanced message recv error: %v\n", err)
					k.stats.Incr("input.kafka_balanced.recv.error", 1)
				}
			case _, open := <-consumer.Notifications():
				if !open {
					return
				}
				k.stats.Incr("input.kafka_balanced.rebalanced", 1)
			}
		}
	}()

	k.consumer = consumer
	k.log.Infof("Receiving KafkaBalanced messages from addresses: %s\n", k.addresses)
	return nil
}

// Read attempts to read a message from a KafkaBalanced topic.
func (k *KafkaBalanced) Read() (types.Message, error) {
	var consumer *cluster.Consumer

	k.cMut.Lock()
	if k.consumer != nil {
		consumer = k.consumer
	}
	k.cMut.Unlock()

	if consumer == nil {
		return nil, types.ErrNotConnected
	}

	data, open := <-consumer.Messages()
	if !open {
		k.closeClients()
		return nil, types.ErrNotConnected
	}
	consumer.MarkOffset(data, "")
	return types.NewMessage([][]byte{data.Value}), nil
}

// Acknowledge instructs whether the current offset should be committed.
func (k *KafkaBalanced) Acknowledge(err error) error {
	if err != nil {
		return nil
	}

	var consumer *cluster.Consumer

	k.cMut.Lock()
	if k.consumer != nil {
		consumer = k.consumer
	}
	k.cMut.Unlock()

	if consumer == nil {
		return types.ErrNotConnected
	}

	return consumer.CommitOffsets()
}

// CloseAsync shuts down the KafkaBalanced input and stops processing requests.
func (k *KafkaBalanced) CloseAsync() {
	go k.closeClients()
}

// WaitForClose blocks until the KafkaBalanced input has closed down.
func (k *KafkaBalanced) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
