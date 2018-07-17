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
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
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
	TLSEnable            bool     `json:"tls_enable" yaml:"tls_enable"`
	SkipCertVerify       bool     `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	Topic                string   `json:"topic" yaml:"topic"`
	Compression          string   `json:"compression" yaml:"compression"`
	MaxMsgBytes          int      `json:"max_msg_bytes" yaml:"max_msg_bytes"`
	TimeoutMS            int      `json:"timeout_ms" yaml:"timeout_ms"`
	AckReplicas          bool     `json:"ack_replicas" yaml:"ack_replicas"`
	TargetVersion        string   `json:"target_version" yaml:"target_version"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Addresses:            []string{"localhost:9092"},
		ClientID:             "benthos_kafka_output",
		Key:                  "",
		RoundRobinPartitions: false,
		TLSEnable:            false,
		SkipCertVerify:       false,
		Topic:                "benthos_stream",
		Compression:          "none",
		MaxMsgBytes:          1000000,
		TimeoutMS:            5000,
		AckReplicas:          false,
		TargetVersion:        sarama.V1_0_0_0.String(),
	}
}

//------------------------------------------------------------------------------

// Kafka is a writer type that writes messages into kafka.
type Kafka struct {
	log   log.Modular
	stats metrics.Type

	addresses []string
	version   sarama.KafkaVersion
	conf      KafkaConfig

	keyBytes       []byte
	interpolateKey bool

	producer    sarama.SyncProducer
	compression sarama.CompressionCodec

	connMut sync.RWMutex
}

// NewKafka creates a new Kafka writer type.
func NewKafka(conf KafkaConfig, log log.Modular, stats metrics.Type) (*Kafka, error) {
	keyBytes := []byte(conf.Key)
	interpolateKey := text.ContainsFunctionVariables(keyBytes)

	compression, err := strToCompressionCodec(conf.Compression)
	if err != nil {
		return nil, err
	}

	k := Kafka{
		log:            log.NewModule(".output.kafka"),
		stats:          stats,
		conf:           conf,
		keyBytes:       keyBytes,
		interpolateKey: interpolateKey,
		compression:    compression,
	}

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

func strToCompressionCodec(str string) (sarama.CompressionCodec, error) {
	switch str {
	case "none":
		return sarama.CompressionNone, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	}
	return sarama.CompressionNone, fmt.Errorf("compression codec not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to a Kafka broker.
func (k *Kafka) Connect() error {
	k.connMut.Lock()
	defer k.connMut.Unlock()

	if k.producer != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.ClientID

	config.Version = k.version

	config.Producer.Compression = k.compression
	config.Producer.MaxMessageBytes = k.conf.MaxMsgBytes
	config.Producer.Timeout = time.Duration(k.conf.TimeoutMS) * time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = k.conf.TLSEnable
	if k.conf.SkipCertVerify {
		config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
	}

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
	k.connMut.RLock()
	producer := k.producer
	k.connMut.RUnlock()

	if producer == nil {
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

	err := producer.SendMessages(msgs)
	if err != nil {
		if pErr, ok := err.(sarama.ProducerErrors); ok && len(pErr) > 0 {
			err = fmt.Errorf("failed to send %v parts from message: %v", len(pErr), pErr[0].Err)
		}
	}

	return err
}

// CloseAsync shuts down the Kafka writer and stops processing messages.
func (k *Kafka) CloseAsync() {
	k.connMut.Lock()
	if nil != k.producer {
		k.producer.Close()
		k.producer = nil
	}
	k.connMut.Unlock()
}

// WaitForClose blocks until the Kafka writer has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
