// Copyright (c) 2019 Ashley Jeffs
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
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Shopify/sarama"
)

//------------------------------------------------------------------------------

// KafkaCGGroupConfig contains config fields for Kafka consumer groups.
type KafkaCGGroupConfig struct {
	SessionTimeout    string `json:"session_timeout" yaml:"session_timeout"`
	HeartbeatInterval string `json:"heartbeat_interval" yaml:"heartbeat_interval"`
	RebalanceTimeout  string `json:"rebalance_timeout" yaml:"rebalance_timeout"`
}

// NewKafkaCGGroupConfig returns a KafkaCGGroupConfig with default
// values.
func NewKafkaCGGroupConfig() KafkaCGGroupConfig {
	return KafkaCGGroupConfig{
		SessionTimeout:    "10s",
		HeartbeatInterval: "3s",
		RebalanceTimeout:  "60s",
	}
}

// KafkaCGConfig contains configuration for the KafkaCG input type.
type KafkaCGConfig struct {
	Addresses           []string           `json:"addresses" yaml:"addresses"`
	ClientID            string             `json:"client_id" yaml:"client_id"`
	ConsumerGroup       string             `json:"consumer_group" yaml:"consumer_group"`
	Group               KafkaCGGroupConfig `json:"group" yaml:"group"`
	CommitPeriod        string             `json:"commit_period" yaml:"commit_period"`
	MaxProcessingPeriod string             `json:"max_processing_period" yaml:"max_processing_period"`
	FetchBufferCap      int                `json:"fetch_buffer_cap" yaml:"fetch_buffer_cap"`
	Topics              []string           `json:"topics" yaml:"topics"`
	Batching            batch.PolicyConfig `json:"batching" yaml:"batching"`
	StartFromOldest     bool               `json:"start_from_oldest" yaml:"start_from_oldest"`
	TargetVersion       string             `json:"target_version" yaml:"target_version"`
	TLS                 btls.Config        `json:"tls" yaml:"tls"`
	SASL                SASLConfig         `json:"sasl" yaml:"sasl"`
}

// NewKafkaCGConfig creates a new KafkaCGConfig with default values.
func NewKafkaCGConfig() KafkaCGConfig {
	batchConf := batch.NewPolicyConfig()
	batchConf.Count = 1
	return KafkaCGConfig{
		Addresses:           []string{"localhost:9092"},
		ClientID:            "benthos_kafka_input",
		ConsumerGroup:       "benthos_consumer_group",
		Group:               NewKafkaCGGroupConfig(),
		CommitPeriod:        "1s",
		MaxProcessingPeriod: "100ms",
		FetchBufferCap:      256,
		Topics:              []string{"benthos_stream"},
		Batching:            batchConf,
		StartFromOldest:     true,
		TargetVersion:       sarama.V2_1_0_0.String(),
		TLS:                 btls.NewConfig(),
	}
}

//------------------------------------------------------------------------------

type asyncMessage struct {
	msg   types.Message
	ackFn AsyncAckFn
}

// KafkaCG is an input type that reads from a Kafka cluster by balancing
// partitions across other consumers of the same consumer group.
type KafkaCG struct {
	version   sarama.KafkaVersion
	tlsConf   *tls.Config
	addresses []string
	topics    []string

	commitPeriod      time.Duration
	sessionTimeout    time.Duration
	heartbeatInterval time.Duration
	rebalanceTimeout  time.Duration
	maxProcPeriod     time.Duration

	cMut          sync.Mutex
	groupCancelFn context.CancelFunc
	session       sarama.ConsumerGroupSession
	msgChan       chan asyncMessage

	mRebalanced metrics.StatCounter

	conf  KafkaCGConfig
	stats metrics.Type
	log   log.Modular
	mgr   types.Manager
}

// NewKafkaCG creates a new KafkaCG input type.
func NewKafkaCG(
	conf KafkaCGConfig, mgr types.Manager, log log.Modular, stats metrics.Type,
) (*KafkaCG, error) {
	k := KafkaCG{
		conf:          conf,
		stats:         stats,
		groupCancelFn: func() {},
		log:           log,
		mgr:           mgr,
		mRebalanced:   stats.GetCounter("rebalanced"),
	}
	if conf.TLS.Enabled {
		var err error
		if k.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if trimmed := strings.TrimSpace(splitAddr); len(trimmed) > 0 {
				k.addresses = append(k.addresses, trimmed)
			}
		}
	}
	for _, t := range conf.Topics {
		for _, splitTopics := range strings.Split(t, ",") {
			if trimmed := strings.TrimSpace(splitTopics); len(trimmed) > 0 {
				k.topics = append(k.topics, trimmed)
			}
		}
	}
	if tout := conf.CommitPeriod; len(tout) > 0 {
		var err error
		if k.commitPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse commit period string: %v", err)
		}
	}
	if tout := conf.Group.SessionTimeout; len(tout) > 0 {
		var err error
		if k.sessionTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse session timeout string: %v", err)
		}
	}
	if tout := conf.Group.HeartbeatInterval; len(tout) > 0 {
		var err error
		if k.heartbeatInterval, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse heartbeat interval string: %v", err)
		}
	}
	if tout := conf.Group.RebalanceTimeout; len(tout) > 0 {
		var err error
		if k.rebalanceTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse rebalance timeout string: %v", err)
		}
	}
	if tout := conf.MaxProcessingPeriod; len(tout) > 0 {
		var err error
		if k.maxProcPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse max processing period string: %v", err)
		}
	}

	var err error
	if k.version, err = sarama.ParseKafkaVersion(conf.TargetVersion); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (k *KafkaCG) Setup(sesh sarama.ConsumerGroupSession) error {
	k.cMut.Lock()
	k.session = sesh
	k.cMut.Unlock()
	k.mRebalanced.Incr(1)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have
// exited but before the offsets are committed for the very last time.
func (k *KafkaCG) Cleanup(sesh sarama.ConsumerGroupSession) error {
	k.cMut.Lock()
	k.session = nil
	k.cMut.Unlock()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (k *KafkaCG) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic, partition := claim.Topic(), claim.Partition()
	k.log.Debugf("Consuming messages from topic '%v' partition '%v'\n", topic, partition)
	defer k.log.Debugf("Stopped consuming messages from topic '%v' partition '%v'\n", topic, partition)

	ackChan := make(chan types.Response)

	latestOffset := claim.InitialOffset()
	batchPolicy, err := batch.NewPolicy(k.conf.Batching, k.mgr, k.log, k.stats)
	if err != nil {
		return fmt.Errorf("failed to initialise batch policy: %v", err)
	}
	var nextTimedBatchChan <-chan time.Time
	flushBatch := func() bool {
		nextTimedBatchChan = nil
		msg := batchPolicy.Flush()
		if msg == nil {
			return true
		}
		select {
		case k.msgChan <- asyncMessage{
			msg: msg,
			ackFn: func(ctx context.Context, res types.Response) error {
				select {
				case ackChan <- res:
					return nil
				case <-ctx.Done():
				}
				return types.ErrTimeout
			},
		}:
			select {
			case res := <-ackChan:
				if res.Error() != nil {
					k.log.Errorf("Received error from message batch: %v, shutting down consumer.\n", res.Error())
					return false
				}
				k.session.MarkOffset(claim.Topic(), claim.Partition(), latestOffset+1, "")
			case <-sess.Context().Done():
				return false
			}
		case <-sess.Context().Done():
			return false
		}
		return true
	}

	for {
		if nextTimedBatchChan == nil {
			if tNext := batchPolicy.UntilNext(); tNext >= 0 {
				nextTimedBatchChan = time.After(tNext)
			}
		}
		select {
		case <-nextTimedBatchChan:
			if !flushBatch() {
				return nil
			}
		case data, open := <-claim.Messages():
			if !open {
				return nil
			}
			latestOffset = data.Offset
			part := message.NewPart(data.Value)

			meta := part.Metadata()
			for _, hdr := range data.Headers {
				meta.Set(string(hdr.Key), string(hdr.Value))
			}

			lag := claim.HighWaterMarkOffset() - data.Offset - 1
			if lag < 0 {
				lag = 0
			}

			meta.Set("kafka_key", string(data.Key))
			meta.Set("kafka_partition", strconv.Itoa(int(data.Partition)))
			meta.Set("kafka_topic", data.Topic)
			meta.Set("kafka_offset", strconv.Itoa(int(data.Offset)))
			meta.Set("kafka_lag", strconv.FormatInt(lag, 10))
			meta.Set("kafka_timestamp_unix", strconv.FormatInt(data.Timestamp.Unix(), 10))

			if batchPolicy.Add(part) {
				if !flushBatch() {
					return nil
				}
			}
		case <-sess.Context().Done():
			return nil
		}
	}
}

//------------------------------------------------------------------------------

func (k *KafkaCG) closeGroup() {
	k.cMut.Lock()
	cancelFn := k.groupCancelFn
	k.cMut.Unlock()

	if cancelFn != nil {
		cancelFn()
	}
}

//------------------------------------------------------------------------------

// Connect establishes a KafkaCG connection.
func (k *KafkaCG) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.ClientID
	config.Net.DialTimeout = time.Second
	config.Version = k.version
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = k.maxProcPeriod
	config.Consumer.Offsets.CommitInterval = k.commitPeriod
	config.Consumer.Group.Session.Timeout = k.sessionTimeout
	config.Consumer.Group.Heartbeat.Interval = k.heartbeatInterval
	config.Consumer.Group.Rebalance.Timeout = k.rebalanceTimeout
	config.ChannelBufferSize = k.conf.FetchBufferCap

	if config.Net.ReadTimeout <= k.sessionTimeout {
		config.Net.ReadTimeout = k.sessionTimeout * 2
	}
	if config.Net.ReadTimeout <= k.rebalanceTimeout {
		config.Net.ReadTimeout = k.rebalanceTimeout * 2
	}

	config.Net.TLS.Enable = k.conf.TLS.Enabled
	if k.conf.TLS.Enabled {
		config.Net.TLS.Config = k.tlsConf
	}
	if k.conf.StartFromOldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	if k.conf.SASL.Enabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = k.conf.SASL.User
		config.Net.SASL.Password = k.conf.SASL.Password
	}

	// Start a new consumer group
	group, err := sarama.NewConsumerGroup(k.addresses, k.conf.ConsumerGroup, config)
	if err != nil {
		return err
	}

	// Handle errors
	go func() {
		for {
			gerr, open := <-group.Errors()
			if !open {
				return
			}
			if gerr != nil {
				k.log.Errorf("KafkaCG message recv error: %v\n", gerr)
				if cerr, ok := gerr.(*sarama.ConsumerError); ok {
					if cerr.Err == sarama.ErrUnknownMemberId {
						// Sarama doesn't seem to recover from this error.
						go k.closeGroup()
					}
				}
			}
		}
	}()

	// Handle session
	go func() {
	groupLoop:
		for {
			ctx, doneFn := context.WithCancel(context.Background())

			k.cMut.Lock()
			k.groupCancelFn = doneFn
			k.cMut.Unlock()

			k.log.Debugln("Starting consumer group")
			gerr := group.Consume(ctx, k.topics, k)
			select {
			case <-ctx.Done():
				break groupLoop
			default:
			}
			doneFn()
			if gerr != nil {
				if gerr != io.EOF {
					k.log.Errorf("KafkaCG group session error: %v\n", gerr)
				}
				break groupLoop
			}
		}
		k.log.Debugln("Closing consumer group")

		group.Close()

		k.cMut.Lock()
		if k.msgChan != nil {
			close(k.msgChan)
			k.msgChan = nil
		}
		k.cMut.Unlock()
	}()

	k.msgChan = make(chan asyncMessage, 0)

	k.log.Infof("Receiving kafka messages from brokers %s as group '%v'\n", k.addresses, k.conf.ConsumerGroup)
	return nil
}

// Read attempts to read a message from a KafkaCG topic.
func (k *KafkaCG) Read(ctx context.Context) (types.Message, AsyncAckFn, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, types.ErrNotConnected
	}

	select {
	case m, open := <-k.msgChan:
		if !open {
			return nil, nil, types.ErrNotConnected
		}
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
	}
	return nil, nil, types.ErrTimeout
}

// CloseAsync shuts down the KafkaCG input and stops processing requests.
func (k *KafkaCG) CloseAsync() {
	go k.closeGroup()
}

// WaitForClose blocks until the KafkaCG input has closed down.
func (k *KafkaCG) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
