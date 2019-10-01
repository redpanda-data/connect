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
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/streadway/amqp"
)

//------------------------------------------------------------------------------

// AMQP09QueueDeclareConfig contains fields indicating whether the target AMQP09
// queue needs to be declared and bound to an exchange, as well as any fields
// specifying how to accomplish that.
type AMQP09QueueDeclareConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
	Durable bool `json:"durable" yaml:"durable"`
}

// AMQP09BindingConfig contains fields describing a queue binding to be
// declared.
type AMQP09BindingConfig struct {
	Exchange   string `json:"exchange" yaml:"exchange"`
	RoutingKey string `json:"key" yaml:"key"`
}

// AMQP09Config contains configuration for the AMQP09 input type.
type AMQP09Config struct {
	URL             string                   `json:"url" yaml:"url"`
	Queue           string                   `json:"queue" yaml:"queue"`
	QueueDeclare    AMQP09QueueDeclareConfig `json:"queue_declare" yaml:"queue_declare"`
	BindingsDeclare []AMQP09BindingConfig    `json:"bindings_declare" yaml:"bindings_declare"`
	ConsumerTag     string                   `json:"consumer_tag" yaml:"consumer_tag"`
	PrefetchCount   int                      `json:"prefetch_count" yaml:"prefetch_count"`
	PrefetchSize    int                      `json:"prefetch_size" yaml:"prefetch_size"`
	Batching        batch.PolicyConfig       `json:"batching" yaml:"batching"`
	TLS             btls.Config              `json:"tls" yaml:"tls"`
}

// NewAMQP09Config creates a new AMQP09Config with default values.
func NewAMQP09Config() AMQP09Config {
	batching := batch.NewPolicyConfig()
	batching.Count = 1
	return AMQP09Config{
		URL:   "amqp://guest:guest@localhost:5672/",
		Queue: "benthos-queue",
		QueueDeclare: AMQP09QueueDeclareConfig{
			Enabled: false,
			Durable: true,
		},
		ConsumerTag:     "benthos-consumer",
		PrefetchCount:   10,
		PrefetchSize:    0,
		TLS:             btls.NewConfig(),
		Batching:        batching,
		BindingsDeclare: []AMQP09BindingConfig{},
	}
}

//------------------------------------------------------------------------------

// AMQP09 is an input type that reads messages via the AMQP09 0.9 protocol.
type AMQP09 struct {
	conn         *amqp.Connection
	amqpChan     *amqp.Channel
	consumerChan <-chan amqp.Delivery

	ackTag  uint64
	tlsConf *tls.Config

	conf  AMQP09Config
	stats metrics.Type
	log   log.Modular

	m sync.RWMutex
}

// NewAMQP09 creates a new AMQP09 input type.
func NewAMQP09(conf AMQP09Config, log log.Modular, stats metrics.Type) (*AMQP09, error) {
	a := AMQP09{
		conf:  conf,
		stats: stats,
		log:   log,
	}
	if conf.TLS.Enabled {
		var err error
		if a.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an AMQP09 server.
func (a *AMQP09) ConnectWithContext(ctx context.Context) (err error) {
	a.m.Lock()
	defer a.m.Unlock()

	if a.conn != nil {
		return nil
	}

	var conn *amqp.Connection
	var amqpChan *amqp.Channel
	var consumerChan <-chan amqp.Delivery

	if a.conf.TLS.Enabled {
		conn, err = amqp.DialTLS(a.conf.URL, a.tlsConf)
		if err != nil {
			return fmt.Errorf("AMQP 0.9 Connect: %s", err)
		}
	} else {
		conn, err = amqp.Dial(a.conf.URL)
		if err != nil {
			return fmt.Errorf("AMQP 0.9 Connect: %s", err)
		}
	}

	amqpChan, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("AMQP 0.9 Channel: %s", err)
	}

	if a.conf.QueueDeclare.Enabled {
		if _, err = amqpChan.QueueDeclare(
			a.conf.Queue,                // name of the queue
			a.conf.QueueDeclare.Durable, // durable
			false,                       // delete when unused
			false,                       // exclusive
			false,                       // noWait
			nil,                         // arguments
		); err != nil {
			return fmt.Errorf("queue Declare: %s", err)
		}
	}

	for _, bConf := range a.conf.BindingsDeclare {
		if err = amqpChan.QueueBind(
			a.conf.Queue,     // name of the queue
			bConf.RoutingKey, // bindingKey
			bConf.Exchange,   // sourceExchange
			false,            // noWait
			nil,              // arguments
		); err != nil {
			return fmt.Errorf("queue Bind: %s", err)
		}
	}

	if err = amqpChan.Qos(
		a.conf.PrefetchCount, a.conf.PrefetchSize, false,
	); err != nil {
		return fmt.Errorf("qos: %s", err)
	}

	if consumerChan, err = amqpChan.Consume(
		a.conf.Queue,       // name
		a.conf.ConsumerTag, // consumerTag,
		false,              // noAck
		false,              // exclusive
		false,              // noLocal
		false,              // noWait
		nil,                // arguments
	); err != nil {
		return fmt.Errorf("queue Consume: %s", err)
	}

	a.conn = conn
	a.amqpChan = amqpChan
	a.consumerChan = consumerChan

	a.log.Infof("Receiving AMQP 0.9 messages from queue: %v\n", a.conf.Queue)
	return
}

// disconnect safely closes a connection to an AMQP09 server.
func (a *AMQP09) disconnect() error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.amqpChan != nil {
		err := a.amqpChan.Cancel(a.conf.ConsumerTag, true)
		a.amqpChan = nil
		if err != nil {
			return fmt.Errorf("consumer cancel failed: %s", err)
		}
	}
	if a.conn != nil {
		err := a.conn.Close()
		a.conn = nil
		if err != nil {
			return fmt.Errorf("AMQP 0.9 connection close error: %s", err)
		}
	}

	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new AMQP09 message.
func (a *AMQP09) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var c <-chan amqp.Delivery

	a.m.RLock()
	if a.conn != nil {
		c = a.consumerChan
	}
	a.m.RUnlock()

	if c == nil {
		return nil, nil, types.ErrNotConnected
	}

	msg := message.New(nil)
	addPart := func(data amqp.Delivery) {
		// Only store the latest delivery tag, but always Ack multiple.
		a.ackTag = data.DeliveryTag

		part := message.NewPart(data.Body)

		for k, v := range data.Headers {
			setMetadata(part, k, v)
		}

		setMetadata(part, "amqp_content_type", data.ContentType)
		setMetadata(part, "amqp_content_encoding", data.ContentEncoding)

		if data.DeliveryMode != 0 {
			setMetadata(part, "amqp_delivery_mode", data.DeliveryMode)
		}

		setMetadata(part, "amqp_priority", data.Priority)
		setMetadata(part, "amqp_correlation_id", data.CorrelationId)
		setMetadata(part, "amqp_reply_to", data.ReplyTo)
		setMetadata(part, "amqp_expiration", data.Expiration)
		setMetadata(part, "amqp_message_id", data.MessageId)

		if !data.Timestamp.IsZero() {
			setMetadata(part, "amqp_timestamp", data.Timestamp.Unix())
		}

		setMetadata(part, "amqp_type", data.Type)
		setMetadata(part, "amqp_user_id", data.UserId)
		setMetadata(part, "amqp_app_id", data.AppId)
		setMetadata(part, "amqp_consumer_tag", data.ConsumerTag)
		setMetadata(part, "amqp_delivery_tag", data.DeliveryTag)
		setMetadata(part, "amqp_redelivered", data.Redelivered)
		setMetadata(part, "amqp_exchange", data.Exchange)
		setMetadata(part, "amqp_routing_key", data.RoutingKey)

		msg.Append(part)
	}

	select {
	case data, open := <-c:
		if !open {
			a.disconnect()
			return nil, nil, types.ErrNotConnected
		}
		addPart(data)
		return msg, func(actx context.Context, res types.Response) error {
			if res.Error() != nil {
				return data.Nack(false, true)
			}
			return data.Ack(false)
		}, nil
	case <-ctx.Done():
	}
	return nil, nil, types.ErrTimeout
}

// CloseAsync shuts down the AMQP09 input and stops processing requests.
func (a *AMQP09) CloseAsync() {
	a.disconnect()
}

// WaitForClose blocks until the AMQP09 input has closed down.
func (a *AMQP09) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
