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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/streadway/amqp"
)

//------------------------------------------------------------------------------

// AMQPQueueDeclareConfig contains fields indicating whether the target AMQP
// queue needs to be declared and bound to an exchange, as well as any fields
// specifying how to accomplish that.
type AMQPQueueDeclareConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
	Durable bool `json:"durable" yaml:"durable"`
}

// AMQPBindingConfig contains fields describing a queue binding to be declared.
type AMQPBindingConfig struct {
	Exchange   string `json:"exchange" yaml:"exchange"`
	RoutingKey string `json:"key" yaml:"key"`
}

// AMQPConfig contains configuration for the AMQP input type.
type AMQPConfig struct {
	URL             string                 `json:"url" yaml:"url"`
	Queue           string                 `json:"queue" yaml:"queue"`
	QueueDeclare    AMQPQueueDeclareConfig `json:"queue_declare" yaml:"queue_declare"`
	BindingsDeclare []AMQPBindingConfig    `json:"bindings_declare" yaml:"bindings_declare"`
	ConsumerTag     string                 `json:"consumer_tag" yaml:"consumer_tag"`
	PrefetchCount   int                    `json:"prefetch_count" yaml:"prefetch_count"`
	PrefetchSize    int                    `json:"prefetch_size" yaml:"prefetch_size"`
	MaxBatchCount   int                    `json:"max_batch_count" yaml:"max_batch_count"`
	TLS             btls.Config            `json:"tls" yaml:"tls"`
}

// NewAMQPConfig creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URL:   "amqp://guest:guest@localhost:5672/",
		Queue: "benthos-queue",
		QueueDeclare: AMQPQueueDeclareConfig{
			Enabled: false,
			Durable: true,
		},
		ConsumerTag:     "benthos-consumer",
		PrefetchCount:   10,
		PrefetchSize:    0,
		TLS:             btls.NewConfig(),
		MaxBatchCount:   1,
		BindingsDeclare: []AMQPBindingConfig{},
	}
}

//------------------------------------------------------------------------------

// AMQP is an input type that reads messages via the AMQP 0.91 protocol.
type AMQP struct {
	conn         *amqp.Connection
	amqpChan     *amqp.Channel
	consumerChan <-chan amqp.Delivery

	ackTag  uint64
	tlsConf *tls.Config

	conf  AMQPConfig
	stats metrics.Type
	log   log.Modular

	m sync.RWMutex
}

// NewAMQP creates a new AMQP input type.
func NewAMQP(conf AMQPConfig, log log.Modular, stats metrics.Type) (Type, error) {
	a := AMQP{
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

// Connect establishes a connection to an AMQP server.
func (a *AMQP) Connect() (err error) {
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
			return fmt.Errorf("AMQP Connect: %s", err)
		}
	} else {
		conn, err = amqp.Dial(a.conf.URL)
		if err != nil {
			return fmt.Errorf("AMQP Connect: %s", err)
		}
	}

	amqpChan, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("AMQP Channel: %s", err)
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

	a.log.Infof("Receiving AMQP messages from queue: %v\n", a.conf.Queue)
	return
}

// disconnect safely closes a connection to an AMQP server.
func (a *AMQP) disconnect() error {
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
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}

	return nil
}

//------------------------------------------------------------------------------

// Determine the type of the value and set the metadata.
func setMetadata(p types.Part, k string, v interface{}) {
	var metaValue string
	var metaKey = strings.Replace(k, "-", "_", -1)

	switch v := v.(type) {
	case bool:
		metaValue = strconv.FormatBool(bool(v))
	case float32:
		metaValue = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		metaValue = strconv.FormatFloat(v, 'f', -1, 64)
	case byte:
		metaValue = strconv.Itoa(int(v))
	case int16:
		metaValue = strconv.Itoa(int(v))
	case int32:
		metaValue = strconv.Itoa(int(v))
	case int64:
		metaValue = strconv.Itoa(int(v))
	case nil:
		metaValue = ""
	case string:
		metaValue = v
	case []byte:
		metaValue = string(v[:])
	case time.Time:
		metaValue = v.Format(time.RFC3339)
	case amqp.Decimal:
		dec := strconv.Itoa(int(v.Value))
		index := len(dec) - int(v.Scale)
		metaValue = dec[:index] + "." + dec[index:]
	case amqp.Table:
		for key, value := range v {
			setMetadata(p, metaKey+"_"+key, value)
		}
		return
	default:
		metaValue = ""
	}

	if metaValue != "" {
		p.Metadata().Set(metaKey, metaValue)
	}
}

//------------------------------------------------------------------------------

// Read a new AMQP message.
func (a *AMQP) Read() (types.Message, error) {
	var c <-chan amqp.Delivery

	a.m.RLock()
	if a.conn != nil {
		c = a.consumerChan
	}
	a.m.RUnlock()

	if c == nil {
		return nil, types.ErrNotConnected
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

	data, open := <-c
	if !open {
		a.disconnect()
		return nil, types.ErrNotConnected
	}
	addPart(data)

batchLoop:
	for i := 1; i < a.conf.MaxBatchCount; i++ {
		select {
		case data, open = <-c:
			if !open {
				return nil, types.ErrTypeClosed
			}
			addPart(data)
		default:
			// Drained the buffer
			break batchLoop
		}
	}

	if msg.Len() == 0 {
		return nil, types.ErrTimeout
	}
	return msg, nil
}

// Acknowledge instructs whether unacknowledged messages have been successfully
// propagated.
func (a *AMQP) Acknowledge(err error) error {
	a.m.RLock()
	defer a.m.RUnlock()
	if a.conn == nil {
		return types.ErrNotConnected
	}
	if err != nil {
		return a.amqpChan.Reject(a.ackTag, true)
	}
	return a.amqpChan.Ack(a.ackTag, true)
}

// CloseAsync shuts down the AMQP input and stops processing requests.
func (a *AMQP) CloseAsync() {
	a.disconnect()
}

// WaitForClose blocks until the AMQP input has closed down.
func (a *AMQP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
