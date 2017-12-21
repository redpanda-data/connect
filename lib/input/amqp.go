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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/streadway/amqp"
)

//------------------------------------------------------------------------------

func init() {
	constructors["amqp"] = typeSpec{
		constructor: NewAMQP,
		description: `
AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

Exchange type options are: direct|fanout|topic|x-custom`,
	}
}

//------------------------------------------------------------------------------

// AMQPConfig is configuration for the AMQP input type.
type AMQPConfig struct {
	URI           string `json:"uri" yaml:"uri"`
	Exchange      string `json:"exchange" yaml:"exchange"`
	ExchangeType  string `json:"exchange_type" yaml:"exchange_type"`
	Queue         string `json:"queue" yaml:"queue"`
	BindingKey    string `json:"key" yaml:"key"`
	ConsumerTag   string `json:"consumer_tag" yaml:"consumer_tag"`
	PrefetchCount int    `json:"prefetch_count" yaml:"prefetch_count"`
	PrefetchSize  int    `json:"prefetch_size" yaml:"prefetch_size"`
}

// NewAMQPConfig creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URI:           "amqp://guest:guest@localhost:5672/",
		Exchange:      "benthos-exchange",
		ExchangeType:  "direct",
		Queue:         "benthos-queue",
		BindingKey:    "benthos-key",
		ConsumerTag:   "benthos-consumer",
		PrefetchCount: 1,
		PrefetchSize:  0,
	}
}

//------------------------------------------------------------------------------

// AMQP is an input type that reads messages via the AMQP 0.91 protocol.
type AMQP struct {
	running int32

	conn         *amqp.Connection
	amqpChan     *amqp.Channel
	consumerChan <-chan amqp.Delivery

	conf  Config
	stats metrics.Type
	log   log.Modular

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewAMQP creates a new AMQP input type.
func NewAMQP(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	a := AMQP{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.amqp"),
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	return &a, nil
}

//------------------------------------------------------------------------------

// connect establishes a connection to an AMQP server.
func (a *AMQP) connect() (err error) {
	a.conn, err = amqp.Dial(a.conf.AMQP.URI)
	if err != nil {
		return fmt.Errorf("AMQP Connect: %s", err)
	}

	a.amqpChan, err = a.conn.Channel()
	if err != nil {
		return fmt.Errorf("AMQP Channel: %s", err)
	}

	if err = a.amqpChan.ExchangeDeclare(
		a.conf.AMQP.Exchange,     // name of the exchange
		a.conf.AMQP.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	if _, err = a.amqpChan.QueueDeclare(
		a.conf.AMQP.Queue, // name of the queue
		true,              // durable
		false,             // delete when usused
		false,             // exclusive
		false,             // noWait
		nil,               // arguments
	); err != nil {
		return fmt.Errorf("Queue Declare: %s", err)
	}

	if err = a.amqpChan.QueueBind(
		a.conf.AMQP.Queue,      // name of the queue
		a.conf.AMQP.BindingKey, // bindingKey
		a.conf.AMQP.Exchange,   // sourceExchange
		false,                  // noWait
		nil,                    // arguments
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	if err = a.amqpChan.Qos(
		a.conf.AMQP.PrefetchCount, a.conf.AMQP.PrefetchSize, false,
	); err != nil {
		return fmt.Errorf("Qos: %s", err)
	}

	if a.consumerChan, err = a.amqpChan.Consume(
		a.conf.AMQP.Queue,       // name
		a.conf.AMQP.ConsumerTag, // consumerTag,
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	return
}

// disconnect safely closes a connection to an AMQP server.
func (a *AMQP) disconnect() error {
	if a.amqpChan != nil {
		if err := a.amqpChan.Cancel(a.conf.AMQP.ConsumerTag, true); err != nil {
			return fmt.Errorf("Consumer cancel failed: %s", err)
		}
		a.amqpChan = nil
	}
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
		a.conn = nil
	}
	return nil
}

//------------------------------------------------------------------------------

func (a *AMQP) loop() {
	defer func() {
		atomic.StoreInt32(&a.running, 0)
		a.disconnect()

		close(a.messages)
		close(a.closedChan)
	}()

	for {
		if err := a.connect(); err != nil {
			a.log.Errorf("Failed to connect to AMQP: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-a.closeChan:
				return
			}
		} else {
			break
		}
	}
	a.log.Infof("Receiving AMQP messages from address: %s\n", a.conf.AMQP.URI)

	var data *amqp.Delivery

	for atomic.LoadInt32(&a.running) == 1 {
		// If no bytes then read a message
		if data == nil {
			select {
			case msg, open := <-a.consumerChan:
				if !open {
					a.log.Warnln("Lost AMQP connection, attempting to reconnect.")
					a.disconnect()
					if err := a.connect(); err != nil {
						a.stats.Incr("input.amqp.reconnect.error", 1)
						select {
						case <-time.After(time.Second):
						case <-a.closeChan:
							return
						}
					} else {
						a.log.Warnln("Successfully reconnected to AMQP.")
						a.stats.Incr("input.amqp.reconnect.success", 1)
					}
				} else {
					data = &msg
					a.stats.Incr("input.amqp.count", 1)
				}
			case <-a.closeChan:
				return
			}
		}

		// If bytes are read then try and propagate.
		if data != nil {
			select {
			case a.messages <- types.Message{Parts: [][]byte{data.Body}}:
			case <-a.closeChan:
				return
			}
			res, open := <-a.responses
			if !open {
				return
			}
			if resErr := res.Error(); resErr == nil {
				if !res.SkipAck() {
					data.Ack(true)
				}
				a.stats.Incr("input.amqp.send.success", 1)
				data = nil
			} else {
				a.stats.Incr("input.amqp.send.error", 1)
			}
		}
	}

}

// StartListening sets the channel used by the input to validate message
// receipt.
func (a *AMQP) StartListening(responses <-chan types.Response) error {
	if a.responses != nil {
		return types.ErrAlreadyStarted
	}
	a.responses = responses
	go a.loop()
	return nil
}

// MessageChan returns the messages channel.
func (a *AMQP) MessageChan() <-chan types.Message {
	return a.messages
}

// CloseAsync shuts down the AMQP input and stops processing requests.
func (a *AMQP) CloseAsync() {
	if atomic.CompareAndSwapInt32(&a.running, 1, 0) {
		close(a.closeChan)
	}
}

// WaitForClose blocks until the AMQP input has closed down.
func (a *AMQP) WaitForClose(timeout time.Duration) error {
	select {
	case <-a.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
