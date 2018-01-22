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
brokers, including RabbitMQ.`,
	}
}

//------------------------------------------------------------------------------

// AMQPConfig is configuration for the AMQP output type.
type AMQPConfig struct {
	URL          string `json:"url" yaml:"url"`
	Exchange     string `json:"exchange" yaml:"exchange"`
	ExchangeType string `json:"exchange_type" yaml:"exchange_type"`
	BindingKey   string `json:"key" yaml:"key"`
}

// NewAMQPConfig creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "benthos-exchange",
		ExchangeType: "direct",
		BindingKey:   "benthos-key",
	}
}

//------------------------------------------------------------------------------

// AMQP is an output type that serves AMQP messages.
type AMQP struct {
	running int32

	log   log.Modular
	stats metrics.Type

	conf Config

	conn            *amqp.Connection
	amqpChan        *amqp.Channel
	amqpConfirmChan <-chan amqp.Confirmation

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
	closeChan  chan struct{}
}

// NewAMQP creates a new AMQP output type.
func NewAMQP(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	a := AMQP{
		running:      1,
		log:          log.NewModule(".output.amqp"),
		stats:        stats,
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}

	return &a, nil
}

//------------------------------------------------------------------------------

// connect establishes a connection to an AMQP server.
func (a *AMQP) connect() (err error) {
	a.conn, err = amqp.Dial(a.conf.AMQP.URL)
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

	if err := a.amqpChan.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}
	a.amqpConfirmChan = a.amqpChan.NotifyPublish(make(chan amqp.Confirmation, 1))

	return
}

// disconnect safely closes a connection to an AMQP server.
func (a *AMQP) disconnect() error {
	if a.amqpChan != nil {
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

// loop is an internal loop that brokers incoming messages to output pipe.
func (a *AMQP) loop() {
	defer func() {
		atomic.StoreInt32(&a.running, 0)

		a.disconnect()
		a.stats.Decr("output.amqp.running", 1)

		close(a.responseChan)
		close(a.closedChan)
	}()
	a.stats.Incr("output.amqp.running", 1)

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
	a.log.Infof("Sending AMQP messages to URL: %s\n", a.conf.AMQP.URL)

	var open bool
	for atomic.LoadInt32(&a.running) == 1 {
		for a.amqpChan == nil {
			a.log.Warnln("Lost AMQP connection, attempting to reconnect.")
			if err := a.connect(); err != nil {
				a.stats.Incr("output.amqp.reconnect.error", 1)
				select {
				case <-time.After(time.Second):
				case <-a.closeChan:
					return
				}
			} else {
				a.log.Warnln("Successfully reconnected to AMQP.")
				a.stats.Incr("output.amqp.reconnect.success", 1)
			}
		}

		var msg types.Message
		select {
		case msg, open = <-a.messages:
			if !open {
				return
			}
		case <-a.closeChan:
			return
		}

		a.stats.Incr("output.amqp.count", 1)
		var err error
		for _, part := range msg.Parts {
			err = a.amqpChan.Publish(
				a.conf.AMQP.Exchange,   // publish to an exchange
				a.conf.AMQP.BindingKey, // routing to 0 or more queues
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					Headers:         amqp.Table{},
					ContentType:     "application/octet-stream",
					ContentEncoding: "",
					Body:            part,
					DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
					Priority:        0,              // 0-9
					// a bunch of application/implementation-specific fields
				},
			)
			if err == nil {
				select {
				case confirm := <-a.amqpConfirmChan:
					if !confirm.Ack {
						err = types.ErrNoAck
					}
				case <-a.closeChan:
					return
				}
			} else {
				a.disconnect()
			}
			if err == nil {
				a.stats.Incr("output.amqp.send.success", 1)
			} else {
				a.stats.Incr("output.amqp.send.error", 1)
				break
			}
		}

		select {
		case a.responseChan <- types.NewSimpleResponse(err):
		case <-a.closeChan:
			return
		}
	}
}

// StartReceiving assigns a messages channel for the output to read.
func (a *AMQP) StartReceiving(msgs <-chan types.Message) error {
	if a.messages != nil {
		return types.ErrAlreadyStarted
	}
	a.messages = msgs
	go a.loop()
	return nil
}

// ResponseChan returns the errors channel.
func (a *AMQP) ResponseChan() <-chan types.Response {
	return a.responseChan
}

// CloseAsync shuts down the AMQP output and stops processing messages.
func (a *AMQP) CloseAsync() {
	if atomic.CompareAndSwapInt32(&a.running, 1, 0) {
		close(a.closeChan)
	}
}

// WaitForClose blocks until the AMQP output has closed down.
func (a *AMQP) WaitForClose(timeout time.Duration) error {
	select {
	case <-a.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
