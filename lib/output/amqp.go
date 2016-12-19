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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	"github.com/streadway/amqp"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["amqp"] = typeSpec{
		constructor: NewAMQP,
		description: `
AMQP is the underlying messaging protocol that is used my RabbitMQ. Support is
currently rather limited, but more configuration options are on the way.`,
	}
}

//--------------------------------------------------------------------------------------------------

// AMQPConfig - Configuration for the AMQP output type.
type AMQPConfig struct {
	URI          string `json:"uri" yaml:"uri"`
	Exchange     string `json:"exchange" yaml:"exchange"`
	ExchangeType string `json:"exchange_type" yaml:"exchange_type"`
	BindingKey   string `json:"key" yaml:"key"`
	// Reliable     bool   `json:"reliable" yaml:"reliable"`
}

// NewAMQPConfig - Creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URI:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "benthos-exchange",
		ExchangeType: "direct",
		BindingKey:   "benthos-key",
		// Reliable:     true,
	}
}

//--------------------------------------------------------------------------------------------------

// AMQP - An output type that serves AMQP messages.
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

// NewAMQP - Create a new AMQP output type.
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

	if err := a.connect(); err != nil {
		return nil, err
	}

	return &a, nil
}

//--------------------------------------------------------------------------------------------------

// connect - Establish a connection to an AMQP server.
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

	/*
		if a.conf.AMQP.Reliable {
			if err := a.amqpChan.Confirm(false); err != nil {
				return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
			}
			a.amqpConfirms = a.amqpChan.NotifyPublish(make(chan amqp.Confirmation, 1))
		}
	*/
	return
}

// disconnect - Safely close a connection to an AMQP server.
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

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe, does not use select.
func (a *AMQP) loop() {
	defer func() {
		atomic.StoreInt32(&a.running, 0)

		a.disconnect()

		close(a.responseChan)
		close(a.closedChan)
	}()

	var open bool
	for atomic.LoadInt32(&a.running) == 1 {
		var msg types.Message
		if msg, open = <-a.messages; !open {
			return
		}
		var err error
		var sending []byte
		var contentType string
		if len(msg.Parts) == 1 {
			sending = msg.Parts[0]
			contentType = "application/octet-stream"
		} else {
			sending = msg.Bytes()
			contentType = "application/x-benthos-multipart"
		}
		err = a.amqpChan.Publish(
			a.conf.AMQP.Exchange,   // publish to an exchange
			a.conf.AMQP.BindingKey, // routing to 0 or more queues
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     contentType,
				ContentEncoding: "",
				Body:            sending,
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
				// a bunch of application/implementation-specific fields
			},
		)
		select {
		case a.responseChan <- types.NewSimpleResponse(err):
		case <-a.closeChan:
			return
		}
	}
}

// StartReceiving - Assigns a messages channel for the output to read.
func (a *AMQP) StartReceiving(msgs <-chan types.Message) error {
	if a.messages != nil {
		return types.ErrAlreadyStarted
	}
	a.messages = msgs
	go a.loop()
	return nil
}

// ResponseChan - Returns the errors channel.
func (a *AMQP) ResponseChan() <-chan types.Response {
	return a.responseChan
}

// CloseAsync - Shuts down the AMQP output and stops processing messages.
func (a *AMQP) CloseAsync() {
	if atomic.CompareAndSwapInt32(&a.running, 1, 0) {
		close(a.closeChan)
	}
}

// WaitForClose - Blocks until the AMQP output has closed down.
func (a *AMQP) WaitForClose(timeout time.Duration) error {
	select {
	case <-a.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
