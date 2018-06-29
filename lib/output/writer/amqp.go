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

package writer

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/streadway/amqp"
)

//------------------------------------------------------------------------------

// AMQPConfig is configuration for the AMQP output type.
type AMQPConfig struct {
	URL          string `json:"url" yaml:"url"`
	Exchange     string `json:"exchange" yaml:"exchange"`
	ExchangeType string `json:"exchange_type" yaml:"exchange_type"`
	BindingKey   string `json:"key" yaml:"key"`
	Persistent   bool   `json:"persistent" yaml:"persistent"`
	Mandatory    bool   `json:"mandatory" yaml:"mandatory"`
	Immediate    bool   `json:"immediate" yaml:"immediate"`
}

// NewAMQPConfig creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URL:          "amqp://guest:guest@localhost:5672/",
		Exchange:     "benthos-exchange",
		ExchangeType: "direct",
		BindingKey:   "benthos-key",
		Persistent:   false,
		Mandatory:    false,
		Immediate:    false,
	}
}

//------------------------------------------------------------------------------

// AMQP is an output type that serves AMQP messages.
type AMQP struct {
	log   log.Modular
	stats metrics.Type

	conf AMQPConfig

	conn        *amqp.Connection
	amqpChan    *amqp.Channel
	confirmChan <-chan amqp.Confirmation
	returnChan  <-chan amqp.Return

	deliveryMode uint8

	connLock sync.RWMutex
}

// NewAMQP creates a new AMQP writer type.
func NewAMQP(conf AMQPConfig, log log.Modular, stats metrics.Type) (*AMQP, error) {
	a := AMQP{
		log:          log.NewModule(".output.amqp"),
		stats:        stats,
		conf:         conf,
		deliveryMode: amqp.Transient,
	}
	if conf.Persistent {
		a.deliveryMode = amqp.Persistent
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an AMQP server.
func (a *AMQP) Connect() error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	conn, err := amqp.Dial(a.conf.URL)
	if err != nil {
		return fmt.Errorf("amqp failed to connect: %v", err)
	}

	var amqpChan *amqp.Channel
	if amqpChan, err = conn.Channel(); err != nil {
		conn.Close()
		return fmt.Errorf("amqp failed to create channel: %v", err)
	}

	if err = amqpChan.ExchangeDeclare(
		a.conf.Exchange,     // name of the exchange
		a.conf.ExchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		conn.Close()
		return fmt.Errorf("amqp failed to declare exchange: %v", err)
	}

	if err = amqpChan.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel could not be put into confirm mode: %v", err)
	}

	a.conn = conn
	a.amqpChan = amqpChan
	a.confirmChan = amqpChan.NotifyPublish(make(chan amqp.Confirmation, 1))
	if a.conf.Mandatory || a.conf.Immediate {
		a.returnChan = amqpChan.NotifyReturn(make(chan amqp.Return, 1))
	}

	a.log.Infof("Sending AMQP messages to URL: %s\n", a.conf.URL)
	return nil
}

// disconnect safely closes a connection to an AMQP server.
func (a *AMQP) disconnect() error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

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

// Write will attempt to write a message over AMQP, wait for acknowledgement,
// and returns an error if applicable.
func (a *AMQP) Write(msg types.Message) error {
	a.connLock.RLock()
	conn := a.conn
	amqpChan := a.amqpChan
	confirmChan := a.confirmChan
	returnChan := a.returnChan
	a.connLock.RUnlock()

	if conn == nil {
		return types.ErrNotConnected
	}

	for _, part := range msg.GetAll() {
		err := amqpChan.Publish(
			a.conf.Exchange,   // publish to an exchange
			a.conf.BindingKey, // routing to 0 or more queues
			a.conf.Mandatory,  // mandatory
			a.conf.Immediate,  // immediate
			amqp.Publishing{
				Headers:         amqp.Table{},
				ContentType:     "application/octet-stream",
				ContentEncoding: "",
				Body:            part,
				DeliveryMode:    a.deliveryMode, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
				// a bunch of application/implementation-specific fields
			},
		)
		if err != nil {
			a.disconnect()
			return types.ErrNotConnected
		}
		select {
		case confirm, open := <-confirmChan:
			if !open {
				return types.ErrNotConnected
			}
			if !confirm.Ack {
				return types.ErrNoAck
			}
		case _, open := <-returnChan:
			if !open {
				return fmt.Errorf("acknowledgement not supported, ensure server supports immediate and mandatory flags")
			}
			return types.ErrNoAck
		}
	}

	return nil
}

// CloseAsync shuts down the AMQP output and stops processing messages.
func (a *AMQP) CloseAsync() {
	a.disconnect()
}

// WaitForClose blocks until the AMQP output has closed down.
func (a *AMQP) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
