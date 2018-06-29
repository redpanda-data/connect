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
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

//------------------------------------------------------------------------------

// MQTTConfig is configuration for the MQTT input type.
type MQTTConfig struct {
	URLs     []string `json:"urls" yaml:"urls"`
	QoS      uint8    `json:"qos" yaml:"qos"`
	Topics   []string `json:"topics" yaml:"topics"`
	ClientID string   `json:"client_id" yaml:"client_id"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:     []string{"tcp://localhost:1883"},
		QoS:      1,
		Topics:   []string{"benthos_topic"},
		ClientID: "benthos_input",
	}
}

//------------------------------------------------------------------------------

// MQTT is an input type that reads MQTT Pub/Sub messages.
type MQTT struct {
	client mqtt.Client
	cMut   sync.Mutex

	conf MQTTConfig

	msgChan       chan mqtt.Message
	interruptChan chan struct{}

	urls []string

	stats metrics.Type
	log   log.Modular
}

// NewMQTT creates a new MQTT input type.
func NewMQTT(
	conf MQTTConfig, log log.Modular, stats metrics.Type,
) (*MQTT, error) {
	m := &MQTT{
		conf:          conf,
		msgChan:       make(chan mqtt.Message),
		interruptChan: make(chan struct{}),
		stats:         stats,
		log:           log.NewModule(".input.mqtt"),
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if len(splitURL) > 0 {
				m.urls = append(m.urls, splitURL)
			}
		}
	}

	return m, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an MQTT server.
func (m *MQTT) Connect() error {
	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		return nil
	}

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(true).
		SetClientID(m.conf.ClientID).
		SetOnConnectHandler(func(c mqtt.Client) {
			for _, topic := range m.conf.Topics {
				tok := c.Subscribe(topic, byte(m.conf.QoS), m.msgHandler)
				tok.Wait()
				if err := tok.Error(); err != nil {
					m.log.Errorf("Failed to subscribe to topic '%v': %v\n", topic, err)
				}
			}
		})

	for _, u := range m.urls {
		conf = conf.AddBroker(u)
	}

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	m.client = client
	return nil
}

func (m *MQTT) msgHandler(c mqtt.Client, msg mqtt.Message) {
	select {
	case m.msgChan <- msg:
	case <-m.interruptChan:
	}
}

// Read attempts to read a new message from an MQTT broker.
func (m *MQTT) Read() (types.Message, error) {
	select {
	case msg := <-m.msgChan:
		return types.NewMessage([][]byte{[]byte(msg.Payload())}), nil
	case <-m.interruptChan:
	}
	return nil, types.ErrTypeClosed
}

// Acknowledge instructs whether messages have been successfully propagated.
func (m *MQTT) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the MQTT input and stops processing requests.
func (m *MQTT) CloseAsync() {
	m.cMut.Lock()
	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
		close(m.interruptChan)
	}
	m.cMut.Unlock()
}

// WaitForClose blocks until the MQTT input has closed down.
func (m *MQTT) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
