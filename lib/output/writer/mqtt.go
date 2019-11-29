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
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

//------------------------------------------------------------------------------

// MQTTConfig contains configuration fields for the MQTT output type.
type MQTTConfig struct {
	URLs     []string `json:"urls" yaml:"urls"`
	QoS      uint8    `json:"qos" yaml:"qos"`
	Topic    string   `json:"topic" yaml:"topic"`
	ClientID string   `json:"client_id" yaml:"client_id"`
	User     string   `json:"user" yaml:"user"`
	Password string   `json:"password" yaml:"password"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:     []string{"tcp://localhost:1883"},
		QoS:      1,
		Topic:    "benthos_topic",
		ClientID: "benthos_output",
		User:     "",
		Password: "",
	}
}

//------------------------------------------------------------------------------

// MQTT is an output type that serves MQTT messages.
type MQTT struct {
	log   log.Modular
	stats metrics.Type

	urls  []string
	conf  MQTTConfig
	topic *text.InterpolatedString

	client  mqtt.Client
	connMut sync.RWMutex
}

// NewMQTT creates a new MQTT output type.
func NewMQTT(
	conf MQTTConfig,
	log log.Modular,
	stats metrics.Type,
) (*MQTT, error) {
	m := &MQTT{
		log:   log,
		stats: stats,
		conf:  conf,

		topic: text.NewInterpolatedString(conf.Topic),
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
	m.connMut.Lock()
	defer m.connMut.Unlock()

	if m.client != nil {
		return nil
	}

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(true).
		SetConnectTimeout(time.Second).
		SetWriteTimeout(time.Second).
		SetClientID(m.conf.ClientID)

	for _, u := range m.urls {
		conf = conf.AddBroker(u)
	}

	if m.conf.User != "" {
		conf.SetUsername(m.conf.User)
	}

	if m.conf.Password != "" {
		conf.SetPassword(m.conf.Password)
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

//------------------------------------------------------------------------------

// Write attempts to write a message by pushing it to an MQTT broker.
func (m *MQTT) Write(msg types.Message) error {
	m.connMut.RLock()
	client := m.client
	m.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		lMsg := message.Lock(msg, i)
		mtok := client.Publish(m.topic.Get(lMsg), byte(m.conf.QoS), false, p.Get())
		mtok.Wait()
		return mtok.Error()
	})
}

// CloseAsync shuts down the MQTT output and stops processing messages.
func (m *MQTT) CloseAsync() {
	m.connMut.Lock()
	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
	}
	m.connMut.Unlock()
}

// WaitForClose blocks until the MQTT output has closed down.
func (m *MQTT) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
