// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mqtt

import (
	"context"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	miFieldTopics       = "topics"
	miFieldQoS          = "qos"
	miFieldCleanSession = "clean_session"
)

func inputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Subscribe to topics on MQTT brokers.").
		Description(`
== Metadata

This input adds the following metadata fields to each message:

- mqtt_duplicate
- mqtt_qos
- mqtt_retained
- mqtt_topic
- mqtt_message_id

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(clientFields()...).
		Fields(
			service.NewStringListField(miFieldTopics).
				Description("A list of topics to consume from."),
			service.NewIntField(miFieldQoS).
				Description("The level of delivery guarantee to enforce. Has options 0, 1, 2.").
				Advanced().
				Default(1),
			service.NewBoolField(miFieldCleanSession).
				Description("Set whether the connection is non-persistent.").
				Default(true).
				Advanced(),
			service.NewAutoRetryNacksToggleField(),
		)
}

func init() {
	service.MustRegisterInput("mqtt", inputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		rdr, err := newMQTTReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksToggled(conf, rdr)
	})
}

type mqttReader struct {
	clientBuilder clientOptsBuilder
	topics        []string
	qos           uint8
	cleanSession  bool

	client  mqtt.Client
	msgChan chan mqtt.Message
	cMut    sync.Mutex

	interruptChan chan struct{}

	log *service.Logger
}

func newMQTTReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*mqttReader, error) {
	m := &mqttReader{
		interruptChan: make(chan struct{}),
		log:           mgr.Logger(),
	}

	var err error
	if m.clientBuilder, err = clientOptsFromParsed(conf); err != nil {
		return nil, err
	}

	if m.topics, err = conf.FieldStringList(miFieldTopics); err != nil {
		return nil, err
	}
	var tmpQoS int
	if tmpQoS, err = conf.FieldInt(miFieldQoS); err != nil {
		return nil, err
	}
	m.qos = uint8(tmpQoS)
	if m.cleanSession, err = conf.FieldBool(miFieldCleanSession); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *mqttReader) Connect(context.Context) error {
	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		return nil
	}

	var msgMut sync.Mutex
	msgChan := make(chan mqtt.Message)

	closeMsgChan := func() bool {
		msgMut.Lock()
		chanOpen := msgChan != nil
		if chanOpen {
			close(msgChan)
			msgChan = nil
		}
		msgMut.Unlock()
		return chanOpen
	}

	conf := m.clientBuilder.apply(mqtt.NewClientOptions()).
		SetCleanSession(m.cleanSession).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			closeMsgChan()
			m.log.Errorf("Connection lost due to: %v\n", reason)
		}).
		SetOnConnectHandler(func(c mqtt.Client) {
			topics := make(map[string]byte)
			for _, topic := range m.topics {
				topics[topic] = m.qos
			}

			tok := c.SubscribeMultiple(topics, func(_ mqtt.Client, msg mqtt.Message) {
				msgMut.Lock()
				if msgChan != nil {
					select {
					case msgChan <- msg:
					case <-m.interruptChan:
					}
				}
				msgMut.Unlock()
			})
			tok.Wait()
			if err := tok.Error(); err != nil {
				m.log.Errorf("Failed to subscribe to topics '%v': %v", m.topics, err)
				m.log.Error("Shutting connection down.")
				closeMsgChan()
			}
		})

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-time.After(time.Second):
				if !client.IsConnected() {
					if closeMsgChan() {
						m.log.Error("Connection lost for unknown reasons.")
					}
					return
				}
			case <-m.interruptChan:
				return
			}
		}
	}()

	m.client = client
	m.msgChan = msgChan
	return nil
}

func (m *mqttReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	m.cMut.Lock()
	msgChan := m.msgChan
	m.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case msg, open := <-msgChan:
		if !open {
			m.cMut.Lock()
			m.msgChan = nil
			m.client = nil
			m.cMut.Unlock()
			return nil, nil, service.ErrNotConnected
		}

		message := service.NewMessage(msg.Payload())

		message.MetaSetMut("mqtt_duplicate", msg.Duplicate())
		message.MetaSetMut("mqtt_qos", int(msg.Qos()))
		message.MetaSetMut("mqtt_retained", msg.Retained())
		message.MetaSetMut("mqtt_topic", msg.Topic())
		message.MetaSetMut("mqtt_message_id", int(msg.MessageID()))

		return message, func(_ context.Context, res error) error {
			if res == nil {
				msg.Ack()
			}
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-m.interruptChan:
		return nil, nil, service.ErrEndOfInput
	}
}

func (m *mqttReader) Close(context.Context) (err error) {
	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
		close(m.interruptChan)
	}
	return
}
