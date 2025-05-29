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
	"fmt"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	moFieldTopic                = "topic"
	moFieldQoS                  = "qos"
	moFieldWriteTimeout         = "write_timeout"
	moFieldRetained             = "retained"
	moFieldRetainedInterpolated = "retained_interpolated"
)

func outputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Pushes messages to an MQTT broker.").
		Description(`
The `+"`topic`"+` field can be dynamically set using function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here]. When sending batched messages these interpolations are performed per message part.`+service.OutputPerformanceDocs(true, false)).
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(moFieldTopic).
				Description("The topic to publish messages to."),
			service.NewIntField(moFieldQoS).
				Description("The QoS value to set for each message. Has options 0, 1, 2.").
				Default(1),
			service.NewDurationField(moFieldWriteTimeout).
				Description("The maximum amount of time to wait to write data before the attempt is abandoned.").
				Examples("1s", "500ms").
				Default("3s").
				Version("3.58.0"),
			service.NewBoolField(moFieldRetained).
				Description("Set message as retained on the topic.").
				Default(false),
			service.NewInterpolatedStringField(moFieldRetainedInterpolated).
				Description("Override the value of `retained` with an interpolable value, this allows it to be dynamically set based on message contents. The value must resolve to either `true` or `false`.").
				Advanced().
				Optional().
				Version("3.59.0"),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	service.MustRegisterOutput("mqtt", outputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
		if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
			return
		}
		out, err = newMQTTWriterFromParsed(conf, mgr)
		return
	})
}

type mqttWriter struct {
	log *service.Logger

	clientBuilder clientOptsBuilder

	writeTimeout   time.Duration
	topic          *service.InterpolatedString
	retained       bool
	retainedInterp *service.InterpolatedString
	qos            uint8

	client  mqtt.Client
	connMut sync.RWMutex
}

func newMQTTWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*mqttWriter, error) {
	m := &mqttWriter{
		log: mgr.Logger(),
	}

	var err error
	if m.clientBuilder, err = clientOptsFromParsed(conf); err != nil {
		return nil, err
	}

	if m.writeTimeout, err = conf.FieldDuration(moFieldWriteTimeout); err != nil {
		return nil, err
	}
	if m.topic, err = conf.FieldInterpolatedString(moFieldTopic); err != nil {
		return nil, err
	}
	if m.retained, err = conf.FieldBool(moFieldRetained); err != nil {
		return nil, err
	}
	if iStrp, _ := conf.FieldString(moFieldRetainedInterpolated); iStrp != "" {
		if m.retainedInterp, err = conf.FieldInterpolatedString(moFieldRetainedInterpolated); err != nil {
			return nil, err
		}
	}
	var tmpQoS int
	if tmpQoS, err = conf.FieldInt(moFieldQoS); err != nil {
		return nil, err
	}
	m.qos = uint8(tmpQoS)
	return m, nil
}

func (m *mqttWriter) Connect(context.Context) error {
	m.connMut.Lock()
	defer m.connMut.Unlock()

	if m.client != nil {
		return nil
	}

	conf := m.clientBuilder.apply(mqtt.NewClientOptions()).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			m.log.Errorf("Connection lost due to: %v", reason)
		}).
		SetWriteTimeout(m.writeTimeout)

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	m.client = client
	return nil
}

func (m *mqttWriter) Write(_ context.Context, msg *service.Message) error {
	m.connMut.RLock()
	client := m.client
	m.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	retained := m.retained
	if m.retainedInterp != nil {
		retainedStr, parseErr := m.retainedInterp.TryString(msg)
		if parseErr != nil {
			m.log.Errorf("Retained interpolation error: %v", parseErr)
		} else if retained, parseErr = strconv.ParseBool(retainedStr); parseErr != nil {
			m.log.Errorf("Error parsing boolean value from retained flag: %v \n", parseErr)
		}
	}

	topicStr, err := m.topic.TryString(msg)
	if err != nil {
		return fmt.Errorf("topic interpolation error: %w", err)
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	mtok := client.Publish(topicStr, m.qos, retained, mBytes)
	mtok.Wait()
	sendErr := mtok.Error()
	if sendErr == mqtt.ErrNotConnected {
		m.connMut.RLock()
		m.client = nil
		m.connMut.RUnlock()
		sendErr = service.ErrNotConnected
	}
	return sendErr
}

func (m *mqttWriter) Close(context.Context) error {
	m.connMut.Lock()
	defer m.connMut.Unlock()

	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
	}
	return nil
}
