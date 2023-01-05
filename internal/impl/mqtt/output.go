package mqtt

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	mqttconf "github.com/benthosdev/benthos/v4/internal/impl/mqtt/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		w, err := newMQTTWriter(conf.MQTT, nm)
		if err != nil {
			return nil, err
		}
		a, err := output.NewAsyncWriter("mqtt", conf.MQTT.MaxInFlight, w, nm)
		if err != nil {
			return nil, err
		}
		return output.OnlySinglePayloads(a), nil
	}), docs.ComponentSpec{
		Name: "mqtt",
		Summary: `
Pushes messages to an MQTT broker.`,
		Description: output.Description(true, false, `
The `+"`topic`"+` field can be dynamically set using function interpolations
described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched
messages these interpolations are performed per message part.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"tcp://localhost:1883"}).Array(),
			docs.FieldString("topic", "The topic to publish messages to."),
			docs.FieldString("client_id", "An identifier for the client connection."),
			docs.FieldString("dynamic_client_id_suffix", "Append a dynamically generated suffix to the specified `client_id` on each run of the pipeline. This can be useful when clustering Benthos producers.").Optional().Advanced().HasAnnotatedOptions(
				"nanoid", "append a nanoid of length 21 characters",
			).LinterFunc(nil),
			docs.FieldInt("qos", "The QoS value to set for each message.").HasOptions("0", "1", "2").LinterFunc(nil),
			docs.FieldString("connect_timeout", "The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.", "1s", "500ms").HasDefault("30s").AtVersion("3.58.0"),
			docs.FieldString("write_timeout", "The maximum amount of time to wait to write data before the attempt is abandoned.", "1s", "500ms").HasDefault("3s").AtVersion("3.58.0"),
			docs.FieldBool("retained", "Set message as retained on the topic."),
			docs.FieldString("retained_interpolated", "Override the value of `retained` with an interpolable value, this allows it to be dynamically set based on message contents. The value must resolve to either `true` or `false`.").IsInterpolated().Advanced().AtVersion("3.59.0"),
			mqttconf.WillFieldSpec(),
			docs.FieldString("user", "A username to connect with.").Advanced(),
			docs.FieldString("password", "A password to connect with.").Advanced().Secret(),
			docs.FieldInt("keepalive", "Max seconds of inactivity before a keepalive message is sent.").Advanced(),
			tls.FieldSpec().AtVersion("3.45.0"),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).ChildDefaultAndTypesFromStruct(output.NewMQTTConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

type mqttWriter struct {
	log log.Modular
	mgr bundle.NewManagement

	connectTimeout time.Duration
	writeTimeout   time.Duration

	urls     []string
	conf     output.MQTTConfig
	topic    *field.Expression
	retained *field.Expression

	client  mqtt.Client
	connMut sync.RWMutex
}

func newMQTTWriter(conf output.MQTTConfig, mgr bundle.NewManagement) (*mqttWriter, error) {
	m := &mqttWriter{
		log:  mgr.Logger(),
		mgr:  mgr,
		conf: conf,
	}

	var err error
	if m.connectTimeout, err = time.ParseDuration(conf.ConnectTimeout); err != nil {
		return nil, fmt.Errorf("unable to parse connect timeout duration string: %w", err)
	}
	if m.writeTimeout, err = time.ParseDuration(conf.WriteTimeout); err != nil {
		return nil, fmt.Errorf("unable to parse write timeout duration string: %w", err)
	}

	if m.topic, err = mgr.BloblEnvironment().NewField(conf.Topic); err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}

	if conf.RetainedInterpolated != "" {
		if m.retained, err = mgr.BloblEnvironment().NewField(conf.RetainedInterpolated); err != nil {
			return nil, fmt.Errorf("failed to parse retained expression: %v", err)
		}
	}

	switch m.conf.DynamicClientIDSuffix {
	case "nanoid":
		nid, err := gonanoid.New()
		if err != nil {
			return nil, fmt.Errorf("failed to generate nanoid: %w", err)
		}
		m.conf.ClientID += nid
	case "":
	default:
		return nil, fmt.Errorf("unknown dynamic_client_id_suffix: %v", m.conf.DynamicClientIDSuffix)
	}

	if err := m.conf.Will.Validate(); err != nil {
		return nil, err
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

func (m *mqttWriter) Connect(ctx context.Context) error {
	m.connMut.Lock()
	defer m.connMut.Unlock()

	if m.client != nil {
		return nil
	}

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(false).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			m.log.Errorf("Connection lost due to: %v\n", reason)
		}).
		SetConnectTimeout(m.connectTimeout).
		SetWriteTimeout(m.writeTimeout).
		SetKeepAlive(time.Duration(m.conf.KeepAlive) * time.Second).
		SetClientID(m.conf.ClientID)

	for _, u := range m.urls {
		conf = conf.AddBroker(u)
	}

	if m.conf.Will.Enabled {
		conf = conf.SetWill(m.conf.Will.Topic, m.conf.Will.Payload, m.conf.Will.QoS, m.conf.Will.Retained)
	}

	if m.conf.TLS.Enabled {
		tlsConf, err := m.conf.TLS.Get(m.mgr.FS())
		if err != nil {
			return err
		}
		conf.SetTLSConfig(tlsConf)
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

func (m *mqttWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	m.connMut.RLock()
	client := m.client
	m.connMut.RUnlock()

	if client == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		retained := m.conf.Retained
		if m.retained != nil {
			retainedStr, parseErr := m.retained.String(i, msg)
			if parseErr != nil {
				m.log.Errorf("Retained interpolation error: %v", parseErr)
			} else if retained, parseErr = strconv.ParseBool(retainedStr); parseErr != nil {
				m.log.Errorf("Error parsing boolean value from retained flag: %v \n", parseErr)
			}
		}

		topicStr, err := m.topic.String(i, msg)
		if err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}

		mtok := client.Publish(topicStr, m.conf.QoS, retained, p.AsBytes())
		mtok.Wait()
		sendErr := mtok.Error()
		if sendErr == mqtt.ErrNotConnected {
			m.connMut.RLock()
			m.client = nil
			m.connMut.RUnlock()
			sendErr = component.ErrNotConnected
		}
		return sendErr
	})
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
