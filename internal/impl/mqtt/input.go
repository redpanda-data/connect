package mqtt

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	mqttconf "github.com/benthosdev/benthos/v4/internal/impl/mqtt/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		m, err := newMQTTReader(conf.MQTT, nm)
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("mqtt", input.NewAsyncPreserver(m), nm)
	}), docs.ComponentSpec{
		Name: "mqtt",
		Summary: `
Subscribe to topics on MQTT brokers.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- mqtt_duplicate
- mqtt_qos
- mqtt_retained
- mqtt_topic
- mqtt_message_id
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").Array(),
			docs.FieldString("topics", "A list of topics to consume from.").Array(),
			docs.FieldString("client_id", "An identifier for the client connection."),
			docs.FieldString("dynamic_client_id_suffix", "Append a dynamically generated suffix to the specified `client_id` on each run of the pipeline. This can be useful when clustering Benthos producers.").Optional().Advanced().HasAnnotatedOptions(
				"nanoid", "append a nanoid of length 21 characters",
			).LinterFunc(nil),
			docs.FieldInt("qos", "The level of delivery guarantee to enforce.").HasOptions("0", "1", "2").Advanced().LinterFunc(nil),
			docs.FieldBool("clean_session", "Set whether the connection is non-persistent.").Advanced(),
			mqttconf.WillFieldSpec(),
			docs.FieldString("connect_timeout", "The maximum amount of time to wait in order to establish a connection before the attempt is abandoned.", "1s", "500ms").HasDefault("30s").AtVersion("3.58.0"),
			docs.FieldString("user", "A username to assume for the connection.").Advanced(),
			docs.FieldString("password", "A password to provide for the connection.").Advanced().Secret(),
			docs.FieldInt("keepalive", "Max seconds of inactivity before a keepalive message is sent.").Advanced(),
			tls.FieldSpec().AtVersion("3.45.0"),
		).ChildDefaultAndTypesFromStruct(input.NewMQTTConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

type mqttReader struct {
	client  mqtt.Client
	msgChan chan mqtt.Message
	cMut    sync.Mutex

	connectTimeout time.Duration
	conf           input.MQTTConfig

	interruptChan chan struct{}

	urls []string

	log log.Modular
	mgr bundle.NewManagement
}

func newMQTTReader(conf input.MQTTConfig, mgr bundle.NewManagement) (*mqttReader, error) {
	m := &mqttReader{
		conf:          conf,
		interruptChan: make(chan struct{}),
		log:           mgr.Logger(),
		mgr:           mgr,
	}

	var err error
	if m.connectTimeout, err = time.ParseDuration(conf.ConnectTimeout); err != nil {
		return nil, fmt.Errorf("unable to parse connect timeout duration string: %w", err)
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

func (m *mqttReader) Connect(ctx context.Context) error {
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

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(false).
		SetClientID(m.conf.ClientID).
		SetCleanSession(m.conf.CleanSession).
		SetConnectTimeout(m.connectTimeout).
		SetKeepAlive(time.Duration(m.conf.KeepAlive) * time.Second).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
			client.Disconnect(0)
			closeMsgChan()
			m.log.Errorf("Connection lost due to: %v\n", reason)
		}).
		SetOnConnectHandler(func(c mqtt.Client) {
			topics := make(map[string]byte)
			for _, topic := range m.conf.Topics {
				topics[topic] = m.conf.QoS
			}

			tok := c.SubscribeMultiple(topics, func(c mqtt.Client, msg mqtt.Message) {
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
				m.log.Errorf("Failed to subscribe to topics '%v': %v\n", m.conf.Topics, err)
				m.log.Errorln("Shutting connection down.")
				closeMsgChan()
			}
		})

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

	for _, u := range m.urls {
		conf = conf.AddBroker(u)
	}

	client := mqtt.NewClient(conf)

	tok := client.Connect()
	tok.Wait()
	if err := tok.Error(); err != nil {
		return err
	}

	m.log.Infof("Receiving MQTT messages from topics: %v\n", m.conf.Topics)
	go func() {
		for {
			select {
			case <-time.After(time.Second):
				if !client.IsConnected() {
					if closeMsgChan() {
						m.log.Errorln("Connection lost for unknown reasons.")
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

func (m *mqttReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	m.cMut.Lock()
	msgChan := m.msgChan
	m.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, component.ErrNotConnected
	}

	select {
	case msg, open := <-msgChan:
		if !open {
			m.cMut.Lock()
			m.msgChan = nil
			m.client = nil
			m.cMut.Unlock()
			return nil, nil, component.ErrNotConnected
		}

		message := message.QuickBatch([][]byte{msg.Payload()})

		p := message.Get(0)
		p.MetaSetMut("mqtt_duplicate", msg.Duplicate())
		p.MetaSetMut("mqtt_qos", int(msg.Qos()))
		p.MetaSetMut("mqtt_retained", msg.Retained())
		p.MetaSetMut("mqtt_topic", msg.Topic())
		p.MetaSetMut("mqtt_message_id", int(msg.MessageID()))

		return message, func(ctx context.Context, res error) error {
			if res == nil {
				msg.Ack()
			}
			return nil
		}, nil
	case <-ctx.Done():
	case <-m.interruptChan:
		return nil, nil, component.ErrTypeClosed
	}
	return nil, nil, component.ErrTimeout
}

func (m *mqttReader) Close(ctx context.Context) (err error) {
	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		m.client.Disconnect(0)
		m.client = nil
		close(m.interruptChan)
	}
	return
}
