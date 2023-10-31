package mqtt5

import (
	"context"
	"fmt"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"net/url"
	"strings"
	"sync"
	"time"

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

type MqttLogger struct {
	log log.Modular
}

func (l MqttLogger) Println(v ...interface{}) {
	l.log.Infof("%v\n", v...)
}

func (l MqttLogger) Printf(format string, v ...interface{}) {
	l.log.Infof(format, v...)
}

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		m, err := newMQTTReader(conf.MQTTv5, nm)
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("mqtt5", input.NewAsyncPreserver(m), nm)
	}), docs.ComponentSpec{
		Name: "mqtt5",
		Summary: `
Subscribe to topics on MQTT brokers, via protocol version 5.`,
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
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
	client  *autopaho.ConnectionManager
	msgChan chan paho.Publish
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

	// if urls is empty, panic
	if len(conf.URLs) == 0 {
		panic("no configured broker URLs (MQTTv5 adapter)")
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
	// will run until canceled
	//ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	//defer stop()

	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		return nil
	}

	var msgMut sync.Mutex
	msgChan := make(chan paho.Publish)

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

	var brokers []*url.URL

	// if m.urls is empty, panic
	if len(m.urls) == 0 {
		panic("no broker URLs (MQTTv5 adapter)")
	}

	for _, u := range m.urls {
		parsedUrl, err := url.Parse(u)
		if err != nil {
			panic("invalid broker url")
		}
		brokers = append(brokers, parsedUrl)
	}

	var active = true
	processMsg := func(msg *paho.Publish, topic string) {
		msgMut.Lock()
		if msgChan != nil {
			select {
			case msgChan <- *msg:
			case <-m.interruptChan:
			}
		}
		msgMut.Unlock()
	}

	router := paho.NewSingleHandlerRouter(func(msg *paho.Publish) {
		m.log.Infof("Received MQTTv5 message via default handler")
		processMsg(msg, "_unknown_")
	})

	for _, topic := range m.conf.Topics {
		router.RegisterHandler(topic, func(msg *paho.Publish) {
			m.log.Infof("Received MQTTv5 message on topic '%s'", topic)
			processMsg(msg, topic)
		})
	}
	infoLogger := MqttLogger{log: m.log}
	errLogger := MqttLogger{log: m.log}

	conf := autopaho.ClientConfig{
		BrokerUrls: brokers,
		Debug:      infoLogger,
		PahoDebug:  infoLogger,
		PahoErrors: errLogger,
		KeepAlive:  uint16(time.Duration(m.conf.KeepAlive) * time.Second),
		OnConnectionUp: func(mgr *autopaho.ConnectionManager, connack *paho.Connack) {
			m.log.Infof("MQTTv5 broker connection active")
			topics := make(map[string]byte)

			// if there are no topics, there are no subscriptions to set
			if len(m.conf.Topics) == 0 {
				m.log.Infof("No MQTT subscription topics")
				return
			}
			for _, topic := range m.conf.Topics {
				topics[topic] = m.conf.QoS
			}

			var subscriptions []paho.SubscribeOptions
			for topic, qos := range topics {
				subscriptions = append(subscriptions, paho.SubscribeOptions{
					Topic: topic,
					QoS:   qos,
				})
			}

			if len(topics) > 0 {
				if ack, err := mgr.Subscribe(context.Background(), &paho.Subscribe{
					Subscriptions: subscriptions,
				}); err != nil {
					m.log.Errorf("Failed to subscribe over MQTTv5 (%s, %v)", err, ack.Reasons)
				}
			}
			m.log.Infof("MQTTv5 subscriptions active (%d)\n", len(topics))
		},
		OnConnectError: func(err error) {
			m.log.Errorf("Failed to connect to MQTTv5 broker: %s", err)
		},
		ClientConfig: paho.ClientConfig{
			ClientID: m.conf.ClientID,
			Router:   router,
			OnClientError: func(err error) {
				m.log.Errorf("MQTTv5 client error: %s", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				active = false
				closeMsgChan()

				if d.Properties != nil {
					m.log.Errorf("Connection lost due to: %s\n", d.Properties.ReasonString)
				} else {
					m.log.Errorf("Connection lost due to reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	if m.conf.Will.Enabled {
		conf.SetWillMessage(
			m.conf.Will.Topic,
			[]byte(m.conf.Will.Payload),
			m.conf.Will.QoS,
			m.conf.Will.Retained,
		)
	}

	if m.conf.TLS.Enabled {
		m.log.Infof("Using MQTTv5 over TLS")
		tlsConf, err := m.conf.TLS.Get(m.mgr.FS())
		if err != nil {
			return err
		}
		conf.TlsCfg = tlsConf
	}

	if m.conf.User != "" && m.conf.Password != "" {
		conf.SetUsernamePassword(
			m.conf.User,
			[]byte(m.conf.Password),
		)
	}

	// connect to the broker
	conn, err := autopaho.NewConnection(ctx, conf)
	if err != nil {
		return err
	}

	// await established connection
	m.log.Infof("Connecting to MQTTv5 broker")
	connErr := conn.AwaitConnection(ctx)
	if connErr != nil {
		panic("Failed to connect to MQTTv5 broker")
	}

	//m.log.Infof("MQTTv5 broker connection active")
	//topics := make(map[string]byte)
	//for _, topic := range m.conf.Topics {
	//	topics[topic] = m.conf.QoS
	//}
	//
	//var subscriptions []paho.SubscribeOptions
	//for topic, qos := range topics {
	//	subscriptions = append(subscriptions, paho.SubscribeOptions{
	//		Topic: topic,
	//		QoS:   qos,
	//	})
	//}
	//
	//if _, err := conn.Subscribe(context.Background(), &paho.Subscribe{
	//	Subscriptions: subscriptions,
	//}); err != nil {
	//	fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
	//}

	m.log.Infof("Receiving MQTTv5 messages from topics: %v\n", m.conf.Topics)
	go func() {
		for {
			select {
			case <-time.After(time.Second):
				if !active {
					if closeMsgChan() {
						m.log.Errorln("Connection lost for unknown reasons.")
					}
					return
				}
			case <-conn.Done():
			case <-m.interruptChan:
				return
			}
		}
	}()
	m.client = conn
	m.msgChan = msgChan
	return nil
}

func (m *mqttReader) Ack(msg *paho.Publish) {
	// nothing at this time @TODO
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

		inner := message.QuickBatch([][]byte{msg.Payload})

		p := inner.Get(0)
		p.MetaSetMut("mqtt_qos", int(msg.QoS))
		p.MetaSetMut("mqtt_retained", msg.Retain)
		p.MetaSetMut("mqtt_topic", msg.Topic)
		p.MetaSetMut("mqtt_message_id", int(msg.PacketID))

		return inner, func(ctx context.Context, res error) error {
			if res == nil {
				m.Ack(&msg)
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
		err := m.client.Disconnect(ctx)
		if err != nil {
			return err
		}
		m.client = nil
		close(m.interruptChan)
	}
	return
}
