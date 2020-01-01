package reader

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

//------------------------------------------------------------------------------

// MQTTConfig contains configuration fields for the MQTT input type.
type MQTTConfig struct {
	URLs         []string `json:"urls" yaml:"urls"`
	QoS          uint8    `json:"qos" yaml:"qos"`
	Topics       []string `json:"topics" yaml:"topics"`
	ClientID     string   `json:"client_id" yaml:"client_id"`
	CleanSession bool     `json:"clean_session" yaml:"clean_session"`
	User         string   `json:"user" yaml:"user"`
	Password     string   `json:"password" yaml:"password"`
}

// NewMQTTConfig creates a new MQTTConfig with default values.
func NewMQTTConfig() MQTTConfig {
	return MQTTConfig{
		URLs:         []string{"tcp://localhost:1883"},
		QoS:          1,
		Topics:       []string{"benthos_topic"},
		ClientID:     "benthos_input",
		CleanSession: true,
		User:         "",
		Password:     "",
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
		log:           log,
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
	return m.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to an MQTT server.
func (m *MQTT) ConnectWithContext(ctx context.Context) error {
	m.cMut.Lock()
	defer m.cMut.Unlock()

	if m.client != nil {
		return nil
	}

	conf := mqtt.NewClientOptions().
		SetAutoReconnect(true).
		SetClientID(m.conf.ClientID).
		SetCleanSession(m.conf.CleanSession).
		SetOnConnectHandler(func(c mqtt.Client) {
			for _, topic := range m.conf.Topics {
				tok := c.Subscribe(topic, byte(m.conf.QoS), m.msgHandler)
				tok.Wait()
				if err := tok.Error(); err != nil {
					m.log.Errorf("Failed to subscribe to topic '%v': %v\n", topic, err)
				}
			}
		})

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

	m.client = client
	return nil
}

func (m *MQTT) msgHandler(c mqtt.Client, msg mqtt.Message) {
	select {
	case m.msgChan <- msg:
	case <-m.interruptChan:
	}
}

// ReadWithContext attempts to read a new message from an MQTT broker.
func (m *MQTT) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	select {
	case msg := <-m.msgChan:
		message := message.New([][]byte{[]byte(msg.Payload())})

		meta := message.Get(0).Metadata()
		meta.Set("mqtt_duplicate", strconv.FormatBool(bool(msg.Duplicate())))
		meta.Set("mqtt_qos", strconv.Itoa(int(msg.Qos())))
		meta.Set("mqtt_retained", strconv.FormatBool(bool(msg.Retained())))
		meta.Set("mqtt_topic", string(msg.Topic()))
		meta.Set("mqtt_message_id", strconv.Itoa(int(msg.MessageID())))

		return message, noopAsyncAckFn, nil
	case <-ctx.Done():
	case <-m.interruptChan:
		return nil, nil, types.ErrTypeClosed
	}
	return nil, nil, types.ErrTimeout
}

// Read attempts to read a new message from an MQTT broker.
func (m *MQTT) Read() (types.Message, error) {
	msg, _, err := m.ReadWithContext(context.Background())
	return msg, err
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
