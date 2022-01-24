package writer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	amqp "github.com/rabbitmq/amqp091-go"
)

var errAMQP09Connect = errors.New("AMQP 0.9 Connect")

//------------------------------------------------------------------------------

// AMQPExchangeDeclareConfig contains fields indicating whether the target AMQP
// exchange needs to be declared, as well as any fields specifying how to
// accomplish that.
type AMQPExchangeDeclareConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Type    string `json:"type" yaml:"type"`
	Durable bool   `json:"durable" yaml:"durable"`
}

// AMQPConfig contains configuration fields for the AMQP output type.
type AMQPConfig struct {
	URLs            []string                     `json:"urls" yaml:"urls"`
	MaxInFlight     int                          `json:"max_in_flight" yaml:"max_in_flight"`
	Exchange        string                       `json:"exchange" yaml:"exchange"`
	ExchangeDeclare AMQPExchangeDeclareConfig    `json:"exchange_declare" yaml:"exchange_declare"`
	BindingKey      string                       `json:"key" yaml:"key"`
	Type            string                       `json:"type" yaml:"type"`
	ContentType     string                       `json:"content_type" yaml:"content_type"`
	ContentEncoding string                       `json:"content_encoding" yaml:"content_encoding"`
	Metadata        metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	Priority        string                       `json:"priority" yaml:"priority"`
	Persistent      bool                         `json:"persistent" yaml:"persistent"`
	Mandatory       bool                         `json:"mandatory" yaml:"mandatory"`
	Immediate       bool                         `json:"immediate" yaml:"immediate"`
	TLS             btls.Config                  `json:"tls" yaml:"tls"`
}

// NewAMQPConfig creates a new AMQPConfig with default values.
func NewAMQPConfig() AMQPConfig {
	return AMQPConfig{
		URLs:        []string{},
		MaxInFlight: 1,
		Exchange:    "benthos-exchange",
		ExchangeDeclare: AMQPExchangeDeclareConfig{
			Enabled: false,
			Type:    "direct",
			Durable: true,
		},
		BindingKey:      "benthos-key",
		Type:            "",
		ContentType:     "application/octet-stream",
		ContentEncoding: "",
		Metadata:        metadata.NewExcludeFilterConfig(),
		Priority:        "",
		Persistent:      false,
		Mandatory:       false,
		Immediate:       false,
		TLS:             btls.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// AMQP is an output type that serves AMQP messages.
type AMQP struct {
	key             *field.Expression
	msgType         *field.Expression
	contentType     *field.Expression
	contentEncoding *field.Expression
	priority        *field.Expression
	metaFilter      *metadata.ExcludeFilter

	log   log.Modular
	stats metrics.Type

	conf    AMQPConfig
	urls    []string
	tlsConf *tls.Config

	conn        *amqp.Connection
	amqpChan    *amqp.Channel
	confirmChan <-chan amqp.Confirmation
	returnChan  <-chan amqp.Return

	deliveryMode uint8

	connLock sync.RWMutex
}

// NewAMQPV2 creates a new AMQP writer type.
func NewAMQPV2(mgr types.Manager, conf AMQPConfig, log log.Modular, stats metrics.Type) (*AMQP, error) {
	a := AMQP{
		log:          log,
		stats:        stats,
		conf:         conf,
		deliveryMode: amqp.Transient,
	}
	var err error
	if a.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	if a.key, err = interop.NewBloblangField(mgr, conf.BindingKey); err != nil {
		return nil, fmt.Errorf("failed to parse binding key expression: %v", err)
	}
	if a.msgType, err = interop.NewBloblangField(mgr, conf.Type); err != nil {
		return nil, fmt.Errorf("failed to parse type property expression: %v", err)
	}
	if a.contentType, err = interop.NewBloblangField(mgr, conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content_type property expression: %v", err)
	}
	if a.contentEncoding, err = interop.NewBloblangField(mgr, conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content_encoding property expression: %v", err)
	}
	if a.priority, err = interop.NewBloblangField(mgr, conf.Priority); err != nil {
		return nil, fmt.Errorf("failed to parse priority property expression: %w", err)
	}
	if conf.Persistent {
		a.deliveryMode = amqp.Persistent
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if trimmed := strings.TrimSpace(splitURL); len(trimmed) > 0 {
				a.urls = append(a.urls, trimmed)
			}
		}
	}
	if len(a.urls) == 0 {
		return nil, errors.New("must specify at least one url")
	}

	if conf.TLS.Enabled {
		if a.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an AMQP server.
func (a *AMQP) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect establishes a connection to an AMQP server.
func (a *AMQP) Connect() error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	conn, err := a.reDial(a.urls)
	if err != nil {
		return err
	}

	var amqpChan *amqp.Channel
	if amqpChan, err = conn.Channel(); err != nil {
		conn.Close()
		return fmt.Errorf("amqp failed to create channel: %v", err)
	}

	if a.conf.ExchangeDeclare.Enabled {
		if err = amqpChan.ExchangeDeclare(
			a.conf.Exchange,                // name of the exchange
			a.conf.ExchangeDeclare.Type,    // type
			a.conf.ExchangeDeclare.Durable, // durable
			false,                          // delete when complete
			false,                          // internal
			false,                          // noWait
			nil,                            // arguments
		); err != nil {
			conn.Close()
			return fmt.Errorf("amqp failed to declare exchange: %v", err)
		}
	}

	if err = amqpChan.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel could not be put into confirm mode: %v", err)
	}

	a.conn = conn
	a.amqpChan = amqpChan
	a.confirmChan = amqpChan.NotifyPublish(make(chan amqp.Confirmation, a.conf.MaxInFlight))
	if a.conf.Mandatory || a.conf.Immediate {
		a.returnChan = amqpChan.NotifyReturn(make(chan amqp.Return, 1))
	}

	a.log.Infof("Sending AMQP messages to exchange: %v\n", a.conf.Exchange)
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
			a.log.Errorf("Failed to close connection cleanly: %v\n", err)
		}
		a.conn = nil
	}
	return nil
}

//------------------------------------------------------------------------------

// WriteWithContext will attempt to write a message over AMQP, wait for
// acknowledgement, and returns an error if applicable.
func (a *AMQP) WriteWithContext(ctx context.Context, msg types.Message) error {
	return a.Write(msg)
}

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

	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		bindingKey := strings.ReplaceAll(a.key.String(i, msg), "/", ".")
		msgType := strings.ReplaceAll(a.msgType.String(i, msg), "/", ".")
		contentType := a.contentType.String(i, msg)
		contentEncoding := a.contentEncoding.String(i, msg)

		var priority uint8
		if priorityString := a.priority.String(i, msg); priorityString != "" {
			priorityInt, err := strconv.Atoi(priorityString)
			if err != nil {
				return fmt.Errorf("failed to parse valid integer from priority expression: %w", err)
			}
			if priorityInt > 9 || priorityInt < 0 {
				return fmt.Errorf("invalid priority parsed from expression, must be <= 9 and >= 0, got %v", priorityInt)
			}
			priority = uint8(priorityInt)
		}

		headers := amqp.Table{}
		a.metaFilter.Iter(p.Metadata(), func(k, v string) error {
			headers[strings.ReplaceAll(k, "_", "-")] = v
			return nil
		})

		err := amqpChan.Publish(
			a.conf.Exchange,  // publish to an exchange
			bindingKey,       // routing to 0 or more queues
			a.conf.Mandatory, // mandatory
			a.conf.Immediate, // immediate
			amqp.Publishing{
				Headers:         headers,
				ContentType:     contentType,
				ContentEncoding: contentEncoding,
				Body:            p.Get(),
				DeliveryMode:    a.deliveryMode, // 1=non-persistent, 2=persistent
				Priority:        priority,       // 0-9
				Type:            msgType,
				// a bunch of application/implementation-specific fields
			},
		)
		if err != nil {
			a.disconnect()
			a.log.Errorf("Failed to send message: %v\n", err)
			return types.ErrNotConnected
		}
		select {
		case confirm, open := <-confirmChan:
			if !open {
				a.log.Errorln("Failed to send message, ensure your target exchange exists.")
				return types.ErrNotConnected
			}
			if !confirm.Ack {
				a.log.Errorln("Failed to acknowledge message.")
				return types.ErrNoAck
			}
		case _, open := <-returnChan:
			if !open {
				return fmt.Errorf("acknowledgement not supported, ensure server supports immediate and mandatory flags")
			}
			return types.ErrNoAck
		}
		return nil
	})
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

// reDial connection to amqp with one or more fallback URLs
func (a *AMQP) reDial(urls []string) (conn *amqp.Connection, err error) {
	for _, u := range urls {
		conn, err = a.dial(u)
		if err != nil {
			if errors.Is(err, errAMQP09Connect) {
				continue
			}
			break
		}
		return conn, nil
	}
	return nil, err
}

// dial attempts to connect to amqp URL
func (a *AMQP) dial(amqpURL string) (conn *amqp.Connection, err error) {
	u, err := url.Parse(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("invalid AMQP URL: %w", err)
	}

	if a.conf.TLS.Enabled {
		if u.User != nil {
			conn, err = amqp.DialTLS(amqpURL, a.tlsConf)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", errAMQP09Connect, err)
			}
		} else {
			conn, err = amqp.DialTLS_ExternalAuth(amqpURL, a.tlsConf)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", errAMQP09Connect, err)
			}
		}
	} else {
		conn, err = amqp.Dial(amqpURL)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", errAMQP09Connect, err)
		}
	}

	return conn, nil
}
