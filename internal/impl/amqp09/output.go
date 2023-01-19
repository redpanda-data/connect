package amqp09

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

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		a, err := newAMQP09Writer(nm, c.AMQP09, nm.Logger())
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("amqp_0_9", c.AMQP09.MaxInFlight, a, nm)
		if err != nil {
			return nil, err
		}
		return output.OnlySinglePayloads(w), nil
	}), docs.ComponentSpec{
		Name: "amqp_0_9",
		Summary: `
Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by
various message brokers, including RabbitMQ.`,
		Description: output.Description(true, false, `
The metadata from each message are delivered as headers.

It's possible for this output type to create the target exchange by setting
`+"`exchange_declare.enabled` to `true`"+`, if the exchange already exists
then the declaration passively verifies that the settings match.

TLS is automatic when connecting to an `+"`amqps`"+` URL, but custom
settings can be enabled in the `+"`tls`"+` section.

The fields 'key' and 'type' can be dynamically set using function interpolations described
[here](/docs/configuration/interpolation#bloblang-queries).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("urls",
				"A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"amqp://guest:guest@127.0.0.1:5672/"},
				[]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"},
				[]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"},
			).Array().AtVersion("3.58.0").HasDefault([]any{}),
			docs.FieldString("exchange", "An AMQP exchange to publish to.").HasDefault(""),
			docs.FieldObject("exchange_declare", "Optionally declare the target exchange (passive).").WithChildren(
				docs.FieldBool("enabled", "Whether to declare the exchange.").HasDefault(false),
				docs.FieldString("type", "The type of the exchange.").HasOptions(
					"direct", "fanout", "topic", "x-custom",
				).HasDefault("direct"),
				docs.FieldBool("durable", "Whether the exchange should be durable.").HasDefault(true),
			).Advanced(),
			docs.FieldString("key", "The binding key to set for each message.").IsInterpolated().HasDefault(""),
			docs.FieldString("type", "The type property to set for each message.").IsInterpolated().HasDefault(""),
			docs.FieldString("content_type", "The content type attribute to set for each message.").IsInterpolated().Advanced().HasDefault("application/octet-stream"),
			docs.FieldString("content_encoding", "The content encoding attribute to set for each message.").IsInterpolated().Advanced().HasDefault(""),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are attached to messages as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			docs.FieldString("priority", "Set the priority of each message with a dynamic interpolated expression.", "0", `${! meta("amqp_priority") }`, `${! json("doc.priority") }`).IsInterpolated().Advanced().HasDefault(""),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").HasDefault(64),
			docs.FieldBool("persistent", "Whether message delivery should be persistent (transient by default).").Advanced().HasDefault(false),
			docs.FieldBool("mandatory", "Whether to set the mandatory flag on published messages. When set if a published message is routed to zero queues it is returned.").Advanced().HasDefault(false),
			docs.FieldBool("immediate", "Whether to set the immediate flag on published messages. When set if there are no ready consumers of a queue then the message is dropped instead of waiting.").Advanced().HasDefault(false),
			docs.FieldString("timeout", "The maximum period to wait before abandoning it and reattempting. If not set, wait indefinitely.").Advanced().HasDefault(""),
			btls.FieldSpec(),
		),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

type amqp09Writer struct {
	key             *field.Expression
	msgType         *field.Expression
	contentType     *field.Expression
	contentEncoding *field.Expression
	priority        *field.Expression
	metaFilter      *metadata.ExcludeFilter

	log log.Modular

	conf    output.AMQPConfig
	urls    []string
	tlsConf *tls.Config

	conn       *amqp.Connection
	amqpChan   *amqp.Channel
	returnChan <-chan amqp.Return
	timeout    time.Duration

	deliveryMode uint8

	connLock sync.RWMutex
}

func newAMQP09Writer(mgr bundle.NewManagement, conf output.AMQPConfig, log log.Modular) (*amqp09Writer, error) {
	var timeout time.Duration
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	a := amqp09Writer{
		log:          log,
		conf:         conf,
		deliveryMode: amqp.Transient,
		timeout:      timeout,
	}
	var err error
	if a.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	if a.key, err = mgr.BloblEnvironment().NewField(conf.BindingKey); err != nil {
		return nil, fmt.Errorf("failed to parse binding key expression: %v", err)
	}
	if a.msgType, err = mgr.BloblEnvironment().NewField(conf.Type); err != nil {
		return nil, fmt.Errorf("failed to parse type property expression: %v", err)
	}
	if a.contentType, err = mgr.BloblEnvironment().NewField(conf.ContentType); err != nil {
		return nil, fmt.Errorf("failed to parse content_type property expression: %v", err)
	}
	if a.contentEncoding, err = mgr.BloblEnvironment().NewField(conf.ContentEncoding); err != nil {
		return nil, fmt.Errorf("failed to parse content_encoding property expression: %v", err)
	}
	if a.priority, err = mgr.BloblEnvironment().NewField(conf.Priority); err != nil {
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
		if a.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

func (a *amqp09Writer) Connect(ctx context.Context) error {
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
	if a.conf.Mandatory || a.conf.Immediate {
		a.returnChan = amqpChan.NotifyReturn(make(chan amqp.Return, 1))
	}

	a.log.Infof("Sending AMQP messages to exchange: %v\n", a.conf.Exchange)
	return nil
}

// disconnect safely closes a connection to an AMQP server.
func (a *amqp09Writer) disconnect() error {
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

func (a *amqp09Writer) WriteBatch(wctx context.Context, msg message.Batch) error {
	a.connLock.RLock()
	conn := a.conn
	amqpChan := a.amqpChan
	returnChan := a.returnChan
	a.connLock.RUnlock()

	if conn == nil {
		return component.ErrNotConnected
	}

	var ctx context.Context
	if a.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			wctx, a.timeout,
		)
		defer cancel()
	} else {
		ctx = wctx
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		bindingKey, err := a.key.String(i, msg)
		if err != nil {
			return fmt.Errorf("binding key interpolation error: %w", err)
		}
		bindingKey = strings.ReplaceAll(bindingKey, "/", ".")

		msgType, err := a.msgType.String(i, msg)
		if err != nil {
			return fmt.Errorf("msg type interpolation error: %w", err)
		}
		msgType = strings.ReplaceAll(msgType, "/", ".")

		contentType, err := a.contentType.String(i, msg)
		if err != nil {
			return fmt.Errorf("content type interpolation error: %w", err)
		}
		contentEncoding, err := a.contentEncoding.String(i, msg)
		if err != nil {
			return fmt.Errorf("content encoding interpolation error: %w", err)
		}

		priorityString, err := a.priority.String(i, msg)
		if err != nil {
			return fmt.Errorf("priority interpolation error: %w", err)
		}

		var priority uint8
		if priorityString != "" {
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
		_ = a.metaFilter.Iter(p, func(k string, v any) error {
			headers[strings.ReplaceAll(k, "_", "-")] = v
			return nil
		})

		conf, err := amqpChan.PublishWithDeferredConfirmWithContext(
			ctx,
			a.conf.Exchange,  // publish to an exchange
			bindingKey,       // routing to 0 or more queues
			a.conf.Mandatory, // mandatory
			a.conf.Immediate, // immediate
			amqp.Publishing{
				Headers:         headers,
				ContentType:     contentType,
				ContentEncoding: contentEncoding,
				Body:            p.AsBytes(),
				DeliveryMode:    a.deliveryMode, // 1=non-persistent, 2=persistent
				Priority:        priority,       // 0-9
				Type:            msgType,
				// a bunch of application/implementation-specific fields
			},
		)
		if err != nil {
			_ = a.disconnect()
			a.log.Errorf("Failed to send message: %v\n", err)
			return component.ErrNotConnected
		}
		if !conf.Wait() {
			a.log.Errorln("Failed to acknowledge message.")
			return component.ErrNoAck
		}
		if returnChan != nil {
			select {
			case _, open := <-returnChan:
				if !open {
					return fmt.Errorf("acknowledgement not supported, ensure server supports immediate and mandatory flags")
				}
				return component.ErrNoAck
			default:
			}
		}
		return nil
	})
}

func (a *amqp09Writer) Close(context.Context) error {
	return a.disconnect()
}

// reDial connection to amqp with one or more fallback URLs.
func (a *amqp09Writer) reDial(urls []string) (conn *amqp.Connection, err error) {
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

// dial attempts to connect to amqp URL.
func (a *amqp09Writer) dial(amqpURL string) (conn *amqp.Connection, err error) {
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
