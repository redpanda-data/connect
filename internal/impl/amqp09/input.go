package amqp09

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		var a input.Async
		var err error
		if a, err = newAMQP09Reader(c.AMQP09, nm); err != nil {
			return nil, err
		}
		return input.NewAsyncReader("amqp_0_9", a, nm)
	}), docs.ComponentSpec{
		Name: "amqp_0_9",
		Summary: `
Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various
message brokers, including RabbitMQ.`,
		Description: `
TLS is automatic when connecting to an ` + "`amqps`" + ` URL, but custom
settings can be enabled in the ` + "`tls`" + ` section.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- amqp_content_type
- amqp_content_encoding
- amqp_delivery_mode
- amqp_priority
- amqp_correlation_id
- amqp_reply_to
- amqp_expiration
- amqp_message_id
- amqp_timestamp
- amqp_type
- amqp_user_id
- amqp_app_id
- amqp_consumer_tag
- amqp_delivery_tag
- amqp_redelivered
- amqp_exchange
- amqp_routing_key
- All existing message headers, including nested headers prefixed with the key of their respective parent.
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Categories: []string{
			"Services",
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("urls",
				"A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"amqp://guest:guest@127.0.0.1:5672/"},
				[]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"},
				[]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"},
			).Array().AtVersion("3.58.0").HasDefault([]any{}),
			docs.FieldString("queue", "An AMQP queue to consume from.").HasDefault(""),
			docs.FieldObject("queue_declare", `
Allows you to passively declare the target queue. If the queue already exists
then the declaration passively verifies that they match the target fields.`,
			).WithChildren(
				docs.FieldBool("enabled", "Whether to enable queue declaration.").HasDefault(false),
				docs.FieldBool("durable", "Whether the declared queue is durable.").HasDefault(true),
				docs.FieldBool("auto_delete", "Whether the declared queue will auto-delete.").HasDefault(false),
			).Advanced(),
			docs.FieldObject("bindings_declare",
				"Allows you to passively declare bindings for the target queue.",
				[]any{
					map[string]any{
						"exchange": "foo",
						"key":      "bar",
					},
				},
			).Array().WithChildren(
				docs.FieldString("exchange", "The exchange of the declared binding.").HasDefault(""),
				docs.FieldString("key", "The key of the declared binding.").HasDefault(""),
			).Advanced().HasDefault([]any{}),
			docs.FieldString("consumer_tag", "A consumer tag.").HasDefault(""),
			docs.FieldBool("auto_ack", "Acknowledge messages automatically as they are consumed rather than waiting for acknowledgments from downstream. This can improve throughput and prevent the pipeline from blocking but at the cost of eliminating delivery guarantees.").Advanced().HasDefault(false),
			docs.FieldString("nack_reject_patterns", "A list of regular expression patterns whereby if a message that has failed to be delivered by Benthos has an error that matches it will be dropped (or delivered to a dead-letter queue if one exists). By default failed messages are nacked with requeue enabled.", []string{"^reject me please:.+$"}).Array().Advanced().AtVersion("3.64.0").HasDefault([]any{}),
			docs.FieldInt("prefetch_count", "The maximum number of pending messages to have consumed at a time.").HasDefault(10),
			docs.FieldInt("prefetch_size", "The maximum amount of pending messages measured in bytes to have consumed at a time.").Advanced().HasDefault(0),
			btls.FieldSpec(),
		),
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

var errAMQP09Connect = errors.New("failed to connect to server")

type amqp09Reader struct {
	conn         *amqp.Connection
	amqpChan     *amqp.Channel
	consumerChan <-chan amqp.Delivery

	urls    []string
	tlsConf *tls.Config

	nackRejectPattens []*regexp.Regexp

	conf input.AMQP09Config
	log  log.Modular

	m sync.RWMutex
}

func newAMQP09Reader(conf input.AMQP09Config, mgr bundle.NewManagement) (*amqp09Reader, error) {
	a := amqp09Reader{
		conf: conf,
		log:  mgr.Logger(),
	}

	if len(conf.URLs) == 0 {
		return nil, errors.New("must specify at least one URL")
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if trimmed := strings.TrimSpace(splitURL); len(trimmed) > 0 {
				a.urls = append(a.urls, trimmed)
			}
		}
	}

	for _, p := range conf.NackRejectPatterns {
		r, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("failed to compile nack reject pattern: %w", err)
		}
		a.nackRejectPattens = append(a.nackRejectPattens, r)
	}

	if conf.TLS.Enabled {
		var err error
		if a.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection to an AMQP09 server.
func (a *amqp09Reader) Connect(ctx context.Context) (err error) {
	a.m.Lock()
	defer a.m.Unlock()

	if a.conn != nil {
		return nil
	}

	var conn *amqp.Connection
	var amqpChan *amqp.Channel
	var consumerChan <-chan amqp.Delivery

	if conn, err = a.reDial(a.urls); err != nil {
		return err
	}

	amqpChan, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("AMQP 0.9 Channel: %s", err)
	}

	if a.conf.QueueDeclare.Enabled {
		if _, err = amqpChan.QueueDeclare(
			a.conf.Queue,                   // name of the queue
			a.conf.QueueDeclare.Durable,    // durable
			a.conf.QueueDeclare.AutoDelete, // delete when unused
			false,                          // exclusive
			false,                          // noWait
			nil,                            // arguments
		); err != nil {
			return fmt.Errorf("queue Declare: %s", err)
		}
	}

	for _, bConf := range a.conf.BindingsDeclare {
		if err = amqpChan.QueueBind(
			a.conf.Queue,     // name of the queue
			bConf.RoutingKey, // bindingKey
			bConf.Exchange,   // sourceExchange
			false,            // noWait
			nil,              // arguments
		); err != nil {
			return fmt.Errorf("queue Bind: %s", err)
		}
	}

	if err = amqpChan.Qos(
		a.conf.PrefetchCount, a.conf.PrefetchSize, false,
	); err != nil {
		return fmt.Errorf("qos: %s", err)
	}

	if consumerChan, err = amqpChan.Consume(
		a.conf.Queue,       // name
		a.conf.ConsumerTag, // consumerTag,
		a.conf.AutoAck,     // autoAck
		false,              // exclusive
		false,              // noLocal
		false,              // noWait
		nil,                // arguments
	); err != nil {
		return fmt.Errorf("queue Consume: %s", err)
	}

	a.conn = conn
	a.amqpChan = amqpChan
	a.consumerChan = consumerChan

	a.log.Infof("Receiving AMQP 0.9 messages from queue: %v\n", a.conf.Queue)
	return
}

// disconnect safely closes a connection to an AMQP09 server.
func (a *amqp09Reader) disconnect() error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.amqpChan != nil {
		if err := a.amqpChan.Cancel(a.conf.ConsumerTag, true); err != nil {
			a.log.Errorf("Failed to cancel consumer: %v\n", err)
		}
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

func amqpSetMetadata(p *message.Part, k string, v any) {
	var metaValue string
	metaKey := strings.ReplaceAll(k, "-", "_")

	switch v := v.(type) {
	case bool:
		metaValue = strconv.FormatBool(v)
	case float32:
		metaValue = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		metaValue = strconv.FormatFloat(v, 'f', -1, 64)
	case byte:
		metaValue = strconv.Itoa(int(v))
	case int16:
		metaValue = strconv.Itoa(int(v))
	case int32:
		metaValue = strconv.Itoa(int(v))
	case int64:
		metaValue = strconv.Itoa(int(v))
	case nil:
		metaValue = ""
	case string:
		metaValue = v
	case []byte:
		metaValue = string(v)
	case time.Time:
		metaValue = v.Format(time.RFC3339)
	case amqp.Decimal:
		dec := strconv.Itoa(int(v.Value))
		index := len(dec) - int(v.Scale)
		metaValue = dec[:index] + "." + dec[index:]
	case amqp.Table:
		for key, value := range v {
			amqpSetMetadata(p, metaKey+"_"+key, value)
		}
		return
	case []interface{}:
		for key, value := range v {
			amqpSetMetadata(p, fmt.Sprintf("%s_%v", metaKey, key), value)
		}
		return
	default:
		metaValue = ""
	}

	if metaValue != "" {
		p.MetaSetMut(metaKey, metaValue)
	}
}

// ReadBatch a new AMQP09 message.
func (a *amqp09Reader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	var c <-chan amqp.Delivery

	a.m.RLock()
	if a.conn != nil {
		c = a.consumerChan
	}
	a.m.RUnlock()

	if c == nil {
		return nil, nil, component.ErrNotConnected
	}

	msg := message.QuickBatch(nil)
	addPart := func(data amqp.Delivery) {
		part := message.NewPart(data.Body)

		for k, v := range data.Headers {
			amqpSetMetadata(part, k, v)
		}

		amqpSetMetadata(part, "amqp_content_type", data.ContentType)
		amqpSetMetadata(part, "amqp_content_encoding", data.ContentEncoding)

		if data.DeliveryMode != 0 {
			amqpSetMetadata(part, "amqp_delivery_mode", data.DeliveryMode)
		}

		amqpSetMetadata(part, "amqp_priority", data.Priority)
		amqpSetMetadata(part, "amqp_correlation_id", data.CorrelationId)
		amqpSetMetadata(part, "amqp_reply_to", data.ReplyTo)
		amqpSetMetadata(part, "amqp_expiration", data.Expiration)
		amqpSetMetadata(part, "amqp_message_id", data.MessageId)

		if !data.Timestamp.IsZero() {
			amqpSetMetadata(part, "amqp_timestamp", data.Timestamp.Unix())
		}

		amqpSetMetadata(part, "amqp_type", data.Type)
		amqpSetMetadata(part, "amqp_user_id", data.UserId)
		amqpSetMetadata(part, "amqp_app_id", data.AppId)
		amqpSetMetadata(part, "amqp_consumer_tag", data.ConsumerTag)
		amqpSetMetadata(part, "amqp_delivery_tag", data.DeliveryTag)
		amqpSetMetadata(part, "amqp_redelivered", data.Redelivered)
		amqpSetMetadata(part, "amqp_exchange", data.Exchange)
		amqpSetMetadata(part, "amqp_routing_key", data.RoutingKey)

		msg = append(msg, part)
	}

	select {
	case data, open := <-c:
		if !open {
			_ = a.disconnect()
			return nil, nil, component.ErrNotConnected
		}
		addPart(data)
		return msg, func(actx context.Context, res error) error {
			if a.conf.AutoAck {
				return nil
			}
			if res != nil {
				errStr := res.Error()
				for _, p := range a.nackRejectPattens {
					if p.MatchString(errStr) {
						return data.Nack(false, false)
					}
				}
				return data.Nack(false, true)
			}
			return data.Ack(false)
		}, nil
	case <-ctx.Done():
	}
	return nil, nil, component.ErrTimeout
}

func (a *amqp09Reader) Close(ctx context.Context) error {
	return a.disconnect()
}

// reDial connection to amqp with one or more fallback URLs.
func (a *amqp09Reader) reDial(urls []string) (conn *amqp.Connection, err error) {
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
func (a *amqp09Reader) dial(amqpURL string) (conn *amqp.Connection, err error) {
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
