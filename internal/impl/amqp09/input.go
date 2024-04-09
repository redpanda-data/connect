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

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

func amqp09InputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary(`Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various message brokers, including RabbitMQ.`).
		Description(`
TLS is automatic when connecting to an `+"`amqps`"+` URL, but custom settings can be enabled in the `+"`tls`"+` section.

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
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
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).Fields(
		service.NewURLListField(urlsField).
			Description("A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"amqp://guest:guest@127.0.0.1:5672/"}).
			Example([]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"}).
			Example([]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"}).
			Version("3.58.0"),
		service.NewStringField(queueField).
			Description("An AMQP queue to consume from."),
		service.NewObjectField(queueDeclareField,
			service.NewBoolField(queueDeclareEnabledField).
				Description("Whether to enable queue declaration.").
				Default(false),
			service.NewBoolField(queueDeclareDurableField).
				Description("Whether the declared queue is durable.").
				Default(true),
			service.NewBoolField(queueDeclareAutoDeleteField).
				Description("Whether the declared queue will auto-delete.").
				Default(false),
		).
			Description(`Allows you to passively declare the target queue. If the queue already exists then the declaration passively verifies that they match the target fields.`).
			Advanced().
			Optional(),
		service.NewObjectListField(bindingsDeclareField,
			service.NewStringField(bindingsDeclareExchangeField).
				Description("The exchange of the declared binding.").
				Default(""),
			service.NewStringField(bindingsDeclareKeyField).
				Description("The key of the declared binding.").
				Default(""),
		).
			Description(`Allows you to passively declare bindings for the target queue.`).
			Advanced().
			Optional().
			Example([]any{
				map[string]any{
					"exchange": "foo",
					"key":      "bar",
				},
			}),
		service.NewStringField(consumerTagField).
			Description("A consumer tag.").
			Default(""),
		service.NewBoolField(autoAckField).
			Description("Acknowledge messages automatically as they are consumed rather than waiting for acknowledgments from downstream. This can improve throughput and prevent the pipeline from blocking but at the cost of eliminating delivery guarantees.").
			Default(false).
			Advanced(),
		service.NewStringListField(nackRejectPattensField).
			Description("A list of regular expression patterns whereby if a message that has failed to be delivered by Benthos has an error that matches it will be dropped (or delivered to a dead-letter queue if one exists). By default failed messages are nacked with requeue enabled.").
			Example([]string{"^reject me please:.+$"}).
			Advanced().
			Version("3.64.0").
			Default([]any{}),
		service.NewIntField(prefetchCountField).
			Description("The maximum number of pending messages to have consumed at a time.").
			Default(10),
		service.NewIntField(prefetchSizeField).
			Description("The maximum amount of pending messages measured in bytes to have consumed at a time.").
			Default(0).
			Advanced(),
		service.NewTLSToggledField(tlsField),
	)
}

func init() {
	err := service.RegisterInput("amqp_0_9", amqp09InputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		return amqp09ReaderFromParsed(conf, mgr)
	})
	if err != nil {
		panic(err)
	}
}

type amqp09BindingDeclare struct {
	exchange   string
	routingKey string
}

//------------------------------------------------------------------------------

var errAMQP09Connect = errors.New("failed to connect to server")

type amqp09Reader struct {
	conn         *amqp.Connection
	amqpChan     *amqp.Channel
	consumerChan <-chan amqp.Delivery

	urls       []string
	queue      string
	tlsEnabled bool
	tlsConf    *tls.Config

	prefetchCount int
	prefetchSize  int
	consumerTag   string
	autoAck       bool

	nackRejectPattens []*regexp.Regexp

	queueDeclare    bool
	queueDurable    bool
	queueAutoDelete bool

	bindingDeclare []amqp09BindingDeclare

	log *service.Logger
	m   sync.RWMutex
}

func amqp09ReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*amqp09Reader, error) {
	a := amqp09Reader{
		log: mgr.Logger(),
	}

	urlStrs, err := conf.FieldStringList(urlsField)
	if err != nil {
		return nil, err
	}
	if len(urlStrs) == 0 {
		return nil, errors.New("must specify at least one URL")
	}
	for _, u := range urlStrs {
		for _, splitURL := range strings.Split(u, ",") {
			if trimmed := strings.TrimSpace(splitURL); trimmed != "" {
				a.urls = append(a.urls, trimmed)
			}
		}
	}

	if a.queue, err = conf.FieldString(queueField); err != nil {
		return nil, err
	}

	if a.tlsConf, a.tlsEnabled, err = conf.FieldTLSToggled(tlsField); err != nil {
		return nil, err
	}

	if a.prefetchCount, err = conf.FieldInt(prefetchCountField); err != nil {
		return nil, err
	}
	if a.prefetchSize, err = conf.FieldInt(prefetchSizeField); err != nil {
		return nil, err
	}
	if a.consumerTag, err = conf.FieldString(consumerTagField); err != nil {
		return nil, err
	}
	if a.autoAck, err = conf.FieldBool(autoAckField); err != nil {
		return nil, err
	}

	if conf.Contains(nackRejectPattensField) {
		nackPatternStrs, err := conf.FieldStringList(nackRejectPattensField)
		if err != nil {
			return nil, err
		}
		for _, p := range nackPatternStrs {
			r, err := regexp.Compile(p)
			if err != nil {
				return nil, fmt.Errorf("failed to compile nack reject pattern: %w", err)
			}
			a.nackRejectPattens = append(a.nackRejectPattens, r)
		}
	}

	if conf.Contains(queueDeclareField) {
		qdConf := conf.Namespace(queueDeclareField)
		a.queueDeclare, _ = qdConf.FieldBool(queueDeclareEnabledField)
		a.queueDurable, _ = qdConf.FieldBool(queueDeclareDurableField)
		a.queueAutoDelete, _ = qdConf.FieldBool(queueDeclareAutoDeleteField)
	}

	if conf.Contains(bindingsDeclareField) {
		qbConfs, err := conf.FieldObjectList(bindingsDeclareField)
		if err != nil {
			return nil, err
		}
		for _, c := range qbConfs {
			var dec amqp09BindingDeclare
			if dec.exchange, err = c.FieldString(bindingsDeclareExchangeField); err != nil {
				return nil, err
			}
			if dec.routingKey, err = c.FieldString(bindingsDeclareKeyField); err != nil {
				return nil, err
			}
			a.bindingDeclare = append(a.bindingDeclare, dec)
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
		return fmt.Errorf("AMQP 0.9 Channel: %w", err)
	}

	if a.queueDeclare {
		if _, err = amqpChan.QueueDeclare(
			a.queue,           // name of the queue
			a.queueDurable,    // durable
			a.queueAutoDelete, // delete when unused
			false,             // exclusive
			false,             // noWait
			nil,               // arguments
		); err != nil {
			_ = amqpChan.Close()
			_ = conn.Close()
			return fmt.Errorf("queue Declare: %w", err)
		}
	}

	for _, bConf := range a.bindingDeclare {
		if err = amqpChan.QueueBind(
			a.queue,          // name of the queue
			bConf.routingKey, // bindingKey
			bConf.exchange,   // sourceExchange
			false,            // noWait
			nil,              // arguments
		); err != nil {
			_ = amqpChan.Close()
			_ = conn.Close()
			return fmt.Errorf("queue Bind: %w", err)
		}
	}

	if err = amqpChan.Qos(
		a.prefetchCount, a.prefetchSize, false,
	); err != nil {
		_ = amqpChan.Close()
		_ = conn.Close()
		return fmt.Errorf("qos: %w", err)
	}

	if consumerChan, err = amqpChan.Consume(
		a.queue,       // name
		a.consumerTag, // consumerTag,
		a.autoAck,     // autoAck
		false,         // exclusive
		false,         // noLocal
		false,         // noWait
		nil,           // arguments
	); err != nil {
		_ = amqpChan.Close()
		_ = conn.Close()
		return fmt.Errorf("queue Consume: %w", err)
	}

	a.conn = conn
	a.amqpChan = amqpChan
	a.consumerChan = consumerChan
	return
}

// disconnect safely closes a connection to an AMQP09 server.
func (a *amqp09Reader) disconnect() error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.amqpChan != nil {
		if err := a.amqpChan.Cancel(a.consumerTag, true); err != nil {
			a.log.Errorf("Failed to cancel consumer: %w", err)
		}
		a.amqpChan = nil
	}
	if a.conn != nil {
		if err := a.conn.Close(); err != nil {
			a.log.Errorf("Failed to close connection cleanly: %w", err)
		}
		a.conn = nil
	}

	return nil
}

//------------------------------------------------------------------------------

func amqpSetMetadata(p *service.Message, k string, v any) {
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
			amqpSetMetadata(p, fmt.Sprintf("%s_%d", metaKey, key), value)
		}
		return
	default:
		metaValue = ""
	}

	if metaValue != "" {
		p.MetaSetMut(metaKey, metaValue)
	}
}

func (a *amqp09Reader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	var c <-chan amqp.Delivery

	a.m.RLock()
	if a.conn != nil {
		c = a.consumerChan
	}
	a.m.RUnlock()

	if c == nil {
		return nil, nil, service.ErrNotConnected
	}

	dataToMsg := func(data amqp.Delivery) *service.Message {
		part := service.NewMessage(data.Body)

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

		return part
	}

	select {
	case data, open := <-c:
		if !open {
			_ = a.disconnect()
			return nil, nil, service.ErrNotConnected
		}
		return dataToMsg(data), func(actx context.Context, res error) error {
			if a.autoAck {
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

	if a.tlsEnabled {
		if u.User != nil {
			conn, err = amqp.DialTLS(amqpURL, a.tlsConf)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errAMQP09Connect, err)
			}
		} else {
			conn, err = amqp.DialTLS_ExternalAuth(amqpURL, a.tlsConf)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errAMQP09Connect, err)
			}
		}
	} else {
		conn, err = amqp.Dial(amqpURL)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errAMQP09Connect, err)
		}
	}

	return conn, nil
}
