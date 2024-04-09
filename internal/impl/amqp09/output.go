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

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

func amqp09OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary(`Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by various message brokers, including RabbitMQ.Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various message brokers, including RabbitMQ.`).
		Description(`The metadata from each message are delivered as headers.

It's possible for this output type to create the target exchange by setting `+"`exchange_declare.enabled` to `true`"+`, if the exchange already exists then the declaration passively verifies that the settings match.

TLS is automatic when connecting to an `+"`amqps`"+` URL, but custom settings can be enabled in the `+"`tls`"+` section.

The fields 'key', 'exchange' and 'type' can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewURLListField(urlsField).
				Description("A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"amqp://guest:guest@127.0.0.1:5672/"}).
				Example([]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"}).
				Example([]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"}).
				Version("3.58.0"),
			service.NewInterpolatedStringField(exchangeField).
				Description("An AMQP exchange to publish to."),
			service.NewObjectField(exchangeDeclareField,
				service.NewBoolField(exchangeDeclareEnabledField).
					Description("Whether to declare the exchange.").
					Default(false),
				service.NewStringEnumField(exchangeDeclareTypeField, "direct", "fanout", "topic", "x-custom").
					Description("The type of the exchange.").
					Default("direct"),
				service.NewBoolField(exchangeDeclareDurableField).
					Description("Whether the exchange should be durable.").
					Default(true),
			).
				Description(`Optionally declare the target exchange (passive).`).
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(keyField).
				Description("The binding key to set for each message.").
				Default(""),
			service.NewInterpolatedStringField(typeField).
				Description("The type property to set for each message.").
				Default(""),
			service.NewInterpolatedStringField(contentTypeField).
				Description("The content type attribute to set for each message.").
				Advanced().
				Default("application/octet-stream"),
			service.NewInterpolatedStringField(contentEncodingField).
				Description("The content encoding attribute to set for each message.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(correlationIDField).
				Description("Set the correlation ID of each message with a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(replyToField).
				Description("Carries response queue name - set with a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(expirationField).
				Description("Set the per-message TTL").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(messageIDField).
				Description("Set the message ID of each message with a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(userIDField).
				Description("Set the user ID to the name of the publisher.  If this property is set by a publisher, its value must be equal to the name of the user used to open the connection.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(appIDField).
				Description("Set the application ID of each message with a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewMetadataExcludeFilterField(metadataFilterField).
				Description("Specify criteria for which metadata values are attached to messages as headers."),
			service.NewInterpolatedStringField(priorityField).
				Description("Set the priority of each message with a dynamic interpolated expression.").
				Advanced().
				Example("0").
				Example(`${! meta("amqp_priority") }`).
				Example(`${! json("doc.priority") }`).
				Default(""),
			service.NewOutputMaxInFlightField(),
			service.NewBoolField(persistentField).
				Description("Whether message delivery should be persistent (transient by default).").
				Advanced().
				Default(false),
			service.NewBoolField(mandatoryField).
				Description("Whether to set the mandatory flag on published messages. When set if a published message is routed to zero queues it is returned.").
				Advanced().
				Default(false),
			service.NewBoolField(immediateField).
				Description("Whether to set the immediate flag on published messages. When set if there are no ready consumers of a queue then the message is dropped instead of waiting.").
				Advanced().
				Default(false),
			service.NewDurationField(timeoutField).
				Description("The maximum period to wait before abandoning it and reattempting. If not set, wait indefinitely.").
				Advanced().
				Default(""),
			service.NewTLSToggledField(tlsField),
		)
}

func init() {
	err := service.RegisterOutput("amqp_0_9", amqp09OutputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		maxInFlight, err := conf.FieldMaxInFlight()
		if err != nil {
			return nil, 0, err
		}
		w, err := amqp09WriterFromParsed(conf, mgr)
		return w, maxInFlight, err
	})
	if err != nil {
		panic(err)
	}
}

type amqp09Writer struct {
	key             *service.InterpolatedString
	msgType         *service.InterpolatedString
	contentType     *service.InterpolatedString
	contentEncoding *service.InterpolatedString
	exchange        *service.InterpolatedString
	priority        *service.InterpolatedString
	correlationID   *service.InterpolatedString
	replyTo         *service.InterpolatedString
	expiration      *service.InterpolatedString
	messageID       *service.InterpolatedString
	userID          *service.InterpolatedString
	appID           *service.InterpolatedString
	metaFilter      *service.MetadataExcludeFilter

	urls         []string
	tlsEnabled   bool
	tlsConf      *tls.Config
	timeout      time.Duration
	deliveryMode uint8
	mandatory    bool
	immediate    bool

	exchangesDeclared    map[string]struct{}
	exchangesDeclaredMut sync.Mutex

	exchangeDeclare        bool
	exchangeDeclareType    string
	exchangeDeclareDurable bool

	log *service.Logger

	conn       *amqp.Connection
	amqpChan   *amqp.Channel
	returnChan <-chan amqp.Return

	connLock sync.RWMutex
}

func amqp09WriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*amqp09Writer, error) {
	a := amqp09Writer{
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

	if a.exchange, err = conf.FieldInterpolatedString(exchangeField); err != nil {
		return nil, err
	}
	if a.tlsConf, a.tlsEnabled, err = conf.FieldTLSToggled(tlsField); err != nil {
		return nil, err
	}
	if durStr, _ := conf.FieldString(timeoutField); durStr != "" {
		if a.timeout, err = conf.FieldDuration(timeoutField); err != nil {
			return nil, err
		}
	}
	if persistent, _ := conf.FieldBool(persistentField); persistent {
		a.deliveryMode = amqp.Persistent
	} else {
		a.deliveryMode = amqp.Transient
	}
	if a.mandatory, err = conf.FieldBool(mandatoryField); err != nil {
		return nil, err
	}
	if a.immediate, err = conf.FieldBool(immediateField); err != nil {
		return nil, err
	}

	if conf.Contains(exchangeDeclareField) {
		edConf := conf.Namespace(exchangeDeclareField)
		if a.exchangeDeclare, err = edConf.FieldBool(exchangeDeclareEnabledField); err != nil {
			return nil, err
		}
		if a.exchangeDeclareType, err = edConf.FieldString(exchangeDeclareTypeField); err != nil {
			return nil, err
		}
		if a.exchangeDeclareDurable, err = edConf.FieldBool(exchangeDeclareDurableField); err != nil {
			return nil, err
		}
	}

	if a.key, err = conf.FieldInterpolatedString(keyField); err != nil {
		return nil, err
	}
	if a.msgType, err = conf.FieldInterpolatedString(typeField); err != nil {
		return nil, err
	}
	if a.contentType, err = conf.FieldInterpolatedString(contentTypeField); err != nil {
		return nil, err
	}
	if a.contentEncoding, err = conf.FieldInterpolatedString(contentEncodingField); err != nil {
		return nil, err
	}
	if a.priority, err = conf.FieldInterpolatedString(priorityField); err != nil {
		return nil, err
	}
	if a.correlationID, err = conf.FieldInterpolatedString(correlationIDField); err != nil {
		return nil, err
	}
	if a.replyTo, err = conf.FieldInterpolatedString(replyToField); err != nil {
		return nil, err
	}
	if a.expiration, err = conf.FieldInterpolatedString(expirationField); err != nil {
		return nil, err
	}
	if a.messageID, err = conf.FieldInterpolatedString(messageIDField); err != nil {
		return nil, err
	}
	if a.userID, err = conf.FieldInterpolatedString(userIDField); err != nil {
		return nil, err
	}
	if a.appID, err = conf.FieldInterpolatedString(appIDField); err != nil {
		return nil, err
	}

	if a.metaFilter, err = conf.FieldMetadataExcludeFilter(metadataFilterField); err != nil {
		return nil, err
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
		return fmt.Errorf("amqp failed to create channel: %w", err)
	}

	if err = amqpChan.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("amqp channel could not be put into confirm mode: %w", err)
	}

	a.conn = conn
	a.amqpChan = amqpChan
	if a.mandatory || a.immediate {
		a.returnChan = amqpChan.NotifyReturn(make(chan amqp.Return, 1))
	}

	if sExchange, isStatic := a.exchange.Static(); isStatic {
		if err := a.declareExchange(sExchange); err != nil {
			a.log.Errorf("Failed to declare exchange: %w", err)
		}
	}
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
			a.log.Errorf("Failed to close connection cleanly: %w", err)
		}
		a.conn = nil
	}
	return nil
}

// declareExchange declare and memoize the declaration of an AMQP exchange
func (a *amqp09Writer) declareExchange(exchange string) error {
	if !a.exchangeDeclare {
		return nil
	}

	a.exchangesDeclaredMut.Lock()
	defer a.exchangesDeclaredMut.Unlock()

	if a.exchangesDeclared == nil {
		a.exchangesDeclared = map[string]struct{}{}
	}

	// check if the exchange name exists in exchangeDeclarationStatus
	if _, exists := a.exchangesDeclared[exchange]; exists {
		a.log.Debugf("Exchange %s exists in cache, not re-declaring", exchange)
		return nil
	}

	a.log.Debugf("Exchange %s does not exist, declaring", exchange)
	if err := a.amqpChan.ExchangeDeclare(
		exchange,                 // name of the exchange
		a.exchangeDeclareType,    // type
		a.exchangeDeclareDurable, // durable
		false,                    // delete when complete
		false,                    // internal
		false,                    // noWait
		nil,                      // arguments
	); err != nil {
		return fmt.Errorf("amqp failed to declare exchange: %w", err)
	}
	a.exchangesDeclared[exchange] = struct{}{}
	return nil
}

func (a *amqp09Writer) Write(ctx context.Context, msg *service.Message) error {
	a.connLock.RLock()
	conn := a.conn
	amqpChan := a.amqpChan
	returnChan := a.returnChan
	a.connLock.RUnlock()

	if conn == nil {
		return service.ErrNotConnected
	}

	if a.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, a.timeout)
		defer cancel()
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	bindingKey, err := a.key.TryString(msg)
	if err != nil {
		return fmt.Errorf("binding key interpolation error: %w", err)
	}
	bindingKey = strings.ReplaceAll(bindingKey, "/", ".")

	msgType, err := a.msgType.TryString(msg)
	if err != nil {
		return fmt.Errorf("msg type interpolation error: %w", err)
	}
	msgType = strings.ReplaceAll(msgType, "/", ".")

	contentType, err := a.contentType.TryString(msg)
	if err != nil {
		return fmt.Errorf("content type interpolation error: %w", err)
	}
	contentEncoding, err := a.contentEncoding.TryString(msg)
	if err != nil {
		return fmt.Errorf("content encoding interpolation error: %w", err)
	}

	priorityString, err := a.priority.TryString(msg)
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
			return fmt.Errorf("invalid priority parsed from expression, must be <= 9 and >= 0, got %d", priorityInt)
		}
		priority = uint8(priorityInt)
	}

	correlationID, err := a.correlationID.TryString(msg)
	if err != nil {
		return fmt.Errorf("correlation ID interpolation error: %w", err)
	}

	replyTo, err := a.replyTo.TryString(msg)
	if err != nil {
		return fmt.Errorf("reply to interpolation error: %w", err)
	}

	expiration, err := a.expiration.TryString(msg)
	if err != nil {
		return fmt.Errorf("expiration interpolation error: %w", err)
	}

	messageID, err := a.messageID.TryString(msg)
	if err != nil {
		return fmt.Errorf("message ID interpolation error: %w", err)
	}

	userID, err := a.userID.TryString(msg)
	if err != nil {
		return fmt.Errorf("user ID interpolation error: %w", err)
	}

	appID, err := a.appID.TryString(msg)
	if err != nil {
		return fmt.Errorf("app ID interpolation error: %w", err)
	}
	headers := amqp.Table{}
	_ = a.metaFilter.WalkMut(msg, func(k string, v any) error {
		headers[strings.ReplaceAll(k, "_", "-")] = v
		return nil
	})

	exchange, err := a.exchange.TryString(msg)
	if err != nil {
		return fmt.Errorf("exchange name interpolation error: %w", err)
	}
	if err := a.declareExchange(exchange); err != nil {
		return fmt.Errorf("amqp failed to declare exchange: %w", err)
	}

	conf, err := amqpChan.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,    // publish to an exchange
		bindingKey,  // routing to 0 or more queues
		a.mandatory, // mandatory
		a.immediate, // immediate
		amqp.Publishing{
			Headers:         headers,
			ContentType:     contentType,
			ContentEncoding: contentEncoding,
			Body:            msgBytes,
			DeliveryMode:    a.deliveryMode, // 1=non-persistent, 2=persistent
			Priority:        priority,       // 0-9
			Type:            msgType,
			CorrelationId:   correlationID,
			ReplyTo:         replyTo,
			Expiration:      expiration,
			MessageId:       messageID,
			AppId:           appID,
			UserId:          userID,
			// a bunch of application/implementation-specific fields
		},
	)
	if err != nil {
		_ = a.disconnect()
		a.log.Errorf("Failed to send message: %w", err)
		return service.ErrNotConnected
	}
	if !conf.Wait() {
		a.log.Error("Failed to acknowledge message.")
		return component.ErrNoAck
	}
	if returnChan != nil {
		select {
		case _, open := <-returnChan:
			if !open {
				return errors.New("acknowledgement not supported, ensure server supports immediate and mandatory flags")
			}
			return component.ErrNoAck
		default:
		}
	}
	return nil
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
