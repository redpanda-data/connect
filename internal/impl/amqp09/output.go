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

	"github.com/redpanda-data/benthos/v4/public/service"
)

func amqp09OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Stable().
		Summary(`Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by various message brokers, including RabbitMQ.Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various message brokers, including RabbitMQ.`).
		Description(`The metadata from each message are delivered as headers.

It's possible for this output type to create the target exchange by setting `+"`exchange_declare.enabled` to `true`"+`, if the exchange already exists then the declaration passively verifies that the settings match.

TLS is automatic when connecting to an `+"`amqps`"+` URL, but custom settings can be enabled in the `+"`tls`"+` section.

The fields 'key', 'exchange' and 'type' can be dynamically set using xref:configuration:interpolation.adoc#bloblang-queries[function interpolations].`).
		Fields(
			urlsFieldSpec(),
			service.NewInterpolatedStringField(exchangeField).
				Description("The AMQP exchange to publish messages to."),
			service.NewObjectField(exchangeDeclareField,
				service.NewBoolField(exchangeDeclareEnabledField).
					Description("Whether to enable exchange declaration.").
					Default(false),
				service.NewStringEnumField(exchangeDeclareTypeField, "direct", "fanout", "topic", "headers", "x-custom").
					Description(`The type of the exchange, which determines how messages are routed to queues. For `+"`"+`topic`+"`"+` exchanges, routing keys are matched as lists of words separated by dots (`+"`"+`.`+"`"+`).`).
					Default("direct"),
				service.NewBoolField(exchangeDeclareDurableField).
					Description("Whether the declared exchange is durable.").
					Default(true),
				service.NewStringMapField(exchangeDeclareArgumentsField).
					Description("Arguments for server-specific implementations of the exchange (optional). You can use arguments to configure additional parameters for exchange types that require them.").
					ShortDescription("Optional arguments specific to the server's exchange implementation, for types needing extra parameters.").
					Advanced().
					Optional().
					Example(map[string]any{
						"alternate-exchange": "my-ae",
					}),
			).
				Description(`Declares the target exchange (`+"`"+`exchange`+"`"+`) to check whether an exchange with the specified name exists and is configured correctly. If the exchange exists, the declaration verifies that the fields specified in this object match its properties. If the target exchange does not exist, this output creates it.`).
				Advanced().
				Optional(),
			service.NewInterpolatedStringField(keyField).
				Description("The binding key to set for each message.").
				Default(""),
			service.NewInterpolatedStringField(typeField).
				Description("A custom message type to set for each message.").
				Default(""),
			service.NewInterpolatedStringField(contentTypeField).
				Description("The MIME type of each message.").
				Advanced().
				Default("application/octet-stream"),
			service.NewInterpolatedStringField(contentEncodingField).
				Description("The content encoding attribute of each message.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(correlationIDField).
				Description("Set a unique correlation ID for each message using a dynamic interpolated expression to help match messages to responses.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(replyToField).
				Description("Set the name of the queue to which responses are sent using a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(expirationField).
				Description("Set the TTL of each message in milliseconds.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(messageIDField).
				Description("Set a message ID for each message using a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(userIDField).
				Description("Set the user ID to the name of the publisher. If this property is set by a publisher, its value must match the name of the user that opened the connection.").
				ShortDescription("The user ID of the publisher. Must equal the user that opened the connection.").
				Advanced().
				Default(""),
			service.NewInterpolatedStringField(appIDField).
				Description("Set an application ID for each message using a dynamic interpolated expression.").
				Advanced().
				Default(""),
			service.NewMetadataExcludeFilterField(metadataFilterField).
				Description("Configure which metadata values are added to messages as headers. This allows you to pass additional context information along with your messages."),
			service.NewInterpolatedStringField(priorityField).
				Description("Set the priority of each message using a dynamic interpolated expression.").
				Advanced().
				Example("0").
				Example(`${! meta("amqp_priority") }`).
				Example(`${! json("doc.priority") }`).
				Default(""),
			service.NewOutputMaxInFlightField(),
			service.NewBoolField(persistentField).
				Description("Whether to store delivered messages on disk. By default, message delivery is transient.").
				Advanced().
				Default(false),
			service.NewBoolField(mandatoryField).
				Description("Whether to set the mandatory flag on published messages. When set to `true`, a published message that cannot be routed to any queues is returned to the sender.").
				ShortDescription("Set the mandatory flag, returning messages that route to zero queues.").
				Advanced().
				Default(false),
			service.NewBoolField(immediateField).
				Description("Whether to set the immediate flag on published messages. When set to `true`, if there are no active consumers for a queue, the message is dropped instead of waiting.").
				ShortDescription("Set the immediate flag, dropping messages when a queue has no ready consumers.").
				Advanced().
				Default(false),
			service.NewDurationField(timeoutField).
				Description("The maximum period to wait for a message acknowledgment before abandoning it and attempting a resend. If this value is not set, the system waits indefinitely.").
				Advanced().
				Default(""),
			service.NewTLSToggledField(tlsField),
		)
}

func init() {
	service.MustRegisterOutput("amqp_0_9", amqp09OutputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		maxInFlight, err := conf.FieldMaxInFlight()
		if err != nil {
			return nil, 0, err
		}
		w, err := amqp09WriterFromParsed(conf, mgr)
		return w, maxInFlight, err
	})
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
	exchangeDeclareArgs    amqp.Table

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
		for splitURL := range strings.SplitSeq(u, ",") {
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

		if edConf.Contains(exchangeDeclareArgumentsField) {
			args, err := edConf.FieldStringMap(exchangeDeclareArgumentsField)
			if err != nil {
				return nil, err
			}
			for key, value := range args {
				a.exchangeDeclareArgs[key] = value
			}
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

// ConnectionTest attempts to test the connection configuration of this output
// without actually sending data. The connection, if successful, is then
// closed.
func (a *amqp09Writer) ConnectionTest(_ context.Context) service.ConnectionTestResults {
	conn, err := a.reDial(a.urls)
	if err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}
	defer conn.Close()

	amqpChan, err := conn.Channel()
	if err != nil {
		return service.ConnectionTestFailed(fmt.Errorf("amqp creating channel: %w", err)).AsList()
	}
	defer amqpChan.Close()

	return service.ConnectionTestSucceeded().AsList()
}

func (a *amqp09Writer) Connect(context.Context) error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	conn, err := a.reDial(a.urls)
	if err != nil {
		return err
	}

	var amqpChan *amqp.Channel
	if amqpChan, err = conn.Channel(); err != nil {
		conn.Close()
		return fmt.Errorf("amqp creating channel: %w", err)
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
			a.log.Errorf("Failed to declare exchange: %v", err)
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
			a.log.Errorf("Failed to close connection cleanly: %v", err)
		}
		a.conn = nil
	}
	return nil
}

// declareExchange declare and memoize the declaration of an AMQP exchange.
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
		a.exchangeDeclareArgs,    // arguments
	); err != nil {
		return fmt.Errorf("declaring amqp exchange: %w", err)
	}
	a.exchangesDeclared[exchange] = struct{}{}
	return nil
}

var errNoAck = errors.New("receiving acknowledgement")

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
	if a.exchangeDeclareType == "topic" {
		bindingKey = strings.ReplaceAll(bindingKey, "/", ".")
	}

	msgType, err := a.msgType.TryString(msg)
	if err != nil {
		return fmt.Errorf("msg type interpolation error: %w", err)
	}
	if a.exchangeDeclareType == "topic" {
		msgType = strings.ReplaceAll(msgType, "/", ".")
	}

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
			return fmt.Errorf("parsing valid integer from priority expression: %w", err)
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
		return fmt.Errorf("declaring amqp exchange: %w", err)
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
		a.log.Errorf("Failed to send message: %v", err)
		return service.ErrNotConnected
	}
	if !conf.Wait() {
		a.log.Error("Failed to acknowledge message.")
		return errNoAck
	}
	if returnChan != nil {
		select {
		case _, open := <-returnChan:
			if !open {
				return errors.New("acknowledgement not supported, ensure server supports immediate and mandatory flags")
			}
			return errNoAck
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
