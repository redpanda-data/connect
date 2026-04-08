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

package amqp1

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type amqpContentType string

const (
	// Data section with opaque binary data
	amqpContentTypeOpaqueBinary amqpContentType = "opaque_binary"
	// Single AMQP string value
	amqpContentTypeString amqpContentType = "string"
)

func amqp1OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Sends messages to an AMQP (1.0) server.").
		Description(`
== Metadata

Message metadata is added to each AMQP message as string annotations. In order to control which metadata keys are added use the `+"`metadata`"+` config field.

== Performance

This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field `+"`max_in_flight`"+`.`).
		Fields(
			service.NewURLField(urlField).
				Description("A URL to connect to.").
				Example("amqp://localhost:5672/").
				Example("amqps://guest:guest@localhost:5672/").
				Deprecated().
				Optional(),
			service.NewURLListField(urlsField).
				Description("A list of URLs to connect to. The first URL to successfully establish a connection will be used until the connection is closed. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"amqp://guest:guest@127.0.0.1:5672/"}).
				Example([]string{"amqp://127.0.0.1:5672/,amqp://127.0.0.2:5672/"}).
				Example([]string{"amqp://127.0.0.1:5672/", "amqp://127.0.0.2:5672/"}).
				Optional().
				Version("4.23.0"),
			service.NewStringField(targetAddrField).
				Description("The target address to write to. When left empty, the output uses the Anonymous Terminus pattern where the destination is specified per-message using `message_properties_to`.").
				Default("").
				Example("/foo").
				Example("queue:/bar").
				Example("topic:/baz").
				Example(""),
			service.NewOutputMaxInFlightField(),
			service.NewTLSToggledField(tlsField),
			service.NewBloblangField(appPropsMapField).
				Description("An optional Bloblang mapping that can be defined in order to set the `application-properties` on output messages.").
				Optional().
				Advanced(),
			saslFieldSpec(),
			service.NewMetadataExcludeFilterField(metaFilterField).
				Description("Specify criteria for which metadata values are attached to messages as headers."),
			service.NewStringEnumField(contentTypeField,
				string(amqpContentTypeOpaqueBinary), string(amqpContentTypeString)).
				Description("Specify the message body content type. The option `string` will transfer the message as an AMQP value of type string. Consider choosing the option `string` if your intention is to transfer UTF-8 string messages (like JSON messages) to the destination.").
				Advanced().
				Default(string(amqpContentTypeOpaqueBinary)),
			service.NewBoolField(persistentField).
				Description("If set to true, the message will be marked as persistent, ensuring it is stored durably and not lost if an intermediary (such as a broker) restarts. By default, messages are not durable.").
				Advanced().
				Default(false),
			service.NewStringListField(targetCapsField).
				Description("Lists the extension capabilities the sender desires from the target, such as support for queues, topics, durability, sharing, or temporary destinations.").
				Optional().
				Advanced().
				Example([]string{"queue"}).
				Example([]string{"topic"}).
				Example([]string{"queue", "topic"}),
			service.NewInterpolatedStringField(messagePropsTo).
				Description("The field specifies the node that is the intended destination of the message, which may differ from the node currently receiving the transfer. This field supports Bloblang interpolation.").
				Optional().
				Advanced().
				Example("amqp://localhost:5672/").
				Example(`${! meta("target_address") }`),
			service.NewInterpolatedStringField(messagePropsMsgID).
				Description("Set the message-id property on outgoing AMQP messages. The value is auto-detected as UUID, uint64, or string. Purely numeric values are sent as uint64 on the wire. This field supports Bloblang interpolation.").
				Optional().
				Advanced().
				Example(`${! uuid_v4() }`).
				Example(`${! meta("amqp_message_id") }`),
			service.NewInterpolatedStringField(messagePropsCorrelID).
				Description("Set the correlation-id property on outgoing AMQP messages. The value is auto-detected as UUID, uint64, or string. Purely numeric values are sent as uint64 on the wire. This field supports Bloblang interpolation.").
				Optional().
				Advanced().
				Example(`${! meta("amqp_correlation_id") }`),
			service.NewInterpolatedStringField(messagePropsSubject).
				Description("Set the subject property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(messagePropsReplyTo).
				Description("Set the reply-to property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(messagePropsGroupID).
				Description("Set the group-id property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(messagePropsGroupSeq).
				Description("Set the group-sequence property on outgoing AMQP messages. Must be a valid uint32 value. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(messagePropsReplyToGrpID).
				Description("Set the reply-to-group-id property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(messagePropsUserID).
				Description("Set the user-id property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
			service.NewInterpolatedStringField(messagePropsContentType).
				Description("Set the content-type property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced().
				Example("application/json").
				Example("text/plain; charset=utf-8"),
			service.NewInterpolatedStringField(messagePropsContentEnc).
				Description("Set the content-encoding property on outgoing AMQP messages. This field supports Bloblang interpolation.").
				Optional().
				Advanced(),
		).LintRule(`
root = if this.url.or("") == "" && this.urls.or([]).length() == 0 {
  "field 'urls' must be set"
} else if this.target_address.or("") == "" && !this.exists("message_properties_to") {
  "when 'target_address' is empty, 'message_properties_to' must be set to specify per-message destinations"
}
`)
}

func init() {
	service.MustRegisterOutput("amqp_1", amqp1OutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := amqp1WriterFromParsed(conf, mgr)
			if err != nil {
				return nil, 0, err
			}

			mIF, err := conf.FieldMaxInFlight()
			if err != nil {
				return nil, 0, err
			}

			return w, mIF, nil
		})
}

type amqp1Writer struct {
	client  *amqp.Conn
	session *amqp.Session
	sender  *amqp.Sender

	urls                     []string
	targetAddr               string
	metaFilter               *service.MetadataExcludeFilter
	applicationPropertiesMap *bloblang.Executor
	connOpts                 *amqp.ConnOptions
	contentType              amqpContentType
	senderOpts               *amqp.SenderOptions
	persistent               bool
	msgTo                    *service.InterpolatedString
	msgMessageID             *service.InterpolatedString
	msgCorrelationID         *service.InterpolatedString
	msgSubject               *service.InterpolatedString
	msgReplyTo               *service.InterpolatedString
	msgGroupID               *service.InterpolatedString
	msgGroupSequence         *service.InterpolatedString
	msgReplyToGroupID        *service.InterpolatedString
	msgUserID                *service.InterpolatedString
	msgContentType           *service.InterpolatedString
	msgContentEncoding       *service.InterpolatedString

	log      *service.Logger
	connLock sync.RWMutex
}

func amqp1WriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*amqp1Writer, error) {
	a := amqp1Writer{
		connOpts:   &amqp.ConnOptions{},
		senderOpts: &amqp.SenderOptions{},
		log:        mgr.Logger(),
	}

	urlStrs, err := conf.FieldStringList(urlsField)
	if err != nil {
		return nil, err
	}

	for _, u := range urlStrs {
		for splitURL := range strings.SplitSeq(u, ",") {
			if trimmed := strings.TrimSpace(splitURL); trimmed != "" {
				a.urls = append(a.urls, trimmed)
			}
		}
	}

	if len(a.urls) == 0 {
		singleURL, err := conf.FieldString(urlField)
		if err != nil {
			err = errors.New("at least one url must be specified")
			return nil, err
		}

		a.urls = []string{singleURL}
	}

	if a.targetAddr, err = conf.FieldString(targetAddrField); err != nil {
		return nil, err
	}

	if err := saslOptFnsFromParsed(conf, a.connOpts); err != nil {
		return nil, err
	}

	tlsConf, enabled, err := conf.FieldTLSToggled(tlsField)
	if err != nil {
		return nil, err
	}
	if enabled {
		a.connOpts.TLSConfig = tlsConf
	}

	if conf.Contains(appPropsMapField) {
		if a.applicationPropertiesMap, err = conf.FieldBloblang(appPropsMapField); err != nil {
			return nil, err
		}
	}

	if a.metaFilter, err = conf.FieldMetadataExcludeFilter(metaFilterField); err != nil {
		return nil, err
	}

	if contentType, err := conf.FieldString(contentTypeField); err != nil {
		return nil, err
	} else {
		a.contentType = amqpContentType(contentType)
	}

	if a.persistent, err = conf.FieldBool(persistentField); err != nil {
		return nil, err
	}

	var targetCaps []string
	targetCaps, err = conf.FieldStringList(targetCapsField)
	if err != nil {
		return nil, err
	}
	if len(targetCaps) != 0 {
		a.senderOpts.TargetCapabilities = targetCaps
	}

	if conf.Contains(messagePropsTo) {
		if a.msgTo, err = conf.FieldInterpolatedString(messagePropsTo); err != nil {
			return nil, err
		}
	}

	for _, f := range []struct {
		name   string
		target **service.InterpolatedString
	}{
		{messagePropsMsgID, &a.msgMessageID},
		{messagePropsCorrelID, &a.msgCorrelationID},
		{messagePropsSubject, &a.msgSubject},
		{messagePropsReplyTo, &a.msgReplyTo},
		{messagePropsGroupID, &a.msgGroupID},
		{messagePropsGroupSeq, &a.msgGroupSequence},
		{messagePropsReplyToGrpID, &a.msgReplyToGroupID},
		{messagePropsUserID, &a.msgUserID},
		{messagePropsContentType, &a.msgContentType},
		{messagePropsContentEnc, &a.msgContentEncoding},
	} {
		if conf.Contains(f.name) {
			if *f.target, err = conf.FieldInterpolatedString(f.name); err != nil {
				return nil, err
			}
		}
	}

	return &a, nil
}

func (a *amqp1Writer) Connect(ctx context.Context) (err error) {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	if a.client != nil {
		return err
	}

	var (
		client  *amqp.Conn
		session *amqp.Session
		sender  *amqp.Sender
	)

	// Create client
	if client, err = a.reDial(ctx, a.urls); err != nil {
		return err
	}

	// Open a session
	if session, err = client.NewSession(ctx, nil); err != nil {
		_ = client.Close()
		return err
	}

	// Create a sender
	// When targetAddr is empty (""), this creates an anonymous terminus pattern
	// where the destination is specified per-message via message.Properties.To.
	// Note: go-amqp v1.5.0 creates an omitted target address rather than an
	// explicit null target as specified in AMQP 1.0 spec section 2.6.12.
	// Most mainstream brokers (ActiveMQ, Azure Service Bus) accept both forms.
	if sender, err = session.NewSender(ctx, a.targetAddr, a.senderOpts); err != nil {
		_ = session.Close(ctx)
		_ = client.Close()
		return err
	}

	a.client = client
	a.session = session
	a.sender = sender
	return nil
}

func (a *amqp1Writer) disconnect(ctx context.Context) error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	if a.client == nil {
		return nil
	}

	if err := a.sender.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close sender: %v\n", err)
	}
	if err := a.session.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close session: %v\n", err)
	}
	if err := a.client.Close(); err != nil {
		a.log.Errorf("Failed to cleanly close client: %v\n", err)
	}
	a.client = nil
	a.session = nil
	a.sender = nil

	return nil
}

//------------------------------------------------------------------------------

func (a *amqp1Writer) Write(ctx context.Context, msg *service.Message) error {
	var s *amqp.Sender
	a.connLock.RLock()
	if a.sender != nil {
		s = a.sender
	}
	a.connLock.RUnlock()

	if s == nil {
		return service.ErrNotConnected
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	var m *amqp.Message
	switch a.contentType {
	case amqpContentTypeOpaqueBinary:
		m = amqp.NewMessage(mBytes)
	case amqpContentTypeString:
		m = &amqp.Message{}
		m.Value = string(mBytes)
	default:
		return fmt.Errorf("invalid content type specified: %s", a.contentType)
	}

	if a.persistent {
		m.Header = &amqp.MessageHeader{Durable: true}
	}

	if err := a.setMessageProperties(m, msg); err != nil {
		return err
	}

	if a.applicationPropertiesMap != nil {
		mapMsg, err := msg.BloblangQuery(a.applicationPropertiesMap)
		if err != nil {
			return err
		}

		var mapVal any
		if mapMsg != nil {
			if mapVal, err = mapMsg.AsStructured(); err != nil {
				return err
			}
		}

		if mapVal != nil {
			applicationProperties, ok := mapVal.(map[string]any)
			if !ok {
				return fmt.Errorf("application_properties_map resulted in a non-object mapping: %T", mapVal)
			}
			m.ApplicationProperties = applicationProperties
		}
	}

	_ = a.metaFilter.WalkMut(msg, func(k string, v any) error {
		if m.Annotations == nil {
			m.Annotations = amqp.Annotations{}
		}
		m.Annotations[k] = v
		return nil
	})

	if err = s.Send(ctx, m, nil); err != nil {
		if ctx.Err() == nil {
			a.log.Errorf("Lost connection due to: %v\n", err)
			_ = a.disconnect(ctx)
			err = service.ErrNotConnected
		}
	}
	return err
}

func (a *amqp1Writer) setMessageProperties(m *amqp.Message, msg *service.Message) error {
	if err := setInterpolatedStringProp(m, msg, a.msgTo, messagePropsTo, func(p *amqp.MessageProperties, v string) { p.To = &v }); err != nil {
		return err
	}
	if err := setInterpolatedIDProp(m, msg, a.msgMessageID, messagePropsMsgID, func(p *amqp.MessageProperties, v any) { p.MessageID = v }); err != nil {
		return err
	}
	if err := setInterpolatedIDProp(m, msg, a.msgCorrelationID, messagePropsCorrelID, func(p *amqp.MessageProperties, v any) { p.CorrelationID = v }); err != nil {
		return err
	}
	if err := setInterpolatedStringProp(m, msg, a.msgSubject, messagePropsSubject, func(p *amqp.MessageProperties, v string) { p.Subject = &v }); err != nil {
		return err
	}
	if err := setInterpolatedStringProp(m, msg, a.msgReplyTo, messagePropsReplyTo, func(p *amqp.MessageProperties, v string) { p.ReplyTo = &v }); err != nil {
		return err
	}
	if err := setInterpolatedStringProp(m, msg, a.msgGroupID, messagePropsGroupID, func(p *amqp.MessageProperties, v string) { p.GroupID = &v }); err != nil {
		return err
	}
	if err := setInterpolatedStringProp(m, msg, a.msgReplyToGroupID, messagePropsReplyToGrpID, func(p *amqp.MessageProperties, v string) { p.ReplyToGroupID = &v }); err != nil {
		return err
	}

	if a.msgGroupSequence != nil {
		v, err := a.msgGroupSequence.TryString(msg)
		if err != nil {
			return fmt.Errorf("interpolating %s: %w", messagePropsGroupSeq, err)
		}
		if v != "" {
			n, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return fmt.Errorf("parsing %s as uint32: %w", messagePropsGroupSeq, err)
			}
			if m.Properties == nil {
				m.Properties = &amqp.MessageProperties{}
			}
			gs := uint32(n)
			m.Properties.GroupSequence = &gs
		}
	}

	if err := setInterpolatedStringProp(m, msg, a.msgUserID, messagePropsUserID, func(p *amqp.MessageProperties, v string) { p.UserID = []byte(v) }); err != nil {
		return err
	}
	if err := setInterpolatedStringProp(m, msg, a.msgContentType, messagePropsContentType, func(p *amqp.MessageProperties, v string) { p.ContentType = &v }); err != nil {
		return err
	}
	if err := setInterpolatedStringProp(m, msg, a.msgContentEncoding, messagePropsContentEnc, func(p *amqp.MessageProperties, v string) { p.ContentEncoding = &v }); err != nil {
		return err
	}

	return nil
}

func setInterpolatedStringProp(m *amqp.Message, msg *service.Message, interp *service.InterpolatedString, fieldName string, set func(*amqp.MessageProperties, string)) error {
	if interp == nil {
		return nil
	}
	v, err := interp.TryString(msg)
	if err != nil {
		return fmt.Errorf("interpolating %s: %w", fieldName, err)
	}
	if v != "" {
		if m.Properties == nil {
			m.Properties = &amqp.MessageProperties{}
		}
		set(m.Properties, v)
	}
	return nil
}

func setInterpolatedIDProp(m *amqp.Message, msg *service.Message, interp *service.InterpolatedString, fieldName string, set func(*amqp.MessageProperties, any)) error {
	if interp == nil {
		return nil
	}
	v, err := interp.TryString(msg)
	if err != nil {
		return fmt.Errorf("interpolating %s: %w", fieldName, err)
	}
	if v != "" {
		if m.Properties == nil {
			m.Properties = &amqp.MessageProperties{}
		}
		set(m.Properties, parseAMQPID(v))
	}
	return nil
}

// parseAMQPID auto-detects the type of an AMQP message-id or correlation-id
// value. It tries UUID format first, then uint64, and falls back to string.
func parseAMQPID(s string) any {
	if u, err := uuid.Parse(s); err == nil {
		return amqp.UUID(u)
	}
	if n, err := strconv.ParseUint(s, 10, 64); err == nil {
		return n
	}
	return s
}

func (a *amqp1Writer) Close(ctx context.Context) error {
	return a.disconnect(ctx)
}

// reDial connection to amqp with one or more fallback URLs.
func (a *amqp1Writer) reDial(ctx context.Context, urls []string) (conn *amqp.Conn, err error) {
	for i, url := range urls {
		conn, err = amqp.Dial(ctx, url, a.connOpts)
		if err != nil {
			a.log.With("error", err).Warnf("unable to connect to url %q #%d, trying next", url, i)

			continue
		}

		a.log.Tracef("successful connection to use %q #%d", url, i)

		return conn, nil
	}

	a.log.With("error", err).Tracef("unable to connect to any of %d urls, return error", len(a.urls))

	return nil, err
}
