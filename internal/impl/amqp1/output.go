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
	"strings"
	"sync"

	"github.com/Azure/go-amqp"

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
				Description("The target address to write to.").
				Example("/foo").
				Example("queue:/bar").
				Example("topic:/baz"),
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
		).LintRule(`
root = if this.url.or("") == "" && this.urls.or([]).length() == 0 {
  "field 'urls' must be set"
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

	log      *service.Logger
	connLock sync.RWMutex
}

func amqp1WriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*amqp1Writer, error) {
	a := amqp1Writer{
		log:      mgr.Logger(),
		connOpts: &amqp.ConnOptions{},
	}

	urlStrs, err := conf.FieldStringList(urlsField)
	if err != nil {
		return nil, err
	}

	for _, u := range urlStrs {
		for _, splitURL := range strings.Split(u, ",") {
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

	return &a, nil
}

func (a *amqp1Writer) Connect(ctx context.Context) (err error) {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	if a.client != nil {
		return
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
		return
	}

	// Create a sender
	if sender, err = session.NewSender(ctx, a.targetAddr, nil); err != nil {
		_ = session.Close(ctx)
		_ = client.Close()
		return
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
			applicationProperties, ok := mapVal.(map[string]interface{})
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
