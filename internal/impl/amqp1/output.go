package amqp1

import (
	"context"
	"fmt"
	"sync"

	"github.com/Azure/go-amqp"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func amqp1OutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Sends messages to an AMQP (1.0) server.").
		Description(`
### Metadata

Message metadata is added to each AMQP message as string annotations. In order to control which metadata keys are added use the `+"`metadata`"+` config field.

## Performance

This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field `+"`max_in_flight`"+`.`).
		Fields(
			service.NewURLField(urlField).
				Description("A URL to connect to.").
				Example("amqp://localhost:5672/").
				Example("amqps://guest:guest@localhost:5672/"),
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
		)
}

func init() {
	err := service.RegisterOutput("amqp_1", amqp1OutputSpec(),
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
	if err != nil {
		panic(err)
	}
}

type amqp1Writer struct {
	client  *amqp.Conn
	session *amqp.Session
	sender  *amqp.Sender

	url                      string
	targetAddr               string
	metaFilter               *service.MetadataExcludeFilter
	applicationPropertiesMap *bloblang.Executor
	connOpts                 *amqp.ConnOptions

	log      *service.Logger
	connLock sync.RWMutex
}

func amqp1WriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*amqp1Writer, error) {
	a := amqp1Writer{
		log:      mgr.Logger(),
		connOpts: &amqp.ConnOptions{},
	}

	var err error
	if a.url, err = conf.FieldString(urlField); err != nil {
		return nil, err
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
	if client, err = amqp.Dial(ctx, a.url, a.connOpts); err != nil {
		return
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

	a.log.Infof("Sending AMQP 1.0 messages to target: %v\n", a.targetAddr)
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

	m := amqp.NewMessage(mBytes)

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
		if ctx.Err() != nil {
			err = component.ErrTimeout
		} else {
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
