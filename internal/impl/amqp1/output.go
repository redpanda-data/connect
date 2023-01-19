package amqp1

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/Azure/go-amqp"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/amqp1/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	itls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		a, err := newAMQP1Writer(c.AMQP1, nm)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("amqp_1", c.AMQP1.MaxInFlight, a, nm)
		if err != nil {
			return nil, err
		}
		return output.OnlySinglePayloads(w), nil
	}), docs.ComponentSpec{
		Name:    "amqp_1",
		Status:  docs.StatusStable,
		Summary: `Sends messages to an AMQP (1.0) server.`,
		Description: output.Description(true, false, `
### Metadata

Message metadata is added to each AMQP message as string annotations. In order to control which metadata keys are added use the `+"`metadata`"+` config field.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url",
				"A URL to connect to.",
				"amqp://localhost:5672/",
				"amqps://guest:guest@localhost:5672/",
			).HasDefault(""),
			docs.FieldString("target_address", "The target address to write to.", "/foo", "queue:/bar", "topic:/baz").HasDefault(""),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").HasDefault(64),
			itls.FieldSpec(),
			docs.FieldBloblang("application_properties_map", "An optional Bloblang mapping that can be defined in order to set the `application-properties` on output messages.").Advanced().HasDefault(""),
			shared.SASLFieldSpec(),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are attached to messages as headers.").
				WithChildren(metadata.ExcludeFilterFields()...),
		),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

type amqp1Writer struct {
	client  *amqp.Client
	session *amqp.Session
	sender  *amqp.Sender

	metaFilter               *metadata.ExcludeFilter
	applicationPropertiesMap *mapping.Executor

	log log.Modular

	conf    output.AMQP1Config
	tlsConf *tls.Config

	connLock sync.RWMutex
}

func newAMQP1Writer(conf output.AMQP1Config, mgr bundle.NewManagement) (*amqp1Writer, error) {
	a := amqp1Writer{
		log:  mgr.Logger(),
		conf: conf,
	}

	var err error
	if conf.ApplicationPropertiesMapping != "" {
		if a.applicationPropertiesMap, err = mgr.BloblEnvironment().NewMapping(conf.ApplicationPropertiesMapping); err != nil {
			return nil, fmt.Errorf("failed to construct application_properties_map: %w", err)
		}
	}

	if conf.TLS.Enabled {
		if a.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}
	if a.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}
	return &a, nil
}

func (a *amqp1Writer) Connect(ctx context.Context) error {
	a.connLock.Lock()
	defer a.connLock.Unlock()

	if a.client != nil {
		return nil
	}

	var (
		client  *amqp.Client
		session *amqp.Session
		sender  *amqp.Sender
		err     error
	)

	opts, err := saslToOptFns(a.conf.SASL)
	if err != nil {
		return err
	}
	if a.conf.TLS.Enabled {
		opts = append(opts, amqp.ConnTLS(true), amqp.ConnTLSConfig(a.tlsConf))
	}

	// Create client
	if client, err = amqp.Dial(a.conf.URL, opts...); err != nil {
		return err
	}

	// Open a session
	if session, err = client.NewSession(); err != nil {
		client.Close()
		return err
	}

	// Create a sender
	if sender, err = session.NewSender(
		amqp.LinkTargetAddress(a.conf.TargetAddress),
	); err != nil {
		session.Close(context.Background())
		client.Close()
		return err
	}

	a.client = client
	a.session = session
	a.sender = sender

	a.log.Infof("Sending AMQP 1.0 messages to target: %v\n", a.conf.TargetAddress)
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

func (a *amqp1Writer) WriteBatch(ctx context.Context, msg message.Batch) error {
	var s *amqp.Sender
	a.connLock.RLock()
	if a.sender != nil {
		s = a.sender
	}
	a.connLock.RUnlock()

	if s == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		m := amqp.NewMessage(p.AsBytes())

		if a.applicationPropertiesMap != nil {
			mapMsg, err := a.applicationPropertiesMap.MapPart(i, msg)
			if err != nil {
				return err
			}

			mapVal, err := mapMsg.AsStructured()
			if err != nil {
				return err
			}

			applicationProperties, ok := mapVal.(map[string]interface{})
			if !ok {
				return fmt.Errorf("application_properties_map resulted in a non-object mapping: %T", mapVal)
			}

			m.ApplicationProperties = applicationProperties
		}
		_ = a.metaFilter.Iter(p, func(k string, v any) error {
			if m.Annotations == nil {
				m.Annotations = amqp.Annotations{}
			}
			m.Annotations[k] = v
			return nil
		})
		err := s.Send(ctx, m)
		if err != nil {
			if err == amqp.ErrTimeout || ctx.Err() != nil {
				err = component.ErrTimeout
			} else {
				if dErr, isDetachError := err.(*amqp.DetachError); isDetachError && dErr.RemoteError != nil {
					a.log.Errorf("Lost connection due to: %v\n", dErr.RemoteError)
				} else {
					a.log.Errorf("Lost connection due to: %v\n", err)
				}
				_ = a.disconnect(ctx)
				err = component.ErrNotConnected
			}
		}
		return err
	})
}

func (a *amqp1Writer) Close(ctx context.Context) error {
	return a.disconnect(ctx)
}
