package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newNATSOutput), docs.ComponentSpec{
		Name:    "nats",
		Summary: `Publish to an NATS subject.`,
		Description: output.Description(true, false, `
This output will interpolate functions within the subject field, you can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).

`+auth.Description()),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldString("subject", "The subject to publish to.").IsInterpolated(),
			docs.FieldString("headers", "Explicit message headers to add to messages.",
				map[string]string{
					"Content-Type": "application/json",
					"Timestamp":    `${!meta("Timestamp")}`,
				},
			).IsInterpolated().Map(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			btls.FieldSpec(),
			auth.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewNATSConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNATSOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newNATSWriter(conf.NATS, mgr, mgr.Logger())
	if err != nil {
		return nil, err
	}
	return output.NewAsyncWriter("nats", conf.NATS.MaxInFlight, w, mgr)
}

type natsWriter struct {
	log log.Modular

	natsConn *nats.Conn
	connMut  sync.RWMutex

	urls       string
	conf       output.NATSConfig
	headers    map[string]*field.Expression
	subjectStr *field.Expression
	tlsConf    *tls.Config
}

func newNATSWriter(conf output.NATSConfig, mgr bundle.NewManagement, log log.Modular) (*natsWriter, error) {
	n := natsWriter{
		log:     log,
		conf:    conf,
		headers: make(map[string]*field.Expression),
	}
	var err error
	if n.subjectStr, err = mgr.BloblEnvironment().NewField(conf.Subject); err != nil {
		return nil, fmt.Errorf("failed to parse subject expression: %v", err)
	}
	for k, v := range conf.Headers {
		if n.headers[k], err = mgr.BloblEnvironment().NewField(v); err != nil {
			return nil, fmt.Errorf("failed to parse header '%s' expresion: %v", k, err)
		}
	}
	n.urls = strings.Join(conf.URLs, ",")

	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}

	return &n, nil
}

func (n *natsWriter) Connect(ctx context.Context) error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var err error
	var opts []nats.Option

	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, authConfToOptions(n.conf.Auth)...)

	if n.natsConn, err = nats.Connect(n.urls, opts...); err != nil {
		return err
	}

	if err == nil {
		n.log.Infof("Sending NATS messages to subject: %v\n", n.conf.Subject)
	}
	return err
}

// WriteBatch attempts to write a message.
func (n *natsWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	return n.Write(msg)
}

// Write attempts to write a message.
func (n *natsWriter) Write(msg message.Batch) error {
	n.connMut.RLock()
	conn := n.natsConn
	n.connMut.RUnlock()

	if conn == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		subject, err := n.subjectStr.String(i, msg)
		if err != nil {
			return fmt.Errorf("subject interpolation error: %w", err)
		}

		n.log.Debugf("Writing NATS message to topic %s", subject)
		// fill message data
		nMsg := nats.NewMsg(subject)
		nMsg.Data = p.AsBytes()
		if conn.HeadersSupported() {
			// fill bloblang headers
			for k, v := range n.headers {
				headerStr, err := v.String(i, msg)
				if err != nil {
					return fmt.Errorf("header %v interpolation error: %w", k, err)
				}
				nMsg.Header.Add(k, headerStr)
			}
		}

		if err = conn.PublishMsg(nMsg); errors.Is(err, nats.ErrConnectionClosed) {
			conn.Close()
			n.connMut.Lock()
			n.natsConn = nil
			n.connMut.Unlock()
			return component.ErrNotConnected
		}
		return err
	})
}

func (n *natsWriter) Close(context.Context) (err error) {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	return
}
