package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/public/service"
)

func natsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Publish to an NATS subject.").
		Description(`This output will interpolate functions within the subject field, you can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).

` + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewInterpolatedStringField("subject").
			Description("The subject to publish to.").
			Example("foo.bar.baz")).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to messages.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Content-Type": "application/json",
				"Timestamp":    `${!meta("Timestamp")}`,
			})).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(64)).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
}

func init() {
	err := service.RegisterOutput(
		"nats", natsOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			w, err := newNATSWriter(conf, mgr)
			return w, maxInFlight, err
		},
	)
	if err != nil {
		panic(err)
	}
}

type natsWriter struct {
	urls          string
	headers       map[string]*service.InterpolatedString
	subjectStr    *service.InterpolatedString
	subjectStrRaw string
	authConf      auth.Config
	tlsConf       *tls.Config

	log *service.Logger
	fs  *service.FS

	natsConn *nats.Conn
	connMut  sync.RWMutex
}

func newNATSWriter(conf *service.ParsedConfig, mgr *service.Resources) (*natsWriter, error) {
	n := natsWriter{
		log:     mgr.Logger(),
		fs:      mgr.FS(),
		headers: make(map[string]*service.InterpolatedString),
	}
	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	n.urls = strings.Join(urlList, ",")

	if n.subjectStrRaw, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if n.subjectStr, err = conf.FieldInterpolatedString("subject"); err != nil {
		return nil, err
	}

	if n.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		n.tlsConf = tlsConf
	}

	if n.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
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

	opts = append(opts, authConfToOptions(n.authConf, n.fs)...)
	opts = append(opts, errorHandlerOption(n.log))

	if n.natsConn, err = nats.Connect(n.urls, opts...); err != nil {
		return err
	}

	if err == nil {
		n.log.Infof("Sending NATS messages to subject: %v\n", n.subjectStrRaw)
	}
	return err
}

// Write attempts to write a message.
func (n *natsWriter) Write(context context.Context, msg *service.Message) error {
	n.connMut.RLock()
	conn := n.natsConn
	n.connMut.RUnlock()

	if conn == nil {
		return service.ErrNotConnected
	}

	subject, err := n.subjectStr.TryString(msg)
	if err != nil {
		return fmt.Errorf("subject interpolation error: %w", err)
	}

	n.log.Debugf("Writing NATS message to subject %s", subject)
	// fill message data
	nMsg := nats.NewMsg(subject)
	nMsg.Data, err = msg.AsBytes()
	if err != nil {
		return err
	}

	if conn.HeadersSupported() {
		// fill bloblang headers
		for k, v := range n.headers {
			headerStr, err := v.TryString(msg)
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
		return service.ErrNotConnected
	}
	return err
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
