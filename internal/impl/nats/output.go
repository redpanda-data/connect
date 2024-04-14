package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/component/output/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

func natsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Publish to an NATS subject.").
		Description(`This output will interpolate functions within the subject field, you can find a list of functions [here](/docs/configuration/interpolation#bloblang-queries).

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
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
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(64)).
		Fields(connectionTailFields()...).
		Field(outputTracingDocs())
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
			if err != nil {
				return nil, 0, err
			}
			spanOutput, err := span.NewOutput("nats", conf, w, mgr)
			return spanOutput, maxInFlight, err
		},
	)
	if err != nil {
		panic(err)
	}
}

type natsWriter struct {
	connDetails   connectionDetails
	headers       map[string]*service.InterpolatedString
	metaFilter    *service.MetadataFilter
	subjectStr    *service.InterpolatedString
	subjectStrRaw string

	log *service.Logger

	natsConn *nats.Conn
	connMut  sync.RWMutex
}

func newNATSWriter(conf *service.ParsedConfig, mgr *service.Resources) (*natsWriter, error) {
	n := natsWriter{
		log:     mgr.Logger(),
		headers: make(map[string]*service.InterpolatedString),
	}

	var err error
	if n.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if n.subjectStrRaw, err = conf.FieldString("subject"); err != nil {
		return nil, err
	}

	if n.subjectStr, err = conf.FieldInterpolatedString("subject"); err != nil {
		return nil, err
	}

	if n.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}

	if conf.Contains("metadata") {
		if n.metaFilter, err = conf.FieldMetadataFilter("metadata"); err != nil {
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
	if n.natsConn, err = n.connDetails.get(ctx); err != nil {
		return err
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
		_ = n.metaFilter.Walk(msg, func(key, value string) error {
			nMsg.Header.Add(key, value)
			return nil
		})
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
