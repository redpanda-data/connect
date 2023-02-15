package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newNATSStreamOutput), docs.ComponentSpec{
		Name:        "nats_stream",
		Summary:     `Publish to a NATS Stream subject.`,
		Description: output.Description(true, false, auth.Description()),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldString("cluster_id", "The cluster ID to publish to."),
			docs.FieldString("subject", "The subject to publish to."),
			docs.FieldString("client_id", "The client ID to connect with."),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			btls.FieldSpec(),
			auth.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewNATSStreamConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNATSStreamOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newNATSStreamWriter(conf.NATSStream, mgr)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("nats_stream", conf.NATSStream.MaxInFlight, w, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(a), nil
}

type natsStreamWriter struct {
	log log.Modular
	fs  *service.FS

	stanConn stan.Conn
	natsConn *nats.Conn
	connMut  sync.RWMutex

	urls    string
	conf    output.NATSStreamConfig
	tlsConf *tls.Config
}

func newNATSStreamWriter(conf output.NATSStreamConfig, mgr bundle.NewManagement) (*natsStreamWriter, error) {
	if conf.ClientID == "" {
		rgen := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Generate random client id if one wasn't supplied.
		b := make([]byte, 16)
		rgen.Read(b)
		conf.ClientID = fmt.Sprintf("client-%x", b)
	}

	n := natsStreamWriter{
		log:  mgr.Logger(),
		fs:   service.NewFS(mgr.FS()),
		conf: conf,
	}
	n.urls = strings.Join(conf.URLs, ",")
	var err error
	if conf.TLS.Enabled {
		if n.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}

	return &n, nil
}

func (n *natsStreamWriter) Connect(ctx context.Context) error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var opts []nats.Option
	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}

	opts = append(opts, authConfToOptions(n.conf.Auth, n.fs)...)
	opts = append(opts, errorHandlerOptionFromModularLogger(n.log))

	natsConn, err := nats.Connect(n.urls, opts...)
	if err != nil {
		return err
	}

	stanConn, err := stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsConn(natsConn),
	)
	if err != nil {
		natsConn.Close()
		return err
	}

	n.stanConn = stanConn
	n.natsConn = natsConn
	n.log.Infof("Sending NATS messages to subject: %v\n", n.conf.Subject)
	return nil
}

func (n *natsStreamWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	n.connMut.RLock()
	conn := n.stanConn
	n.connMut.RUnlock()

	if conn == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		err := conn.Publish(n.conf.Subject, p.AsBytes())
		if errors.Is(err, stan.ErrConnectionClosed) {
			conn.Close()
			n.connMut.Lock()
			n.stanConn = nil
			n.natsConn.Close()
			n.natsConn = nil
			n.connMut.Unlock()
			return component.ErrNotConnected
		}
		return err
	})
}

func (n *natsStreamWriter) Close(context.Context) (err error) {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	if n.stanConn != nil {
		err = n.stanConn.Close()
		n.stanConn = nil
	}
	return
}
