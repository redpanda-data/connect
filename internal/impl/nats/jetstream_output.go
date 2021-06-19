package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/nats-io/nats.go"
)

func init() {
	bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c output.Config, nm bundle.NewManagement) (output.Type, error) {
		w, err := newJetStreamOutput(c.NATSJetStream, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		o, err := output.NewAsyncWriter(output.TypeNATSJetStream, c.NATSJetStream.MaxInFlight, w, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return output.OnlySinglePayloads(o), nil
	}), docs.ComponentSpec{
		Name:    output.TypeNATSJetStream,
		Type:    docs.TypeOutput,
		Status:  docs.StatusExperimental,
		Version: "3.46.0",
		Summary: `Write messages to a NATS JetStream subject.`,
		Categories: []string{
			string(output.CategoryServices),
		},
		Config: docs.FieldComponent().WithChildren(
			docs.FieldCommon(
				"urls",
				"A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.",
				[]string{"nats://127.0.0.1:4222"},
				[]string{"nats://username:password@127.0.0.1:4222"},
			).Array(),
			docs.FieldCommon("subject", "A subject to write to.").IsInterpolated(),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			btls.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewNATSJetStreamConfig()),
	})
}

//------------------------------------------------------------------------------

type jetStreamOutput struct {
	urls    string
	conf    output.NATSJetStreamConfig
	tlsConf *tls.Config

	subjectStr *field.Expression

	stats metrics.Type
	log   log.Modular

	connMut  sync.Mutex
	natsConn *nats.Conn
	jCtx     nats.JetStreamContext

	shutSig *shutdown.Signaller
}

func newJetStreamOutput(conf output.NATSJetStreamConfig, log log.Modular, stats metrics.Type) (*jetStreamOutput, error) {
	j := jetStreamOutput{
		conf:    conf,
		stats:   stats,
		log:     log,
		shutSig: shutdown.NewSignaller(),
	}
	j.urls = strings.Join(conf.URLs, ",")
	var err error
	if conf.TLS.Enabled {
		if j.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	if j.subjectStr, err = bloblang.NewField(conf.Subject); err != nil {
		return nil, fmt.Errorf("subject expression: %w", err)
	}
	return &j, nil
}

//------------------------------------------------------------------------------

func (j *jetStreamOutput) ConnectWithContext(ctx context.Context) error {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var jCtx nats.JetStreamContext
	var err error

	defer func() {
		if err != nil && natsConn != nil {
			natsConn.Close()
		}
	}()

	var opts []nats.Option
	if j.tlsConf != nil {
		opts = append(opts, nats.Secure(j.tlsConf))
	}
	if natsConn, err = nats.Connect(j.urls, opts...); err != nil {
		return err
	}

	if jCtx, err = natsConn.JetStream(); err != nil {
		return err
	}

	j.log.Infof("Sending NATS messages to JetStream subject: %v\n", j.conf.Subject)

	j.natsConn = natsConn
	j.jCtx = jCtx
	return nil
}

func (j *jetStreamOutput) disconnect() {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		j.natsConn.Close()
		j.natsConn = nil
	}
	j.jCtx = nil
}

//------------------------------------------------------------------------------

func (j *jetStreamOutput) WriteWithContext(ctx context.Context, msg types.Message) error {
	j.connMut.Lock()
	jCtx := j.jCtx
	j.connMut.Unlock()
	if jCtx == nil {
		return types.ErrNotConnected
	}

	return writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		subject := j.subjectStr.String(i, msg)
		_, err := jCtx.Publish(subject, p.Get())
		return err
	})
}

func (j *jetStreamOutput) CloseAsync() {
	go func() {
		j.disconnect()
		j.shutSig.ShutdownComplete()
	}()
}

func (j *jetStreamOutput) WaitForClose(timeout time.Duration) error {
	select {
	case <-j.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}
