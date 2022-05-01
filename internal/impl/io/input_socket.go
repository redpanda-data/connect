package io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	oinput "github.com/benthosdev/benthos/v4/internal/old/input"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
)

func init() {
	err := bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c oinput.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newSocketInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "socket",
		Summary: `Connects to a tcp or unix socket and consumes a continuous stream of messages.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("network", "A network type to assume (unix|tcp).").HasOptions(
				"unix", "tcp",
			),
			docs.FieldString("address", "The address to connect to.", "/tmp/benthos.sock", "127.0.0.1:6000"),
			codec.ReaderDocs.AtVersion("3.42.0"),
			docs.FieldInt("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed.").Advanced(),
		).ChildDefaultAndTypesFromStruct(oinput.NewSocketConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newSocketInput(conf oinput.Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	rdr, err := newSocketReader(conf.Socket, log)
	if err != nil {
		return nil, err
	}
	// TODO: Consider removing the async cut off here. It adds an overhead and
	// we can get the same results by making sure that the async readers forward
	// CloseAsync all the way through. We would need it to be configurable as it
	// wouldn't be appropriate for inputs that have real acks.
	return oinput.NewAsyncReader("socket", true, reader.NewAsyncCutOff(reader.NewAsyncPreserver(rdr)), log, stats)
}

type socketReader struct {
	log log.Modular

	conf      oinput.SocketConfig
	codecCtor codec.ReaderConstructor

	codecMut sync.Mutex
	codec    codec.Reader
}

func newSocketReader(conf oinput.SocketConfig, logger log.Modular) (*socketReader, error) {
	switch conf.Network {
	case "tcp", "unix":
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this input", conf.Network)
	}

	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	return &socketReader{
		log:       logger,
		conf:      conf,
		codecCtor: ctor,
	}, nil
}

func (s *socketReader) ConnectWithContext(ctx context.Context) error {
	s.codecMut.Lock()
	defer s.codecMut.Unlock()

	if s.codec != nil {
		return nil
	}

	conn, err := net.Dial(s.conf.Network, s.conf.Address)
	if err != nil {
		return err
	}

	if s.codec, err = s.codecCtor("", conn, func(ctx context.Context, err error) error {
		return nil
	}); err != nil {
		conn.Close()
		return err
	}

	s.log.Infof("Consuming from socket at '%v://%v'\n", s.conf.Network, s.conf.Address)
	return nil
}

func (s *socketReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	s.codecMut.Lock()
	codec := s.codec
	s.codecMut.Unlock()

	if codec == nil {
		return nil, nil, component.ErrNotConnected
	}

	parts, codecAckFn, err := codec.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			s.codecMut.Lock()
			if s.codec != nil && s.codec == codec {
				s.codec.Close(ctx)
				s.codec = nil
			}
			s.codecMut.Unlock()
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, component.ErrTimeout
		}
		return nil, nil, err
	}

	// We simply bounce rejected messages in a loop downstream so there's no
	// benefit to aggregating acks.
	_ = codecAckFn(context.Background(), nil)

	msg := message.QuickBatch(nil)
	msg.Append(parts...)

	if msg.Len() == 0 {
		return nil, nil, component.ErrTimeout
	}

	return msg, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (s *socketReader) CloseAsync() {
	s.codecMut.Lock()
	if s.codec != nil {
		s.codec.Close(context.Background())
		s.codec = nil
	}
	s.codecMut.Unlock()
}

func (s *socketReader) WaitForClose(time.Duration) error {
	return nil
}
