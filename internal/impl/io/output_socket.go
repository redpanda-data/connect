package io

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newSocketOutput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "socket",
		Summary: `Connects to a (tcp/udp/unix) server and sends a continuous stream of data, dividing messages according to the specified codec.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("network", "The network type to connect as.").HasOptions(
				"unix", "tcp", "udp",
			),
			docs.FieldString("address", "The address (or path) to connect to.", "/tmp/benthos.sock", "localhost:9000"),
			codec.WriterDocs,
		).ChildDefaultAndTypesFromStruct(output.NewSocketConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newSocketOutput(conf output.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	t, err := newSocketWriter(conf.Socket, mgr, log)
	if err != nil {
		return nil, err
	}
	return output.NewAsyncWriter("socket", 1, t, mgr)
}

type socketWriter struct {
	network   string
	address   string
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	log log.Modular

	writer    codec.Writer
	writerMut sync.Mutex
}

func newSocketWriter(conf output.SocketConfig, mgr bundle.NewManagement, log log.Modular) (*socketWriter, error) {
	switch conf.Network {
	case "tcp", "udp", "unix":
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this output", conf.Network)
	}
	codec, codecConf, err := codec.GetWriter(conf.Codec)
	if err != nil {
		return nil, err
	}
	t := socketWriter{
		network:   conf.Network,
		address:   conf.Address,
		codec:     codec,
		codecConf: codecConf,
		log:       log,
	}
	return &t, nil
}

func (s *socketWriter) Connect(ctx context.Context) error {
	s.writerMut.Lock()
	defer s.writerMut.Unlock()
	if s.writer != nil {
		return nil
	}

	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		return err
	}

	s.writer, err = s.codec(conn)
	if err != nil {
		conn.Close()
		return err
	}

	s.log.Infof("Sending messages over %v socket to: %s\n", s.network, s.address)
	return nil
}

func (s *socketWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	s.writerMut.Lock()
	w := s.writer
	s.writerMut.Unlock()

	if w == nil {
		return component.ErrNotConnected
	}

	return msg.Iter(func(i int, part *message.Part) error {
		serr := w.Write(ctx, part)
		if serr != nil || s.codecConf.CloseAfter {
			s.writerMut.Lock()
			s.writer.Close(ctx)
			s.writer = nil
			s.writerMut.Unlock()
		}
		return serr
	})
}

func (s *socketWriter) Close(ctx context.Context) error {
	s.writerMut.Lock()
	defer s.writerMut.Unlock()

	var err error
	if s.writer != nil {
		err = s.writer.Close(context.Background())
		s.writer = nil
	}
	return err
}
