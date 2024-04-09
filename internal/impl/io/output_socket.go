package io

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	osFieldNetwork = "network"
	osFieldAddress = "address"
)

func socketOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Connects to a (tcp/udp/unix) server and sends a continuous stream of data, dividing messages according to the specified codec.`).
		Categories("Network").
		Fields(
			service.NewStringEnumField(osFieldNetwork, "unix", "tcp", "udp").
				Description("A network type to connect as."),
			service.NewStringField(osFieldAddress).
				Description("The address to connect to.").
				Examples("/tmp/benthos.sock", "127.0.0.1:6000"),
			service.NewInternalField(codec.NewWriterDocs("codec").HasDefault("lines")),
		)
}

func init() {
	err := service.RegisterOutput("socket", socketOutputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
		maxInFlight = 1
		out, err = newSocketWriterFromParsed(conf, mgr)
		return
	})
	if err != nil {
		panic(err)
	}
}

type socketWriter struct {
	network    string
	address    string
	suffixFn   codec.SuffixFn
	appendMode bool

	log *service.Logger

	writer    io.WriteCloser
	writerMut sync.Mutex
}

func newSocketWriterFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (w *socketWriter, err error) {
	w = &socketWriter{
		log: mgr.Logger(),
	}
	if w.address, err = pConf.FieldString(osFieldAddress); err != nil {
		return
	}
	if w.network, err = pConf.FieldString(osFieldNetwork); err != nil {
		return
	}

	var codecStr string
	if codecStr, err = pConf.FieldString("codec"); err != nil {
		return
	}
	if w.suffixFn, w.appendMode, err = codec.GetWriter(codecStr); err != nil {
		return
	}
	return
}

func (s *socketWriter) Connect(ctx context.Context) error {
	s.writerMut.Lock()
	defer s.writerMut.Unlock()
	if s.writer != nil {
		return nil
	}

	var err error
	if s.writer, err = net.Dial(s.network, s.address); err != nil {
		return err
	}
	return nil
}

func (s *socketWriter) writeTo(wtr io.Writer, p *service.Message) error {
	mBytes, err := p.AsBytes()
	if err != nil {
		return err
	}

	suffix, addSuffix := s.suffixFn(mBytes)

	if _, err := wtr.Write(mBytes); err != nil {
		return err
	}
	if addSuffix {
		if _, err := wtr.Write(suffix); err != nil {
			return err
		}
	}
	return nil
}

func (s *socketWriter) Write(ctx context.Context, msg *service.Message) error {
	s.writerMut.Lock()
	w := s.writer
	s.writerMut.Unlock()

	if w == nil {
		return component.ErrNotConnected
	}

	serr := s.writeTo(w, msg)
	if serr != nil || !s.appendMode {
		s.writerMut.Lock()
		_ = s.writer.Close()
		s.writer = nil
		s.writerMut.Unlock()
	}
	return serr
}

func (s *socketWriter) Close(ctx context.Context) error {
	s.writerMut.Lock()
	defer s.writerMut.Unlock()

	var err error
	if s.writer != nil {
		err = s.writer.Close()
		s.writer = nil
	}
	return err
}
