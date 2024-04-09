package io

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	isFieldNetwork = "network"
	isFieldAddress = "address"
)

func socketInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Connects to a tcp or unix socket and consumes a continuous stream of messages.`).
		Categories("Network").
		Fields(
			service.NewStringEnumField(isFieldNetwork, "unix", "tcp").
				Description("A network type to assume (unix|tcp)."),
			service.NewStringField(isFieldAddress).
				Description("The address to connect to.").
				Examples("/tmp/benthos.sock", "127.0.0.1:6000"),
			service.NewAutoRetryNacksToggleField(),
		).
		Fields(interop.OldReaderCodecFields("lines")...)
}

func init() {
	err := service.RegisterBatchInput("socket", socketInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		i, err := newSocketReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		// TODO: Inject async cut off?
		return service.AutoRetryNacksBatchedToggled(conf, i)
	})
	if err != nil {
		panic(err)
	}
}

type socketReader struct {
	log *service.Logger

	address   string
	network   string
	codecCtor interop.FallbackReaderCodec

	codecMut sync.Mutex
	codec    interop.FallbackReaderStream
}

func newSocketReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (rdr *socketReader, err error) {
	rdr = &socketReader{
		log: mgr.Logger(),
	}
	if rdr.address, err = pConf.FieldString(isFieldAddress); err != nil {
		return
	}
	if rdr.network, err = pConf.FieldString(isFieldNetwork); err != nil {
		return
	}
	if rdr.codecCtor, err = interop.OldReaderCodecFromParsed(pConf); err != nil {
		return
	}
	return
}

func (s *socketReader) Connect(ctx context.Context) error {
	s.codecMut.Lock()
	defer s.codecMut.Unlock()

	if s.codec != nil {
		return nil
	}

	conn, err := net.Dial(s.network, s.address)
	if err != nil {
		return err
	}

	if s.codec, err = s.codecCtor.Create(conn, func(ctx context.Context, err error) error {
		return nil
	}, scanner.SourceDetails{}); err != nil {
		conn.Close()
		return err
	}
	return nil
}

func (s *socketReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.codecMut.Lock()
	codec := s.codec
	s.codecMut.Unlock()

	if codec == nil {
		return nil, nil, service.ErrNotConnected
	}

	parts, codecAckFn, err := codec.NextBatch(ctx)
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

	if len(parts) == 0 {
		return nil, nil, component.ErrTimeout
	}

	return parts, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (s *socketReader) Close(ctx context.Context) (err error) {
	s.codecMut.Lock()
	defer s.codecMut.Unlock()

	if s.codec != nil {
		err = s.codec.Close(ctx)
		s.codec = nil
	}

	return
}
