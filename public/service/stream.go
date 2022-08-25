package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

// Stream executes a full Benthos stream and provides methods for performing
// status checks, terminating the stream, and blocking until the stream ends.
type Stream struct {
	strm    *stream.Type
	httpAPI *api.Type
	strmMut sync.Mutex
	shutSig *shutdown.Signaller
	onStart func()

	conf   stream.Config
	mgr    *manager.Type
	stats  metrics.Type
	tracer trace.TracerProvider
	logger log.Modular
}

func newStream(
	conf stream.Config,
	httpAPI *api.Type,
	mgr *manager.Type,
	stats metrics.Type,
	tracer trace.TracerProvider,
	logger log.Modular,
	onStart func(),
) *Stream {
	return &Stream{
		conf:    conf,
		httpAPI: httpAPI,
		mgr:     mgr,
		stats:   stats,
		tracer:  tracer,
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
		onStart: onStart,
	}
}

// Run attempts to start the stream pipeline and blocks until either the stream
// has gracefully come to a stop, or the provided context is cancelled.
func (s *Stream) Run(ctx context.Context) (err error) {
	s.strmMut.Lock()
	if s.strm != nil {
		err = errors.New("stream has already been run")
	} else {
		s.strm, err = stream.New(s.conf, s.mgr,
			stream.OptOnClose(func() {
				s.shutSig.ShutdownComplete()
			}))
	}
	s.strmMut.Unlock()
	if err != nil {
		return
	}

	if s.httpAPI != nil {
		go func() {
			_ = s.httpAPI.ListenAndServe()
		}()
	}
	go s.onStart()

	select {
	case <-s.shutSig.HasClosedChan():
		return s.Stop(ctx)
	case <-ctx.Done():
	}
	return ctx.Err()
}

// StopWithin attempts to close the stream within the specified timeout period.
// Initially the attempt is graceful, but as the timeout draws close the attempt
// becomes progressively less graceful.
//
// An ungraceful shutdown increases the likelihood of processing duplicate
// messages on the next start up, but never results in dropped messages as long
// as the input source supports at-least-once delivery.
func (s *Stream) StopWithin(timeout time.Duration) error {
	ctx, done := context.WithTimeout(context.Background(), timeout)
	defer done()
	return s.Stop(ctx)
}

// Stop attempts to close the stream gracefully, but if the context is closed or
// draws near to a deadline the attempt becomes less graceful.
//
// An ungraceful shutdown increases the likelihood of processing duplicate
// messages on the next start up, but never results in dropped messages as long
// as the input source supports at-least-once delivery.
func (s *Stream) Stop(ctx context.Context) (err error) {
	s.strmMut.Lock()
	strm := s.strm
	s.strmMut.Unlock()
	if strm == nil {
		return errors.New("stream has not been run yet")
	}

	stopStats := s.stats
	closeStats := func() error {
		if stopStats == nil {
			return nil
		}
		err := stopStats.Close()
		stopStats = nil
		return err
	}

	stopTracer := s.tracer
	closeTracer := func(ctx context.Context) error {
		if stopTracer == nil {
			return nil
		}
		if shutter, ok := stopTracer.(interface {
			Shutdown(context.Context) error
		}); ok {
			return shutter.Shutdown(ctx)
		}
		return nil
	}

	stopHTTP := s.httpAPI
	closeHTTP := func(ctx context.Context) error {
		if stopHTTP == nil {
			return nil
		}
		err := s.httpAPI.Shutdown(ctx)
		stopHTTP = nil
		return err
	}

	defer func() {
		if err == nil {
			return
		}

		// Still attempt to shut down other resources on an error, but do not
		// block.
		s.mgr.TriggerStopConsuming()
		_ = closeStats()
		_ = closeTracer(context.Background())
		_ = closeHTTP(context.Background())
	}()

	if err = strm.Stop(ctx); err != nil {
		return
	}

	s.mgr.TriggerStopConsuming()
	if err = s.mgr.WaitForClose(ctx); err != nil {
		return
	}

	if err = closeStats(); err != nil {
		return
	}

	if err = closeTracer(ctx); err != nil {
		return
	}

	err = closeHTTP(ctx)
	return
}
