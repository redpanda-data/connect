package service

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/stream"
)

// Stream executes a full Benthos stream and provides methods for performing
// status checks, terminating the stream, and blocking until the stream ends.
type Stream struct {
	strm    *stream.Type
	shutSig *shutdown.Signaller

	conf   stream.Config
	mgr    *manager.Type
	stats  metrics.Type
	logger log.Modular
}

func newStream(conf stream.Config, mgr *manager.Type, stats metrics.Type, logger log.Modular) *Stream {
	return &Stream{
		conf:    conf,
		mgr:     mgr,
		stats:   stats,
		logger:  logger,
		shutSig: shutdown.NewSignaller(),
	}
}

// Run attempts to start the stream pipeline and blocks until either the stream
// has gracefully come to a stop, or the provided context is cancelled.
func (s *Stream) Run(ctx context.Context) (err error) {
	if s.strm, err = stream.New(s.conf,
		stream.OptOnClose(func() {
			s.shutSig.ShutdownComplete()
		}),
		stream.OptSetManager(s.mgr),
		stream.OptSetLogger(s.logger),
		stream.OptSetStats(s.stats)); err != nil {
		return
	}
	select {
	case <-s.shutSig.HasClosedChan():
		for {
			if err = s.StopWithin(time.Millisecond * 100); err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return
			}
		}
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
	stopAt := time.Now().Add(timeout)
	if err := s.strm.Stop(timeout); err != nil {
		// Still attempt to shut down other resources but do not block.
		defer func() {
			s.mgr.CloseAsync()
			s.stats.Close()
		}()
		return err
	}

	s.mgr.CloseAsync()
	if err := s.mgr.WaitForClose(time.Until(stopAt)); err != nil {
		// Same as above, attempt to shut down other resources but do not block.
		defer s.stats.Close()
		return err
	}

	return s.stats.Close()
}
