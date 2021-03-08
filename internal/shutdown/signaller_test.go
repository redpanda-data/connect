package shutdown

import (
	"context"
	"testing"
	"time"
)

func assertOpen(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
		t.Error("expected channel to be open")
	default:
	}
}

func assertClosed(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Millisecond * 100):
		t.Error("expected channel to be closed")
	}
}

func TestSignallerNotClosed(t *testing.T) {
	s := NewSignaller()
	assertOpen(t, s.CloseAtLeisureChan())
	assertOpen(t, s.CloseNowChan())
	assertOpen(t, s.HasClosedChan())

	s.ShutdownComplete()
	assertOpen(t, s.CloseAtLeisureChan())
	assertOpen(t, s.CloseNowChan())
	assertClosed(t, s.HasClosedChan())
}

func TestSignallerAtLeisure(t *testing.T) {
	s := NewSignaller()
	s.ShouldCloseAtLeisure()

	assertClosed(t, s.CloseAtLeisureChan())
	assertOpen(t, s.CloseNowChan())
	assertOpen(t, s.HasClosedChan())

	s.ShutdownComplete()
	assertClosed(t, s.CloseAtLeisureChan())
	assertOpen(t, s.CloseNowChan())
	assertClosed(t, s.HasClosedChan())
}

func TestSignallerNow(t *testing.T) {
	s := NewSignaller()
	s.ShouldCloseNow()

	assertClosed(t, s.CloseAtLeisureChan())
	assertClosed(t, s.CloseNowChan())
	assertOpen(t, s.HasClosedChan())

	s.ShutdownComplete()
	assertClosed(t, s.CloseAtLeisureChan())
	assertClosed(t, s.CloseNowChan())
	assertClosed(t, s.HasClosedChan())
}

func TestSignallerAtLeisureCtx(t *testing.T) {
	s := NewSignaller()

	// Cancelled from original context
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done := s.CloseAtLeisureCtx(inCtx)
	assertOpen(t, ctx.Done())
	inDone()
	assertClosed(t, ctx.Done())
	done()

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseAtLeisureCtx(inCtx)
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at leisure signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseAtLeisureCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.ShouldCloseAtLeisure()
	assertClosed(t, ctx.Done())
	done()
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseAtLeisureCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.ShouldCloseNow()
	assertClosed(t, ctx.Done())
	done()
	inDone()
}

func TestSignallerNowCtx(t *testing.T) {
	s := NewSignaller()

	// Cancelled from original context
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done := s.CloseNowCtx(inCtx)
	assertOpen(t, ctx.Done())
	inDone()
	assertClosed(t, ctx.Done())
	done()

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseNowCtx(inCtx)
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Not cancelled from at leisure signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseNowCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.ShouldCloseAtLeisure()
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseNowCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.ShouldCloseNow()
	assertClosed(t, ctx.Done())
	done()
	inDone()
}

func TestSignallerHasClosedCtx(t *testing.T) {
	s := NewSignaller()

	// Cancelled from original context
	inCtx, inDone := context.WithCancel(context.Background())
	ctx, done := s.HasClosedCtx(inCtx)
	assertOpen(t, ctx.Done())
	inDone()
	assertClosed(t, ctx.Done())
	done()

	// Cancelled from returned cancel func
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HasClosedCtx(inCtx)
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at leisure signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.HasClosedCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.ShutdownComplete()
	assertClosed(t, ctx.Done())
	done()
	inDone()
}
