package shutdown

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.False(t, s.ShouldCloseAtLeisure())

	assertOpen(t, s.CloseNowChan())
	assert.False(t, s.ShouldCloseNow())

	assertOpen(t, s.HasClosedChan())
	assert.False(t, s.HasClosed())

	s.ShutdownComplete()

	assertOpen(t, s.CloseAtLeisureChan())
	assert.False(t, s.ShouldCloseAtLeisure())

	assertOpen(t, s.CloseNowChan())
	assert.False(t, s.ShouldCloseNow())

	assertClosed(t, s.HasClosedChan())
	assert.True(t, s.HasClosed())
}

func TestSignallerAtLeisure(t *testing.T) {
	s := NewSignaller()
	s.CloseAtLeisure()

	assertClosed(t, s.CloseAtLeisureChan())
	assert.True(t, s.ShouldCloseAtLeisure())

	assertOpen(t, s.CloseNowChan())
	assert.False(t, s.ShouldCloseNow())

	assertOpen(t, s.HasClosedChan())
	assert.False(t, s.HasClosed())

	s.ShutdownComplete()

	assertClosed(t, s.CloseAtLeisureChan())
	assert.True(t, s.ShouldCloseAtLeisure())

	assertOpen(t, s.CloseNowChan())
	assert.False(t, s.ShouldCloseNow())

	assertClosed(t, s.HasClosedChan())
	assert.True(t, s.HasClosed())
}

func TestSignallerNow(t *testing.T) {
	s := NewSignaller()
	s.CloseNow()

	assertClosed(t, s.CloseAtLeisureChan())
	assert.True(t, s.ShouldCloseAtLeisure())

	assertClosed(t, s.CloseNowChan())
	assert.True(t, s.ShouldCloseNow())

	assertOpen(t, s.HasClosedChan())
	assert.False(t, s.HasClosed())

	s.ShutdownComplete()

	assertClosed(t, s.CloseAtLeisureChan())
	assert.True(t, s.ShouldCloseAtLeisure())

	assertClosed(t, s.CloseNowChan())
	assert.True(t, s.ShouldCloseNow())

	assertClosed(t, s.HasClosedChan())
	assert.True(t, s.HasClosed())
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
	s.CloseAtLeisure()
	assertClosed(t, ctx.Done())
	done()
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancel(context.Background())
	s = NewSignaller()
	ctx, done = s.CloseAtLeisureCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.CloseNow()
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
	s.CloseAtLeisure()
	assertOpen(t, ctx.Done())
	done()
	assertClosed(t, ctx.Done())
	inDone()

	// Cancelled from at immediate signal
	inCtx, inDone = context.WithCancel(context.Background())
	ctx, done = s.CloseNowCtx(inCtx)
	assertOpen(t, ctx.Done())
	s.CloseNow()
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
