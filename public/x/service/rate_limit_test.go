package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type closableRateLimit struct {
	next   time.Duration
	err    error
	closed bool
}

func (c *closableRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return c.next, c.err
}

func (c *closableRateLimit) Close(ctx context.Context) error {
	c.closed = true
	return nil
}

func TestRateLimitAirGapShutdown(t *testing.T) {
	rl := &closableRateLimit{
		next: time.Second,
	}
	agrl := newAirGapRateLimit(rl)

	tout, err := agrl.Access()
	assert.NoError(t, err)
	assert.Equal(t, time.Second, tout)

	rl.next = time.Millisecond
	rl.err = errors.New("test error")

	tout, err = agrl.Access()
	assert.EqualError(t, err, "test error")
	assert.Equal(t, time.Millisecond, tout)

	err = agrl.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	assert.False(t, rl.closed)

	agrl.CloseAsync()
	err = agrl.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	assert.True(t, rl.closed)
}

//------------------------------------------------------------------------------

type closableRateLimitType struct {
	next   time.Duration
	err    error
	closed bool
}

func (c *closableRateLimitType) Access() (time.Duration, error) {
	return c.next, c.err
}

func (c *closableRateLimitType) CloseAsync() {
	c.closed = true
}

func (c *closableRateLimitType) WaitForClose(tout time.Duration) error {
	return nil
}

func TestRateLimitReverseAirGapShutdown(t *testing.T) {
	rl := &closableRateLimitType{
		next: time.Second,
	}
	agrl := newReverseAirGapRateLimit(rl)

	tout, err := agrl.Access(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, time.Second, tout)

	rl.next = time.Millisecond
	rl.err = errors.New("test error")

	tout, err = agrl.Access(context.Background())
	assert.EqualError(t, err, "test error")
	assert.Equal(t, time.Millisecond, tout)

	assert.NoError(t, agrl.Close(context.Background()))
	assert.True(t, rl.closed)
}
