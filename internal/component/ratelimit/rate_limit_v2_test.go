package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/stretchr/testify/assert"
)

type closableRateLimit struct {
	closed bool
}

func (c *closableRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return 0, nil
}

func (c *closableRateLimit) Close(ctx context.Context) error {
	c.closed = true
	return nil
}

func TestRateLimitAirGapShutdown(t *testing.T) {
	rl := &closableRateLimit{}
	agrl := NewV2ToV1RateLimit(rl, metrics.Noop())

	err := agrl.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	assert.False(t, rl.closed)

	agrl.CloseAsync()
	err = agrl.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	assert.True(t, rl.closed)
}
