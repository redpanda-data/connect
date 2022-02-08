package writer

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
)

func TestBatchedSendHappy(t *testing.T) {
	parts := []string{
		"foo", "bar", "baz", "buz",
	}

	msg := message.QuickBatch(nil)
	for _, p := range parts {
		msg.Append(message.NewPart([]byte(p)))
	}

	seen := []string{}
	assert.NoError(t, IterateBatchedSend(msg, func(i int, p *message.Part) error {
		assert.Equal(t, i, len(seen))
		seen = append(seen, string(p.Get()))
		return nil
	}))

	assert.Equal(t, parts, seen)
}

func TestBatchedSendALittleSad(t *testing.T) {
	parts := []string{
		"foo", "bar", "baz", "buz",
	}

	msg := message.QuickBatch(nil)
	for _, p := range parts {
		msg.Append(message.NewPart([]byte(p)))
	}

	errFirst, errSecond := errors.New("first"), errors.New("second")

	seen := []string{}
	err := IterateBatchedSend(msg, func(i int, p *message.Part) error {
		assert.Equal(t, i, len(seen))
		seen = append(seen, string(p.Get()))
		if i == 1 {
			return errFirst
		}
		if i == 3 {
			return errSecond
		}
		return nil
	})
	assert.Error(t, err)

	expErr := batch.NewError(msg, errFirst).Failed(1, errFirst).Failed(3, errSecond)

	assert.Equal(t, parts, seen)
	assert.Equal(t, expErr, err)
}

func TestBatchedSendFatal(t *testing.T) {
	msg := message.QuickBatch(nil)
	for _, p := range []string{
		"foo", "bar", "baz", "buz",
	} {
		msg.Append(message.NewPart([]byte(p)))
	}

	seen := []string{}
	err := IterateBatchedSend(msg, func(i int, p *message.Part) error {
		assert.Equal(t, i, len(seen))
		seen = append(seen, string(p.Get()))
		if i == 1 {
			return component.ErrTypeClosed
		}
		return nil
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "type was closed")
	assert.Equal(t, []string{"foo", "bar"}, seen)

	seen = []string{}
	err = IterateBatchedSend(msg, func(i int, p *message.Part) error {
		assert.Equal(t, i, len(seen))
		seen = append(seen, string(p.Get()))
		if i == 1 {
			return component.ErrNotConnected
		}
		return nil
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "not connected to target source or sink")
	assert.Equal(t, []string{"foo", "bar"}, seen)

	seen = []string{}
	err = IterateBatchedSend(msg, func(i int, p *message.Part) error {
		assert.Equal(t, i, len(seen))
		seen = append(seen, string(p.Get()))
		if i == 1 {
			return component.ErrTimeout
		}
		return nil
	})
	assert.Error(t, err)
	assert.EqualError(t, err, "action timed out")
	assert.Equal(t, []string{"foo", "bar"}, seen)
}
