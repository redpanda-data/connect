package batch

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCombinedAckFunc(t *testing.T) {
	var ackCalled int
	var ackErr error
	combined := NewCombinedAcker(func(c context.Context, e error) error {
		ackCalled++
		ackErr = e
		return nil
	})

	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	first := combined.Derive()
	second := combined.Derive()
	third := combined.Derive()

	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	assert.NoError(t, first(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	assert.NoError(t, second(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	assert.NoError(t, third(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 1, ackCalled)

	// Call multiple times
	assert.NoError(t, first(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 1, ackCalled)

	assert.NoError(t, second(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 1, ackCalled)

	assert.NoError(t, third(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 1, ackCalled)
}

func TestCombinedAckError(t *testing.T) {
	var ackCalled int
	var ackErr error
	combined := NewCombinedAcker(func(c context.Context, e error) error {
		ackCalled++
		ackErr = e
		return nil
	})

	testErr := errors.New("test error")

	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	first := combined.Derive()
	second := combined.Derive()
	third := combined.Derive()

	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	assert.NoError(t, first(context.Background(), nil))
	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	assert.NoError(t, second(context.Background(), testErr))
	assert.NoError(t, ackErr)
	assert.Equal(t, 0, ackCalled)

	assert.NoError(t, third(context.Background(), nil))
	assert.Equal(t, testErr, ackErr)
	assert.Equal(t, 1, ackCalled)

	// Call multiple times
	assert.NoError(t, first(context.Background(), nil))
	assert.Equal(t, testErr, ackErr)
	assert.Equal(t, 1, ackCalled)

	assert.NoError(t, second(context.Background(), nil))
	assert.Equal(t, testErr, ackErr)
	assert.Equal(t, 1, ackCalled)

	assert.NoError(t, third(context.Background(), nil))
	assert.Equal(t, testErr, ackErr)
	assert.Equal(t, 1, ackCalled)
}

func TestCombinedAckErrorSync(t *testing.T) {
	var ackCalled bool
	combined := NewCombinedAcker(func(c context.Context, e error) error {
		ackCalled = true
		return nil
	})

	var derivedFuncs []AckFunc
	for i := 0; i < 1000; i++ {
		derivedFuncs = append(derivedFuncs, combined.Derive())
	}

	startChan := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			<-startChan

			for j := 0; j < 100; j++ {
				assert.NoError(t, derivedFuncs[(i*100)+j](context.Background(), nil))
			}
		}()
	}

	close(startChan)
	wg.Wait()

	assert.True(t, ackCalled)
}
