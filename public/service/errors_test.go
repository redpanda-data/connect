package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockWalkableError(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("a")),
		NewMessage([]byte("b")),
		NewMessage([]byte("c")),
	}

	batchError := errors.New("simulated error")
	err := NewBatchError(batch, batchError).
		Failed(0, errors.New("a error")).
		Failed(1, errors.New("b error")).
		Failed(2, errors.New("c error"))

	require.Equal(t, err.IndexedErrors(), len(batch), "indexed errors did not match size of batch")
	require.ErrorIs(t, err, batchError, "headline error is not propagated")

	runs := 0
	err.WalkMessages(func(i int, m *Message, err error) bool {
		runs++

		bs, berr := m.AsBytes()
		require.NoErrorf(t, berr, "could not get bytes from message at %d", i)
		require.Equal(t, err.Error(), fmt.Sprintf("%s error", bs))
		return true
	})

	require.Equal(t, len(batch), runs, "WalkMessages did not iterate the whole batch")
}

func TestMockWalkableError_ExcessErrors(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("a")),
		NewMessage([]byte("b")),
		NewMessage([]byte("c")),
	}

	batchError := errors.New("simulated error")
	err := NewBatchError(batch, batchError).
		Failed(0, errors.New("a error")).
		Failed(1, errors.New("b error")).
		Failed(2, errors.New("c error")).
		Failed(3, errors.New("d error"))

	require.Equal(t, len(batch), err.IndexedErrors(), "indexed errors did not match size of batch")
}

func TestMockWalkableError_OmitSuccessfulMessages(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("a")),
		NewMessage([]byte("b")),
		NewMessage([]byte("c")),
	}

	batchError := errors.New("simulated error")
	err := NewBatchError(batch, batchError).
		Failed(0, errors.New("a error")).
		Failed(2, errors.New("c error"))

	require.Equal(t, err.IndexedErrors(), 2, "indexed errors did not match size of batch")
}
