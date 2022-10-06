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
	err := MockWalkableError(batch, batchError,
		errors.New("a error"),
		errors.New("b error"),
		errors.New("c error"),
	)

	require.Equal(t, err.IndexedErrors(), len(batch), "indexed errors did not match size of batch")
	require.Equal(t, err.Error(), batchError.Error(), "headline error is not propagated")
	err.WalkMessages(func(i int, m *Message, err error) bool {
		bs, berr := m.AsBytes()
		require.NoErrorf(t, berr, "could not get bytes from message at %d", i)
		require.Equal(t, err.Error(), fmt.Sprintf("%s error", bs))
		return true
	})
}

func TestMockWalkableError_ExcessErrors(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("a")),
		NewMessage([]byte("b")),
		NewMessage([]byte("c")),
	}

	batchError := errors.New("simulated error")
	err := MockWalkableError(batch, batchError,
		errors.New("a error"),
		errors.New("b error"),
		errors.New("c error"),
		errors.New("d error"),
	)

	require.Equal(t, err.IndexedErrors(), len(batch), "indexed errors did not match size of batch")
}

func TestMockWalkableError_OmitSuccessfulMessages(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("a")),
		NewMessage([]byte("b")),
		NewMessage([]byte("c")),
	}

	batchError := errors.New("simulated error")
	err := MockWalkableError(batch, batchError,
		errors.New("a error"),
		nil,
		errors.New("c error"),
	)

	require.Equal(t, err.IndexedErrors(), 2, "indexed errors did not match size of batch")
}
