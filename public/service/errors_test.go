package service

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestBatchErrorIndexedBy(t *testing.T) {
	batch := MessageBatch{
		NewMessage([]byte("a")),
		NewMessage([]byte("b")),
		NewMessage([]byte("c")),
		NewMessage([]byte("d")),
	}

	indexer := batch.Index()

	// Scramble the batch
	batch[0], batch[1] = batch[1], batch[0]
	batch[1], batch[2] = batch[2], batch[1]
	batch[3] = NewMessage([]byte("e"))
	batch = append(batch, batch[2], batch[1], batch[0])

	batchError := errors.New("simulated error")
	err := NewBatchError(batch, batchError).
		Failed(0, errors.New("b error")).
		Failed(2, errors.New("a error")).
		Failed(3, errors.New("e error")).
		Failed(6, errors.New("b error"))

	type walkResult struct {
		i int
		c string
		e string
	}
	var results []walkResult
	err.WalkMessagesIndexedBy(indexer, func(i int, m *Message, err error) bool {
		bs, berr := m.AsBytes()
		require.NoErrorf(t, berr, "could not get bytes from message at %d", i)

		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		results = append(results, walkResult{
			i: i, c: string(bs), e: errStr,
		})
		return true
	})

	assert.Equal(t, []walkResult{
		{i: 1, c: "b", e: "b error"},
		{i: 2, c: "c", e: ""},
		{i: 0, c: "a", e: "a error"},
		{i: 0, c: "a", e: ""},
		{i: 2, c: "c", e: ""},
		{i: 1, c: "b", e: "b error"},
	}, results)
}
