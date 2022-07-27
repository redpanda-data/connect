package transaction

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestTaggingErrorsSinglePart(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")
	errTest3 := errors.New("test err 3")

	tran := NewTracked(msg, nil)

	// No error
	assert.Equal(t, nil, tran.resFromError(nil))

	// Static error
	assert.Equal(t, errTest1, tran.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(tran.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest2, tran.resFromError(batchErr))

	// Create new message, no common part, and create batch error
	newMsg := message.QuickBatch([][]byte{[]byte("bar")})
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest1, tran.resFromError(batchErr))

	// Add tran part to new message and create batch error with error on non-tran part
	newMsg = append(newMsg, tran.Message().Get(0))
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, nil, tran.resFromError(batchErr))

	// Create batch error for tran part
	batchErr.Failed(1, errTest3)

	assert.Equal(t, errTest3, tran.resFromError(batchErr))
}

func TestTaggingErrorsMultiplePart(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")
	errTest3 := errors.New("test err 3")

	tran := NewTracked(msg, nil)

	// No error
	assert.Equal(t, nil, tran.resFromError(nil))

	// Static error
	assert.Equal(t, errTest1, tran.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(tran.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest2, tran.resFromError(batchErr))

	// Create new message, no common part, and create batch error
	newMsg := message.QuickBatch([][]byte{[]byte("baz")})
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest1, tran.resFromError(batchErr))

	// Add tran part to new message, still returning general error due to
	// missing part
	newMsg = append(newMsg, tran.Message().Get(0))
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest1, tran.resFromError(batchErr))

	// Add next tran part to new message, and return ack now
	newMsg = append(newMsg, tran.Message().Get(1))
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, nil, tran.resFromError(batchErr))

	// Create batch error with error on non-tran part
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, nil, tran.resFromError(batchErr))

	// Create batch error for tran part
	batchErr.Failed(1, errTest3)
	assert.Equal(t, errTest3, tran.resFromError(batchErr))
}

func TestTaggingErrorsNestedOverlap(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")

	tranOne := NewTracked(msg, nil)

	msgTwo := message.QuickBatch(nil)
	msgTwo = append(msgTwo, tranOne.Message().Get(1), tranOne.Message().Get(0))
	tranTwo := NewTracked(msgTwo, nil)

	// No error
	assert.Equal(t, nil, tranOne.resFromError(nil))
	assert.Equal(t, nil, tranTwo.resFromError(nil))

	// Static error
	assert.Equal(t, errTest1, tranOne.resFromError(errTest1))
	assert.Equal(t, errTest1, tranTwo.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(tranTwo.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest2, tranOne.resFromError(batchErr))
	assert.Equal(t, errTest2, tranTwo.resFromError(batchErr))

	// And if the batch error only touches the first message, only see error in
	// first transaction
	batchErr = batch.NewError(tranOne.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest2, tranOne.resFromError(batchErr))
	assert.Equal(t, errTest1, tranTwo.resFromError(batchErr))
}

func TestTaggingErrorsNestedSerial(t *testing.T) {
	msgOne := message.QuickBatch([][]byte{
		[]byte("foo"),
	})
	msgTwo := message.QuickBatch([][]byte{
		[]byte("bar"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")

	tranOne := NewTracked(msgOne, nil)
	tranTwo := NewTracked(msgTwo, nil)

	msg := message.Batch{
		tranOne.Message().Get(0),
		tranTwo.Message().Get(0),
	}

	// No error
	assert.Equal(t, nil, tranOne.resFromError(nil))
	assert.Equal(t, nil, tranTwo.resFromError(nil))

	// Static error
	assert.Equal(t, errTest1, tranOne.resFromError(errTest1))
	assert.Equal(t, errTest1, tranTwo.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(msg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, errTest2, tranOne.resFromError(batchErr))
	assert.Equal(t, nil, tranTwo.resFromError(batchErr))
}

func BenchmarkErrorWithTagging(b *testing.B) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("buz"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")

	for i := 0; i < b.N; i++ {
		tran := NewTracked(msg, nil)

		batchErr := batch.NewError(tran.Message(), errTest1)
		batchErr.Failed(0, errTest2)

		assert.Equal(b, errTest2, tran.resFromError(batchErr))
	}
}

func BenchmarkErrorWithTaggingN3(b *testing.B) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("buz"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")

	for i := 0; i < b.N; i++ {
		tran := NewTracked(msg, nil)
		tranTwo := NewTracked(tran.Message(), nil)
		tranThree := NewTracked(tranTwo.Message(), nil)

		batchErr := batch.NewError(tranThree.Message(), errTest1)
		batchErr.Failed(0, errTest2)

		assert.Equal(b, errTest2, tran.resFromError(batchErr))
		assert.Equal(b, errTest2, tranTwo.resFromError(batchErr))
		assert.Equal(b, errTest2, tranThree.resFromError(batchErr))
	}
}

func BenchmarkErrorWithTaggingN2(b *testing.B) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("buz"),
	})

	errTest1 := errors.New("test err 1")
	errTest2 := errors.New("test err 2")

	for i := 0; i < b.N; i++ {
		tran := NewTracked(msg, nil)
		tranTwo := NewTracked(tran.Message(), nil)

		batchErr := batch.NewError(tranTwo.Message(), errTest1)
		batchErr.Failed(0, errTest2)

		assert.Equal(b, errTest2, tran.resFromError(batchErr))
		assert.Equal(b, errTest2, tranTwo.resFromError(batchErr))
	}
}

func BenchmarkErrorNoTagging(b *testing.B) {
	msg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
		[]byte("buz"),
	})

	errTest1 := errors.New("test err 1")

	for i := 0; i < b.N; i++ {
		tran := NewTracked(msg, nil)
		assert.Equal(b, errTest1, tran.resFromError(errTest1))
	}
}
