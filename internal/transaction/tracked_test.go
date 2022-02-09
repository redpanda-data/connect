package transaction

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, response.NewError(nil), tran.resFromError(nil))

	// Static error
	assert.Equal(t, response.NewError(errTest1), tran.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(tran.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest2), tran.resFromError(batchErr))

	// Create new message, no common part, and create batch error
	newMsg := message.QuickBatch([][]byte{[]byte("bar")})
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest1), tran.resFromError(batchErr))

	// Add tran part to new message and create batch error with error on non-tran part
	newMsg.Append(tran.Message().Get(0))
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(nil), tran.resFromError(batchErr))

	// Create batch error for tran part
	batchErr.Failed(1, errTest3)

	assert.Equal(t, response.NewError(errTest3), tran.resFromError(batchErr))
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
	assert.Equal(t, response.NewError(nil), tran.resFromError(nil))

	// Static error
	assert.Equal(t, response.NewError(errTest1), tran.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(tran.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest2), tran.resFromError(batchErr))

	// Create new message, no common part, and create batch error
	newMsg := message.QuickBatch([][]byte{[]byte("baz")})
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest1), tran.resFromError(batchErr))

	// Add tran part to new message, still returning general error due to
	// missing part
	newMsg.Append(tran.Message().Get(0))
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest1), tran.resFromError(batchErr))

	// Add next tran part to new message, and return ack now
	newMsg.Append(tran.Message().Get(1))
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(nil), tran.resFromError(batchErr))

	// Create batch error with error on non-tran part
	batchErr = batch.NewError(newMsg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(nil), tran.resFromError(batchErr))

	// Create batch error for tran part
	batchErr.Failed(1, errTest3)
	assert.Equal(t, response.NewError(errTest3), tran.resFromError(batchErr))
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
	msgTwo.Append(tranOne.Message().Get(1))
	msgTwo.Append(tranOne.Message().Get(0))
	tranTwo := NewTracked(msgTwo, nil)

	// No error
	assert.Equal(t, response.NewError(nil), tranOne.resFromError(nil))
	assert.Equal(t, response.NewError(nil), tranTwo.resFromError(nil))

	// Static error
	assert.Equal(t, response.NewError(errTest1), tranOne.resFromError(errTest1))
	assert.Equal(t, response.NewError(errTest1), tranTwo.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(tranTwo.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest2), tranOne.resFromError(batchErr))
	assert.Equal(t, response.NewError(errTest2), tranTwo.resFromError(batchErr))

	// And if the batch error only touches the first message, only see error in
	// first transaction
	batchErr = batch.NewError(tranOne.Message(), errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest2), tranOne.resFromError(batchErr))
	assert.Equal(t, response.NewError(errTest1), tranTwo.resFromError(batchErr))
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

	msg := message.QuickBatch(nil)
	msg.Append(tranOne.Message().Get(0))
	msg.Append(tranTwo.Message().Get(0))

	// No error
	assert.Equal(t, response.NewError(nil), tranOne.resFromError(nil))
	assert.Equal(t, response.NewError(nil), tranTwo.resFromError(nil))

	// Static error
	assert.Equal(t, response.NewError(errTest1), tranOne.resFromError(errTest1))
	assert.Equal(t, response.NewError(errTest1), tranTwo.resFromError(errTest1))

	// Create batch error with single part
	batchErr := batch.NewError(msg, errTest1)
	batchErr.Failed(0, errTest2)

	assert.Equal(t, response.NewError(errTest2), tranOne.resFromError(batchErr))
	assert.Equal(t, response.NewError(nil), tranTwo.resFromError(batchErr))
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

		assert.Equal(b, response.NewError(errTest2), tran.resFromError(batchErr))
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

		assert.Equal(b, response.NewError(errTest2), tran.resFromError(batchErr))
		assert.Equal(b, response.NewError(errTest2), tranTwo.resFromError(batchErr))
		assert.Equal(b, response.NewError(errTest2), tranThree.resFromError(batchErr))
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

		assert.Equal(b, response.NewError(errTest2), tran.resFromError(batchErr))
		assert.Equal(b, response.NewError(errTest2), tranTwo.resFromError(batchErr))
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
		assert.Equal(b, response.NewError(errTest1), tran.resFromError(errTest1))
	}
}
