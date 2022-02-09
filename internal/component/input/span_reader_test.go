package input

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fnReader struct {
	connectWithContext func(ctx context.Context) error
	readWithContext    func(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error)
	closeAsync         func()
	waitForClose       func(timeout time.Duration) error
}

func (f *fnReader) ConnectWithContext(ctx context.Context) error {
	return f.connectWithContext(ctx)
}

func (f *fnReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	return f.readWithContext(ctx)
}

func (f *fnReader) CloseAsync() {
	f.closeAsync()
}

func (f *fnReader) WaitForClose(timeout time.Duration) error {
	return f.waitForClose(timeout)
}

func TestSpanReader(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		mapping  string
	}{
		{
			name:     "mapping fails",
			contents: `{}`,
			mapping:  `root.foo = this.bar.not_null()`,
		},
		{
			name:     "result not JSON",
			contents: `{}`,
			mapping:  `root = "this isn't json"`,
		},
		{
			name:     "result not an object",
			contents: `{}`,
			mapping:  `root = ["foo","bar"]`,
		},
		{
			name:     "result not a span",
			contents: `{}`,
			mapping:  `root = {"foo":"bar"}`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var connCalled, closeCalled, waitCalled bool

			r, err := NewSpanReader("foo", test.mapping, &fnReader{
				connectWithContext: func(ctx context.Context) error {
					connCalled = true
					return nil
				},
				readWithContext: func(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
					m := message.QuickBatch([][]byte{
						[]byte(test.contents),
					})
					return m, func(context.Context, response.Error) error {
						return nil
					}, nil
				},
				closeAsync: func() {
					closeCalled = true
				},
				waitForClose: func(tout time.Duration) error {
					waitCalled = true
					return nil
				},
			}, mock.NewManager(), log.Noop())
			require.NoError(t, err)

			assert.Nil(t, r.ConnectWithContext(context.Background()))

			res, _, err := r.ReadWithContext(context.Background())
			require.NoError(t, err)
			assert.Equal(t, 1, res.Len())
			assert.Equal(t, test.contents, string(res.Get(0).Get()))

			r.CloseAsync()
			assert.Nil(t, r.WaitForClose(time.Second))

			assert.True(t, connCalled)
			assert.True(t, closeCalled)
			assert.True(t, waitCalled)
		})
	}
}
