package span

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type fnReader struct {
	connectWithContext func(ctx context.Context) error
	readWithContext    func(ctx context.Context) (message.Batch, input.AsyncAckFn, error)
	close              func(ctx context.Context) error
}

func (f *fnReader) Connect(ctx context.Context) error {
	return f.connectWithContext(ctx)
}

func (f *fnReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	return f.readWithContext(ctx)
}

func (f *fnReader) Close(ctx context.Context) error {
	return f.close(ctx)
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

			r, err := NewReader("foo", test.mapping, &fnReader{
				connectWithContext: func(ctx context.Context) error {
					connCalled = true
					return nil
				},
				readWithContext: func(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
					m := message.QuickBatch([][]byte{
						[]byte(test.contents),
					})
					return m, func(context.Context, error) error {
						return nil
					}, nil
				},
				close: func(ctx context.Context) error {
					closeCalled = true
					waitCalled = true
					return nil
				},
			}, mock.NewManager())
			require.NoError(t, err)

			assert.Nil(t, r.Connect(context.Background()))

			res, _, err := r.ReadBatch(context.Background())
			require.NoError(t, err)
			assert.Equal(t, 1, res.Len())
			assert.Equal(t, test.contents, string(res.Get(0).AsBytes()))

			assert.NoError(t, r.Close(context.Background()))

			assert.True(t, connCalled)
			assert.True(t, closeCalled)
			assert.True(t, waitCalled)
		})
	}
}
