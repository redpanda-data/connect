package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fnBatchWriter struct {
	connectWithContext    func(ctx context.Context) error
	writeBatchWithContext func(ctx context.Context, b MessageBatch) error
	close                 func(ctx context.Context) error
}

func (f *fnBatchWriter) Connect(ctx context.Context) error {
	return f.connectWithContext(ctx)
}

func (f *fnBatchWriter) WriteBatch(ctx context.Context, msg MessageBatch) error {
	return f.writeBatchWithContext(ctx, msg)
}

func (f *fnBatchWriter) Close(ctx context.Context) error {
	return f.close(ctx)
}

func TestSpanBatchWriter(t *testing.T) {
	tests := []struct {
		name        string
		outContents string
		mapping     string
	}{
		{
			name:        "mapping succeeds",
			outContents: `{"meow":{}}`,
			mapping:     `root.meow = this`,
		},
		{
			name:        "mapping fails",
			outContents: `{}`,
			mapping:     `root.meow = this.uhhhh.not_null()`,
		},
		{
			name:        "result is deleted",
			outContents: `{}`,
			mapping:     `root = deleted()`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var connCalled, closeCalled, waitCalled bool

			spec := NewConfigSpec().Field(NewInjectTracingSpanMappingField())
			pConf, err := spec.ParseYAML(fmt.Sprintf(`inject_tracing_map: '%v'`, test.mapping), nil)
			require.NoError(t, err)

			r, err := pConf.WrapBatchOutputExtractTracingSpanMapping("foo", &fnBatchWriter{
				connectWithContext: func(ctx context.Context) error {
					connCalled = true
					return nil
				},
				writeBatchWithContext: func(ctx context.Context, b MessageBatch) error {
					assert.Len(t, b, 1)
					msgBytes, err := b[0].AsBytes()
					require.NoError(t, err)
					assert.Equal(t, test.outContents, string(msgBytes))
					return nil
				},
				close: func(ctx context.Context) error {
					closeCalled = true
					waitCalled = true
					return nil
				},
			})
			require.NoError(t, err)

			assert.NoError(t, r.Connect(context.Background()))

			require.NoError(t, r.WriteBatch(context.Background(), MessageBatch{NewMessage([]byte(`{}`))}))

			assert.NoError(t, r.Close(context.Background()))

			assert.True(t, connCalled)
			assert.True(t, closeCalled)
			assert.True(t, waitCalled)
		})
	}
}

type fnWriter struct {
	connectWithContext func(ctx context.Context) error
	writeWithContext   func(ctx context.Context, m *Message) error
	close              func(ctx context.Context) error
}

func (f *fnWriter) Connect(ctx context.Context) error {
	return f.connectWithContext(ctx)
}

func (f *fnWriter) Write(ctx context.Context, m *Message) error {
	return f.writeWithContext(ctx, m)
}

func (f *fnWriter) Close(ctx context.Context) error {
	return f.close(ctx)
}

func TestSpanWriter(t *testing.T) {
	tests := []struct {
		name        string
		outContents string
		mapping     string
	}{
		{
			name:        "mapping succeeds",
			outContents: `{"meow":{}}`,
			mapping:     `root.meow = this`,
		},
		{
			name:        "mapping fails",
			outContents: `{}`,
			mapping:     `root.meow = this.uhhhh.not_null()`,
		},
		{
			name:        "result is deleted",
			outContents: `{}`,
			mapping:     `root = deleted()`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var connCalled, closeCalled, waitCalled bool

			spec := NewConfigSpec().Field(NewInjectTracingSpanMappingField())
			pConf, err := spec.ParseYAML(fmt.Sprintf(`inject_tracing_map: '%v'`, test.mapping), nil)
			require.NoError(t, err)

			r, err := pConf.WrapOutputExtractTracingSpanMapping("foo", &fnWriter{
				connectWithContext: func(ctx context.Context) error {
					connCalled = true
					return nil
				},
				writeWithContext: func(ctx context.Context, m *Message) error {
					msgBytes, err := m.AsBytes()
					require.NoError(t, err)
					assert.Equal(t, test.outContents, string(msgBytes))
					return nil
				},
				close: func(ctx context.Context) error {
					closeCalled = true
					waitCalled = true
					return nil
				},
			})
			require.NoError(t, err)

			assert.NoError(t, r.Connect(context.Background()))

			require.NoError(t, r.Write(context.Background(), NewMessage([]byte(`{}`))))

			assert.NoError(t, r.Close(context.Background()))

			assert.True(t, connCalled)
			assert.True(t, closeCalled)
			assert.True(t, waitCalled)
		})
	}
}
