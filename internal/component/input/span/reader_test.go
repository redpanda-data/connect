package span

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type fnBatchReader struct {
	connectWithContext   func(ctx context.Context) error
	readBatchWithContext func(ctx context.Context) (service.MessageBatch, service.AckFunc, error)
	close                func(ctx context.Context) error
}

func (f *fnBatchReader) Connect(ctx context.Context) error {
	return f.connectWithContext(ctx)
}

func (f *fnBatchReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return f.readBatchWithContext(ctx)
}

func (f *fnBatchReader) Close(ctx context.Context) error {
	return f.close(ctx)
}

func TestSpanBatchReader(t *testing.T) {
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
			mapping:  `root = "this isnt json"`,
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

			spec := service.NewConfigSpec().Field(ExtractTracingSpanMappingDocs())
			pConf, err := spec.ParseYAML(fmt.Sprintf(`extract_tracing_map: '%v'`, test.mapping), nil)
			require.NoError(t, err)

			r, err := NewBatchInput("foo", pConf, &fnBatchReader{
				connectWithContext: func(ctx context.Context) error {
					connCalled = true
					return nil
				},
				readBatchWithContext: func(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
					m := service.MessageBatch{
						service.NewMessage([]byte(test.contents)),
					}
					return m, func(context.Context, error) error {
						return nil
					}, nil
				},
				close: func(ctx context.Context) error {
					closeCalled = true
					waitCalled = true
					return nil
				},
			}, service.MockResources())
			require.NoError(t, err)

			assert.NoError(t, r.Connect(context.Background()))

			res, _, err := r.ReadBatch(context.Background())
			require.NoError(t, err)
			assert.Len(t, res, 1)

			rBytes, err := res[0].AsBytes()
			require.NoError(t, err)
			assert.Equal(t, test.contents, string(rBytes))

			assert.NoError(t, r.Close(context.Background()))

			assert.True(t, connCalled)
			assert.True(t, closeCalled)
			assert.True(t, waitCalled)
		})
	}
}

type fnReader struct {
	connectWithContext func(ctx context.Context) error
	readWithContext    func(ctx context.Context) (*service.Message, service.AckFunc, error)
	close              func(ctx context.Context) error
}

func (f *fnReader) Connect(ctx context.Context) error {
	return f.connectWithContext(ctx)
}

func (f *fnReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
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
			mapping:  `root = "this isnt json"`,
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

			spec := service.NewConfigSpec().Field(ExtractTracingSpanMappingDocs())
			pConf, err := spec.ParseYAML(fmt.Sprintf(`extract_tracing_map: '%v'`, test.mapping), nil)
			require.NoError(t, err)

			r, err := NewInput("foo", pConf, &fnReader{
				connectWithContext: func(ctx context.Context) error {
					connCalled = true
					return nil
				},
				readWithContext: func(ctx context.Context) (*service.Message, service.AckFunc, error) {
					m := service.NewMessage([]byte(test.contents))
					return m, func(context.Context, error) error {
						return nil
					}, nil
				},
				close: func(ctx context.Context) error {
					closeCalled = true
					waitCalled = true
					return nil
				},
			}, service.MockResources())
			require.NoError(t, err)

			assert.NoError(t, r.Connect(context.Background()))

			msg, _, err := r.Read(context.Background())
			require.NoError(t, err)

			rBytes, err := msg.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, test.contents, string(rBytes))

			assert.NoError(t, r.Close(context.Background()))

			assert.True(t, connCalled)
			assert.True(t, closeCalled)
			assert.True(t, waitCalled)
		})
	}
}
