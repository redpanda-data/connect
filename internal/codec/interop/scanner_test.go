package interop_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestInteropCodecOldStyle(t *testing.T) {
	confSpec := service.NewConfigSpec().Fields(interop.OldReaderCodecFields("lines")...)
	pConf, err := confSpec.ParseYAML(`
codec: lines
max_buffer: 1000000
`, nil)
	require.NoError(t, err)

	rdr, err := interop.OldReaderCodecFromParsed(pConf)
	require.NoError(t, err)

	buf := bytes.NewReader([]byte(`first
second
third`))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, scanner.SourceDetails{})
	require.NoError(t, err)

	for _, s := range []string{
		"first", "second", "third",
	} {
		m, aFn, err := strm.NextBatch(context.Background())
		require.NoError(t, err)
		require.Len(t, m, 1)
		mBytes, err := m[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, s, string(mBytes))
		require.NoError(t, aFn(context.Background(), nil))
		assert.False(t, acked)
	}

	_, _, err = strm.NextBatch(context.Background())
	require.Equal(t, io.EOF, err)

	require.NoError(t, strm.Close(context.Background()))
	assert.True(t, acked)
}

func TestInteropCodecNewStyle(t *testing.T) {
	confSpec := service.NewConfigSpec().Fields(interop.OldReaderCodecFields("lines")...)
	pConf, err := confSpec.ParseYAML(`
scanner:
  lines:
    custom_delimiter: 'X'
    max_buffer_size: 200
`, nil)
	require.NoError(t, err)

	rdr, err := interop.OldReaderCodecFromParsed(pConf)
	require.NoError(t, err)

	buf := bytes.NewReader([]byte(`firstXsecondXthird`))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, scanner.SourceDetails{})
	require.NoError(t, err)

	for _, s := range []string{
		"first", "second", "third",
	} {
		m, aFn, err := strm.NextBatch(context.Background())
		require.NoError(t, err)
		require.Len(t, m, 1)
		mBytes, err := m[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, s, string(mBytes))
		require.NoError(t, aFn(context.Background(), nil))
		assert.False(t, acked)
	}

	_, _, err = strm.NextBatch(context.Background())
	require.Equal(t, io.EOF, err)

	require.NoError(t, strm.Close(context.Background()))
	assert.True(t, acked)
}

func TestInteropCodecDefault(t *testing.T) {
	confSpec := service.NewConfigSpec().Fields(interop.OldReaderCodecFields("lines")...)
	pConf, err := confSpec.ParseYAML(`{}`, nil)
	require.NoError(t, err)

	rdr, err := interop.OldReaderCodecFromParsed(pConf)
	require.NoError(t, err)

	buf := bytes.NewReader([]byte("first\nsecond\nthird"))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, scanner.SourceDetails{})
	require.NoError(t, err)

	for _, s := range []string{
		"first", "second", "third",
	} {
		m, aFn, err := strm.NextBatch(context.Background())
		require.NoError(t, err)
		require.Len(t, m, 1)
		mBytes, err := m[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, s, string(mBytes))
		require.NoError(t, aFn(context.Background(), nil))
		assert.False(t, acked)
	}

	_, _, err = strm.NextBatch(context.Background())
	require.Equal(t, io.EOF, err)

	require.NoError(t, strm.Close(context.Background()))
	assert.True(t, acked)
}
