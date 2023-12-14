package pure_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/scanner/testutil"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestLinesScanner(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  lines:
    custom_delimiter: 'X'
    max_buffer_size: 200
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	buf := bytes.NewReader([]byte(`firstXsecondXthird`))
	var acked bool
	strm, err := rdr.Create(io.NopCloser(buf), func(ctx context.Context, err error) error {
		acked = true
		return nil
	}, service.NewScannerSourceDetails())
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

func TestLinesScannerSuite(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  lines:
    custom_delimiter: 'X'
    max_buffer_size: 200
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`firstXsecondXthird`), "first", "second", "third")

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`firstXsecondXXthird`), "first", "second", "", "third")
}
