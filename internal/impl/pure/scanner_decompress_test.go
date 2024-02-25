package pure_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/scanner/testutil"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestDecompressScannerSuite(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  decompress:
    algorithm: gzip
    into:
      lines:
        custom_delimiter: X
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	inputBytes, err := hex.DecodeString("1f8b080000096e8800ff001e00e1ff68656c6c6f58776f726c64587468697358697358636f6d7072657373656403009104d92d1e000000")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, inputBytes, "hello", "world", "this", "is", "compressed")
}
