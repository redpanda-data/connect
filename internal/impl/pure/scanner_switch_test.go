package pure_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/scanner/testutil"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSwitchScanner(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  switch:
    - re_match_name: '\.json$'
      scanner: { to_the_end: {} }
    - re_match_name: '\.csv$'
      scanner: { csv: {} }
    - re_match_name: '\.chunks$'
      scanner:
        chunker:
          size: 4
    - scanner: { to_the_end: {} }

`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	details := service.NewScannerSourceDetails()
	details.SetName("a/b/foo.csv")
	testutil.ScannerTestSuite(t, rdr, details, []byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
	)

	details = service.NewScannerSourceDetails()
	details.SetName("woof/meow.chunks")
	testutil.ScannerTestSuite(t, rdr, details, []byte(`abcdefghijklmnopqrstuvwxyz`), "abcd", "efgh", "ijkl", "mnop", "qrst", "uvwx", "yz")

	details = service.NewScannerSourceDetails()
	details.SetName("./meow.json")
	testutil.ScannerTestSuite(t, rdr, details, []byte(`{"hello":"world"}`), `{"hello":"world"}`)
}
