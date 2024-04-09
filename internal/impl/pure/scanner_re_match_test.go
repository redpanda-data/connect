package pure_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/scanner/testutil"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestReMatchScannerSuite(t *testing.T) {
	testREPattern := func(pattern, input string, expected ...string) {
		confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
		pConf, err := confSpec.ParseYAML(fmt.Sprintf(`
test:
  re_match:
    pattern: '%v'
    max_buffer_size: 200
`, pattern), nil)
		require.NoError(t, err)

		rdr, err := pConf.FieldScanner("test")
		require.NoError(t, err)

		testutil.ScannerTestSuite(t, rdr, nil, []byte(input), expected...)
	}

	testREPattern("(?m)^", "foo\nbar\nbaz", "foo\n", "bar\n", "baz")

	testREPattern("split", "foo\nbar\nsplit\nbaz\nsplitsplit", "foo\nbar\n", "split\nbaz\n", "split", "split")

	testREPattern("\\n", "split", "split")
	testREPattern("split", "split", "split")

	testREPattern("\\n", "foo\nbar\nsplit\nbaz\nsplitsplit", "foo", "\nbar", "\nsplit", "\nbaz", "\nsplitsplit")

	testREPattern("\\n", "foo\nbar\nsplit\nbaz", "foo", "\nbar", "\nsplit", "\nbaz")

	testREPattern("\\n\\d", "20:20:22 ERROR\nCode\n20:20:21 INFO\n20:20:21 INFO\n20:20:22 ERROR\nCode\n", "20:20:22 ERROR\nCode", "\n20:20:21 INFO", "\n20:20:21 INFO", "\n20:20:22 ERROR\nCode\n")

	testREPattern("(?m)^\\d\\d:\\d\\d:\\d\\d", "20:20:22 ERROR\nCode\n20:20:21 INFO\n20:20:21 INFO\n20:20\n20:20:22 ERROR\nCode\n2022", "20:20:22 ERROR\nCode\n", "20:20:21 INFO\n", "20:20:21 INFO\n20:20\n", "20:20:22 ERROR\nCode\n2022")

	testREPattern("split", "")
}
