package pure_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/scanner/testutil"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestJSONScannerDefault(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`{"a":"a0"}
{"a":"a1"}
{"a":"a2"}
{"a":"a3"}
{"a":"a4"}
`),
		`{"a":"a0"}`,
		`{"a":"a1"}`,
		`{"a":"a2"}`,
		`{"a":"a3"}`,
		`{"a":"a4"}`,
	)
}

func TestJSONScannerFormatted(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`{
		"a":"a0"
	}
{
	"a":"a1"
}
{
	"a":"a2"
}
{
	"a":"a3"
}
{
	"a":"a4"
}
`),
		`{"a":"a0"}`,
		`{"a":"a1"}`,
		`{"a":"a2"}`,
		`{"a":"a3"}`,
		`{"a":"a4"}`,
	)
}

func TestJSONScannerNested(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`{"a":{"b":"ab0"}}
{"a":{"b":"ab1"}}
{"a":{"b":"ab2"}}
{"a":{"b":"ab3"}}
{"a":{"b":"ab4"}}
`),
		`{"a":{"b":"ab0"}}`,
		`{"a":{"b":"ab1"}}`,
		`{"a":{"b":"ab2"}}`,
		`{"a":{"b":"ab3"}}`,
		`{"a":{"b":"ab4"}}`,
	)
}

func TestJSONScannerNestedAndFormatted(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`{
	"a": {
		"b": "ab0"
	}
}
{
	"a": {
		"b": "ab1"
	}
}
{
	"a": {
		"b": "ab2"
	}
}
{
	"a": {
		"b": "ab3"
	}
}
{
	"a": {
		"b": "ab4"
	}
}
`),
		`{"a":{"b":"ab0"}}`,
		`{"a":{"b":"ab1"}}`,
		`{"a":{"b":"ab2"}}`,
		`{"a":{"b":"ab3"}}`,
		`{"a":{"b":"ab4"}}`,
	)
}

func TestJSONScannerMultipleValuesAndFormatted(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`{
		"a": "a0",
		"b": "b0"
	}
	{
		"b": "b1",
		"a": "a1"
	}
	{
		"a": "a2",
		"b": "b2"
	}
`),
		`{"a":"a0","b":"b0"}`,
		`{"a":"a1","b":"b1"}`,
		`{"a":"a2","b":"b2"}`,
	)
}

func TestJSONScannerArrayElement(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`{
		"a": ["a0","a1","a2"],
		"b": "b0"
	}
	{
		"a": "a1",
		"b": "b1"
	}
	{
		"a": "a2",
		"b": "b2"
	}
`),
		`{"a":["a0","a1","a2"],"b":"b0"}`,
		`{"a":"a1","b":"b1"}`,
		`{"a":"a2","b":"b2"}`,
	)
}

func TestJSONScannerArray(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  json_documents: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`[
	{
		"a": "a0",
		"b": "b0"
	},
	{
		"a": "a1",
		"b": "b1"
	},
	{
		"a": "a2",
		"b": "b2"
	}
	]
`),
		`[{"a":"a0","b":"b0"},{"a":"a1","b":"b1"},{"a":"a2","b":"b2"}]`,
	)
}
