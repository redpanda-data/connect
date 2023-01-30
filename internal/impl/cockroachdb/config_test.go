package crdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestCRDBConfigParse(t *testing.T) {
	conf := `
cockroach_changefeed:
dsn: postgresql://dan:xxxx@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/defaultdb?sslmode=require&options=--cluster%3Dportly-impala-2852
tables:
    - strm_2
options:
    - UPDATED
    - CURSOR='1637953249519902405.0000000000'
`

	spec := crdbChangefeedInputConfig()
	env := service.NewEnvironment()

	selectConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newCRDBChangefeedInputFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "EXPERIMENTAL CHANGEFEED FOR strm_2 WITH UPDATED, CURSOR='1637953249519902405.0000000000'", selectInput.statement)
	require.NoError(t, selectInput.Close(context.Background()))
}
