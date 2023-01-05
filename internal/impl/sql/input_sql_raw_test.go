package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSQLRawInputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
query: "select * from quack"
args_mapping: 'root = [ this.id ]'
`

	spec := sqlSelectInputConfig()
	env := service.NewEnvironment()

	selectConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newSQLRawInputFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, selectInput.Close(context.Background()))
}
