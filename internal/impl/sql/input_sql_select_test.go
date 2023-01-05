package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestSQLSelectInputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
columns: [ foo, bar, baz ]
where: foo = ?
args_mapping: 'root = [ this.id ]'
`

	spec := sqlSelectInputConfig()
	env := service.NewEnvironment()

	selectConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newSQLSelectInputFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, selectInput.Close(context.Background()))
}
