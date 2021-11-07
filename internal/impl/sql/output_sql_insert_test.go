package sql

import (
	"context"
	"testing"

	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/stretchr/testify/require"
)

func TestSQLInsertOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
columns: [ foo ]
args_mapping: 'root = [ this.id ]'
`

	spec := sqlInsertOutputConfig()
	env := service.NewEnvironment()

	insertConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	insertOutput, err := newSQLInsertOutputFromConfig(insertConfig, nil)
	require.NoError(t, err)
	require.NoError(t, insertOutput.Close(context.Background()))
}
