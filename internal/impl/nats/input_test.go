package nats

import (
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInputParse(t *testing.T) {
	spec := natsInputConfig()
	env := service.NewEnvironment()

	t.Run("Successful config parsing", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
subject: foobar
connection_name: my_connection
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		input, err := newNATSReader(conf, service.MockResources())
		require.NoError(t, err)

		assert.Equal(t, "url1", input.urls)
		assert.Equal(t, "foobar", input.subject)
		assert.Equal(t, "my_connection", input.connection_name)
	})
}
