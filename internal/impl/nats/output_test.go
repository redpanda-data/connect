package nats

import (
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputParse(t *testing.T) {
	spec := natsOutputConfig()
	env := service.NewEnvironment()

	t.Run("Successful config parsing", func(t *testing.T) {
		outputConfig := `
urls: [ url1 ]
subject: foobar
connection_name: my_connection
`

		conf, err := spec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		output, err := newNATSWriter(conf, service.MockResources())
		require.NoError(t, err)

		assert.Equal(t, "url1", output.urls)
		assert.Equal(t, "foobar", output.subjectStrRaw)
		assert.Equal(t, "my_connection", output.connection_name)
	})
}
