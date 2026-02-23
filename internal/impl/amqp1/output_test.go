package amqp1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestAMQP1ConfigParsing(t *testing.T) {
	spec := amqp1OutputSpec()
	env := service.NewEnvironment()

	t.Run("All options omitted (backward compatible)", func(t *testing.T) {
		inputConfig := `urls:
  - "amqp://localhost:5672"
target_address: "/queue"`
		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)
		w, err := amqp1WriterFromParsed(conf, service.MockResources())
		require.False(t, w.persistent)
		require.Nil(t, w.msgTo)
		require.Empty(t, w.senderOpts.TargetCapabilities)
		require.NoError(t, err)
	})

	t.Run("All new options set", func(t *testing.T) {
		inputConfig := `urls:
  - "amqp://localhost:5672"
target_address: "/queue"
target_capabilities:
  - "queue"
  - "topic"
message_properties_to: "amqp://otherhost:5672/otherqueue"
persistent: true`
		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)
		w, wErr := amqp1WriterFromParsed(conf, service.MockResources())
		require.NoError(t, wErr)
		require.True(t, w.persistent)
		require.Equal(t, []string{"queue", "topic"}, w.senderOpts.TargetCapabilities)
		require.NotNil(t, w.msgTo)
		msgToStr, isStatic := w.msgTo.Static()
		require.True(t, isStatic)
		require.Equal(t, "amqp://otherhost:5672/otherqueue", msgToStr)
		require.True(t, w.persistent)
	})

	t.Run("Invalid type for persistent", func(t *testing.T) {
		inputConfig := `urls:
  - "amqp://localhost:5672"
target_address: "/queue"
persistent: "notabool"`
		_, err := spec.ParseYAML(inputConfig, env)
		require.Error(t, err)
	})

	t.Run("Anonymous Terminus with static message_properties_to", func(t *testing.T) {
		inputConfig := `urls:
  - "amqp://localhost:5672"
target_address: ""
message_properties_to: "queue:/my-destination"`
		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)
		w, wErr := amqp1WriterFromParsed(conf, service.MockResources())
		require.NoError(t, wErr)
		require.Empty(t, w.targetAddr)
		require.NotNil(t, w.msgTo)
		msgToStr, isStatic := w.msgTo.Static()
		require.True(t, isStatic)
		require.Equal(t, "queue:/my-destination", msgToStr)
	})

	t.Run("Anonymous Terminus with interpolated message_properties_to", func(t *testing.T) {
		inputConfig := `urls:
  - "amqp://localhost:5672"
target_address: ""
message_properties_to: '${! meta("target_queue") }'`
		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)
		w, wErr := amqp1WriterFromParsed(conf, service.MockResources())
		require.NoError(t, wErr)
		require.Empty(t, w.targetAddr)
		require.NotNil(t, w.msgTo)
		_, isStatic := w.msgTo.Static()
		require.False(t, isStatic, "message_properties_to should be dynamic/interpolated")
	})

	t.Run("Default empty target_address without message_properties_to", func(t *testing.T) {
		inputConfig := `urls:
  - "amqp://localhost:5672"`
		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)
		w, wErr := amqp1WriterFromParsed(conf, service.MockResources())
		require.NoError(t, wErr)
		require.Empty(t, w.targetAddr)
		require.Nil(t, w.msgTo)
		// This config is valid - it will use Anonymous Terminus with no message_properties_to
		// The To property would need to be set programmatically or the sender will fail
	})
}
