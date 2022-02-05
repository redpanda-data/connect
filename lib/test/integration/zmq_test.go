//go:build ZMQ4
// +build ZMQ4

package integration

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/integration"
)

var _ = registerIntegrationTest("zeromq", func(t *testing.T) {
	t.Parallel()

	template := `
output:
  zmq4:
    urls:
      - tcp://localhost:$PORT
    bind: false
    socket_type: $VAR1
    poll_timeout: 5s

input:
  zmq4:
    urls:
      - tcp://*:$PORT
    bind: true
    socket_type: $VAR2
    sub_filters: [ $VAR3 ]
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
		integration.StreamTestOptVarOne("PUSH"),
		integration.StreamTestOptVarTwo("PULL"),
	)
	t.Run("with pub sub", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptVarOne("PUB"),
			integration.StreamTestOptVarTwo("SUB"),
			integration.StreamTestOptVarThree(`""`),
		)
	})
})
