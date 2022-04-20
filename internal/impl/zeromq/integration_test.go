//go:build x_benthos_extra
// +build x_benthos_extra

package zeromq

import (
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationZMQ(t *testing.T) {
	integration.CheckSkip(t)
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
}
