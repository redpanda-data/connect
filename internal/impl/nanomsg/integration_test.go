package nanomsg

import (
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNanomsg(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	template := `
output:
  nanomsg:
    urls:
      - tcp://localhost:$PORT
    bind: false
    socket_type: $VAR1
    poll_timeout: 5s
    max_in_flight: $MAX_IN_FLIGHT

input:
  nanomsg:
    urls:
      - tcp://0.0.0.0:$PORT
    bind: true
    socket_type: $VAR2
    sub_filters: [ $VAR3 ]
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
		integration.StreamTestOptVarSet("VAR1", "PUSH"),
		integration.StreamTestOptVarSet("VAR2", "PULL"),
	)
	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptVarSet("VAR1", "PUSH"),
			integration.StreamTestOptVarSet("VAR2", "PULL"),
			integration.StreamTestOptMaxInFlight(10),
		)
	})
	t.Run("with pub sub", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptSleepAfterInput(500*time.Millisecond),
			integration.StreamTestOptSleepAfterOutput(500*time.Millisecond),
			integration.StreamTestOptVarSet("VAR1", "PUB"),
			integration.StreamTestOptVarSet("VAR2", "SUB"),
			integration.StreamTestOptVarSet("VAR3", `""`),
		)
	})
}
