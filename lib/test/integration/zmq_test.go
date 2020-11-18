// +build ZMQ4

package integration

import (
	"testing"
	"time"
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
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestStreamParallel(100),
	)
	suite.Run(
		t, template,
		testOptSleepAfterInput(500*time.Millisecond),
		testOptSleepAfterOutput(500*time.Millisecond),
		testOptVarOne("PUSH"),
		testOptVarTwo("PULL"),
	)
	t.Run("with pub sub", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			testOptSleepAfterInput(500*time.Millisecond),
			testOptSleepAfterOutput(500*time.Millisecond),
			testOptVarOne("PUB"),
			testOptVarTwo("SUB"),
			testOptVarThree(`""`),
		)
	})
})
