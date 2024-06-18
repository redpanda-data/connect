package nsq

import (
	"net"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegration(t *testing.T) {
	t.Parallel()

	{
		timeout := time.Second
		conn, err := net.DialTimeout("tcp", "localhost:4150", timeout)
		if err != nil {
			t.Skip("Skipping NSQ tests as services are not running")
		}
		conn.Close()
	}

	template := `
output:
  nsq:
    nsqd_tcp_address: localhost:4150
    topic: topic-$ID
    # user_agent: ""
    max_in_flight: $MAX_IN_FLIGHT

input:
  nsq:
    nsqd_tcp_addresses: [ localhost:4150 ]
    lookupd_http_addresses: [ localhost:4160 ^]
    topic: topic-$ID
    channel: channel-$ID
    # user_agent: ""
    max_in_flight: 100
    max_attempts: 5
`
	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
	)
	suite.Run(t, template)

	t.Run("with max in flight", func(t *testing.T) {
		t.Parallel()
		suite.Run(t, template, integration.StreamTestOptMaxInFlight(10))
	})
}
