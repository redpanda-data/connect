// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hdfs

import (
	"runtime"
	"testing"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationHDFS(t *testing.T) {
	integration.CheckSkip(t)
	if runtime.GOOS == "darwin" {
		t.Skip("CON-377: HDFS datanode networking incompatible with Docker on macOS")
	}

	// Not parallel: HDFS requires fixed port bindings because the namenode
	// reports localhost as the datanode address, and the client connects
	// to the datanode directly on the host-mapped ports.

	ctr, err := testcontainers.Run(t.Context(), "cybermaggedon/hadoop:2.8.2",
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithExposedPorts("9000/tcp", "50070/tcp", "50075/tcp", "50010/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = mobynet.PortMap{
				mobynet.MustParsePort("9000/tcp"):  {{HostPort: "19000"}},
				mobynet.MustParsePort("50070/tcp"): {{HostPort: "19070"}},
				mobynet.MustParsePort("50075/tcp"): {{HostPort: "19075"}},
				mobynet.MustParsePort("50010/tcp"): {{HostPort: "19010"}},
			}
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(5*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		testFile := "/cluster_ready" + time.Now().Format("20060102150405")
		client, err := hdfs.NewClient(hdfs.ClientOptions{
			Addresses: []string{"localhost:19000"},
			User:      "root",
		})
		if err != nil {
			return false
		}
		fw, err := client.Create(testFile)
		if err != nil {
			return false
		}
		if _, err := fw.Write([]byte("testing hdfs reader")); err != nil {
			return false
		}
		if err := fw.Close(); err != nil {
			return false
		}
		_ = client.Remove(testFile)
		return true
	}, 5*time.Minute, 2*time.Second, "HDFS cluster not ready")

	template := `
output:
  hdfs:
    hosts: [ localhost:19000 ]
    user: root
    directory: /$ID
    path: ${!counter()}-${!timestamp_unix_nano()}.txt
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  hdfs:
    hosts: [ localhost:19000 ]
    user: root
    directory: /$ID
`
	integration.StreamTests(
		integration.StreamTestOpenCloseIsolated(),
		integration.StreamTestStreamIsolated(10),
		integration.StreamTestSendBatchCountIsolated(10),
	).Run(t, template)
}
