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
	"strings"
	"testing"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/moby/moby/api/types/container"
	mobynet "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// hdfsSiteXML overrides the image's default hdfs-site.xml so the datanode is
// reachable from the host. By default the datanode registers under its
// container hostname (e.g. "d9abe2180ff7"), which the HDFS client resolves and
// dials directly for block transfers - this fails from the host with "no such
// host". Setting dfs.datanode.hostname to localhost and pinning the transfer
// port lets the client reach it via a fixed host-mapped port on every platform,
// including Docker Desktop on macOS. See CON-377.
const hdfsSiteXML = `<configuration>
    <property><name>dfs.replication</name><value>1</value></property>
    <property><name>dfs.data.dir</name><value>/data/hadoop/data</value></property>
    <property><name>dfs.name.dir</name><value>/data/hadoop/name</value></property>
    <property><name>dfs.namenode.rpc-bind-host</name><value>0.0.0.0</value></property>
    <property><name>dfs.namenode.servicerpc-bind-host</name><value>0.0.0.0</value></property>
    <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
    <property><name>dfs.datanode.hostname</name><value>localhost</value></property>
    <property><name>dfs.datanode.address</name><value>0.0.0.0:19010</value></property>
</configuration>
`

func TestIntegrationHDFS(t *testing.T) {
	integration.CheckSkip(t)

	// Not parallel: HDFS requires fixed port bindings because the datanode is
	// configured to advertise localhost (see hdfsSiteXML) and the client
	// connects to it directly on a host-mapped port matching its transfer port.

	ctr, err := testcontainers.Run(t.Context(), "cybermaggedon/hadoop:2.8.2",
		testcontainers.WithImagePlatform("linux/amd64"),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(hdfsSiteXML),
			ContainerFilePath: "/usr/local/hadoop/etc/hadoop/hdfs-site.xml",
			FileMode:          0o644,
		}),
		testcontainers.WithExposedPorts("9000/tcp", "50070/tcp", "50075/tcp", "19010/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = mobynet.PortMap{
				mobynet.MustParsePort("9000/tcp"):  {{HostPort: "19000"}},
				mobynet.MustParsePort("50070/tcp"): {{HostPort: "19070"}},
				mobynet.MustParsePort("50075/tcp"): {{HostPort: "19075"}},
				mobynet.MustParsePort("19010/tcp"): {{HostPort: "19010"}},
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
