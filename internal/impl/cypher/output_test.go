// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cypher

import (
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func outputFromConf(t *testing.T, confStr string, args ...any) *output {
	t.Helper()

	yml := fmt.Sprintf(confStr, args...)
	pConf, err := outputConfig().ParseYAML(yml, nil)
	require.NoError(t, err, "YAML: %s", yml)

	o, err := newCypherOutput(pConf, service.MockResources())
	require.NoError(t, err)

	return o
}

func makeBatch(args ...string) service.MessageBatch {
	batch := make(service.MessageBatch, len(args))
	for i, arg := range args {
		batch[i] = service.NewMessage([]byte(arg))
	}
	return batch
}

func TestIntegrationCypher(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "neo4j:latest",
		testcontainers.WithExposedPorts("7687/tcp"),
		testcontainers.WithEnv(map[string]string{"NEO4J_AUTH": "none"}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("7687/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	mappedPort, err := ctr.MappedPort(t.Context(), "7687/tcp")
	require.NoError(t, err)

	uri := fmt.Sprintf("bolt://%s:%s", host, mappedPort.Port())
	out := outputFromConf(t, `
uri: %s
cypher: |
  MERGE  (s:State {name: $st})
  CREATE (c:City {name: $cit, population_size: $pop})
  CREATE (s)<-[r:IN]-(c)
args_mapping: |
  root = {}
  root.st = this.state
  root.cit = this.city
  root.pop = this.population
    `, uri)
	require.Eventually(t, func() bool {
		return out.Connect(t.Context()) == nil
	}, 60*time.Second, time.Second)
	t.Cleanup(func() {
		if err = out.Close(t.Context()); err != nil {
			t.Logf("Failed to cleanup output: %v", err)
		}
	})
	batch := makeBatch(
		`{"state":"OR","city":"Prineville", "population":11000}`,
		`{"state":"OR","city":"Bend", "population":103000}`,
		`{"state":"OR","city":"Portland", "population":635000}`,
		`{"state":"WI","city":"Madison", "population":272000}`,
	)
	require.NoError(t, out.WriteBatch(t.Context(), batch))
	result, err := neo4j.ExecuteQuery(
		t.Context(),
		out.driver,
		`
    MATCH (c:City)-[:IN]->(:State{name:"OR"})
    RETURN c.name AS city, c.population_size AS pop
    `,
		nil,
		neo4j.EagerResultTransformer,
	)
	require.NoError(t, err)
	resultMap := map[any]any{}
	for _, record := range result.Records {
		t.Log(record.AsMap())
		city, ok := record.Get("city")
		require.True(t, ok, "record missing city: %v", record.AsMap())
		pop, ok := record.Get("pop")
		require.True(t, ok, "record missing pop: %v", record.AsMap())
		resultMap[city] = pop
	}
	require.Equal(t, map[any]any{
		"Prineville": "11000",
		"Portland":   "635000",
		"Bend":       "103000",
	}, resultMap)
}
