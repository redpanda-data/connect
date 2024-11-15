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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/ory/dockertest/v3"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/require"
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
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 60

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "neo4j",
		ExposedPorts: []string{"7687/tcp"},
		Env:          []string{"NEO4J_AUTH=none"},
	})
	require.NoError(t, err, "Could not start resource: %s", err)
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})

	uri := fmt.Sprintf("bolt://127.0.0.1:%s", resource.GetPort("7687/tcp"))
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
	require.NoError(t, pool.Retry(func() error {
		return out.Connect(context.Background())
	}))
	t.Cleanup(func() {
		if err = out.Close(context.Background()); err != nil {
			t.Logf("Failed to cleanup output: %v", err)
		}
	})
	batch := makeBatch(
		`{"state":"OR","city":"Prineville", "population":11000}`,
		`{"state":"OR","city":"Bend", "population":103000}`,
		`{"state":"OR","city":"Portland", "population":635000}`,
		`{"state":"WI","city":"Madison", "population":272000}`,
	)
	require.NoError(t, out.WriteBatch(context.Background(), batch))
	result, err := neo4j.ExecuteQuery(
		context.Background(),
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
