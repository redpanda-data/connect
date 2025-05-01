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

package spicedb

import (
	"fmt"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ory/dockertest/v3"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestIntegrationSpiceDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()
	ctx := t.Context()
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	t.Logf("=== Created docker pool")
	pool.MaxWait = time.Second * 60
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "authzed/spicedb",
		Tag:          "v1.37.1",
		ExposedPorts: []string{"50051/tcp"},
		Cmd:          []string{"serve-testing"},
	})
	require.NoError(t, err, "Could not start resource: %s", err)
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})

	uri := fmt.Sprintf("127.0.0.1:%s", resource.GetPort("50051/tcp"))
	confYaml := fmt.Sprintf(`
endpoint: %s
tls:
  enabled: false
cache: test_cache
`, uri)

	wi, resources := watchInputFromConf(t, confYaml)
	client, err := wi.clientConfig.loadSpiceDBClient()
	require.NoError(t, err)

	var schemaZedToken string
	err = pool.Retry(func() error {
		r, err := client.WriteSchema(ctx, &v1.WriteSchemaRequest{
			Schema: `
definition user {}

definition document {
	relation writer: user
	relation reader: user

	/**
	* edit determines whether a user can edit the document
	*/
	permission edit = writer

	/**
	* view determines whether a user can view the document
	*/
	permission view = reader + writer
}`,
		})
		if err != nil {
			return err
		}

		schemaZedToken = r.WrittenAt.Token
		return nil
	})
	require.NoError(t, err)
	t.Logf("=== Zed token: %s", schemaZedToken)
	err = resources.AccessCache(ctx, "test_cache", func(c service.Cache) {
		require.NoError(t, c.Add(ctx, "authzed.com/spicedb/watch/last_zed_token", []byte(schemaZedToken), nil))
	})
	require.NoError(t, err)

	require.NoError(t, pool.Retry(func() error {
		t.Logf("=== Connecting to spicedb...")
		err := wi.Connect(ctx)
		require.NoError(t, err)
		return err
	}))
	t.Logf("=== Connected to spicedb")
	t.Cleanup(func() {
		t.Logf("=== Cleaning up input")
		if err = wi.Close(ctx); err != nil {
			t.Logf("Failed to cleanup input: %v", err)
		}
	})
	t.Run("TestWriteRelationships", func(t *testing.T) {
		_, err = client.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{
			Updates: []*v1.RelationshipUpdate{{
				Operation: v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: &v1.Relationship{
					Resource: &v1.ObjectReference{
						ObjectType: "document",
						ObjectId:   "a",
					},
					Relation: "writer",
					Subject: &v1.SubjectReference{
						Object: &v1.ObjectReference{
							ObjectType: "user",
							ObjectId:   "alice",
						},
					},
				},
			}},
		})
		require.NoError(t, err)
		msg, ack, err := wi.Read(ctx)
		require.NoError(t, err)
		bytes, err := msg.AsBytes()
		require.NoError(t, err)
		var resp = v1.WatchResponse{}
		require.NoError(t, protojson.Unmarshal(bytes, &resp))
		require.Len(t, resp.Updates, 1)
		require.Equal(t, "alice", resp.Updates[0].Relationship.Subject.Object.ObjectId)
		require.Equal(t, "writer", resp.Updates[0].Relationship.Relation)
		require.Equal(t, "document", resp.Updates[0].Relationship.Resource.ObjectType)
		require.Equal(t, "a", resp.Updates[0].Relationship.Resource.ObjectId)
		require.NotEmpty(t, resp.ChangesThrough.Token)
		err = resources.AccessCache(ctx, "test_cache", func(c service.Cache) {
			b, err := c.Get(ctx, "authzed.com/spicedb/watch/last_zed_token")
			require.NoError(t, err)
			require.Equal(t, schemaZedToken, string(b))
		})
		require.NoError(t, err)
		require.NoError(t, ack(ctx, nil))
		err = resources.AccessCache(ctx, "test_cache", func(c service.Cache) {
			b, err := c.Get(ctx, "authzed.com/spicedb/watch/last_zed_token")
			require.NoError(t, err)
			require.Equal(t, resp.ChangesThrough.Token, string(b))
		})
		require.NoError(t, err)
	})
}

func watchInputFromConf(t *testing.T, yml string) (*watchInput, *service.Resources) {
	t.Helper()
	pConf, err := watchInputSpec().ParseYAML(yml, nil)
	require.NoError(t, err, "YAML: %s", yml)
	mockResources := service.MockResources(
		service.MockResourcesOptAddCache("test_cache"),
	)
	o, err := newWatchInput(pConf, mockResources)
	require.NoError(t, err)

	return o, mockResources
}
