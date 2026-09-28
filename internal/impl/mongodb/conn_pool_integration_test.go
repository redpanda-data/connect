// Copyright 2026 Redpanda Data, Inc.
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

// This file is package mongodb rather than mongodb_test because the drift
// canary below probes the unexported isConnPoolError against live driver
// errors, which the black-box integration_test.go package cannot reach.

package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// TestIntegrationConnPoolErrorShapes is a drift canary for isConnPoolError.
//
// TestIsConnPoolError in common_test.go pins the classifier against error
// strings captured by hand from mongo-driver v2.5.0, which means a driver bump
// that reworded any of those messages would leave that test passing while the
// classifier silently stopped recognising real failures. This test feeds
// isConnPoolError errors produced by the live driver against a real server
// instead, so the same bump fails CI. The literal substring assertions are
// deliberate: they name which message shape moved.
func TestIntegrationConnPoolErrorShapes(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)
	port := mp.Port()

	connect := func(t *testing.T, uri string, opt func(*options.ClientOptions)) *mongo.Client {
		t.Helper()
		o := options.Client().
			SetConnectTimeout(5 * time.Second).
			SetTimeout(10 * time.Second).
			SetServerSelectionTimeout(10 * time.Second).
			ApplyURI(uri)
		if opt != nil {
			opt(o)
		}
		client, err := mongo.Connect(o)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
		return client
	}

	// Wait for the server to accept authenticated connections before probing
	// error shapes, otherwise a not-yet-ready server produces the wrong error.
	good := connect(t, "mongodb://localhost:"+port, func(o *options.ClientOptions) {
		o.SetAuth(options.Credential{Username: "mongoadmin", Password: "secret"})
	})
	require.Eventually(t, func() bool {
		return good.Ping(t.Context(), nil) == nil
	}, time.Minute, time.Second)

	t.Run("handshake_auth_failure", func(t *testing.T) {
		bad := connect(t, "mongodb://localhost:"+port, func(o *options.ClientOptions) {
			o.SetAuth(options.Credential{Username: "mongoadmin", Password: "wrong-password"})
		})
		_, err := bad.Database("TestDB").Collection("handshake").InsertOne(t.Context(), bson.M{"a": 1})
		require.Error(t, err)
		t.Logf("handshake failure error: %v", err)
		// Names the exact substring isConnPoolError depends on, so a driver
		// rewording points at this line rather than at the classifier.
		require.Contains(t, err.Error(), "error occurred during connection handshake")
		require.True(t, isConnPoolError(err), "real handshake failure must be classified as a pool error: %v", err)
	})

	t.Run("duplicate_key_is_not_a_pool_error", func(t *testing.T) {
		coll := good.Database("TestDB").Collection("dupkey")
		_, err := coll.InsertOne(t.Context(), bson.M{"_id": "dup"})
		require.NoError(t, err)
		_, err = coll.InsertOne(t.Context(), bson.M{"_id": "dup"})
		require.Error(t, err)
		t.Logf("duplicate key error: %v", err)
		require.True(t, mongo.IsDuplicateKeyError(err), "expected a duplicate key error, got: %v", err)
		require.False(t, isConnPoolError(err), "an ordinary write error must not trigger a reconnect: %v", err)
	})

	t.Run("server_selection_failure", func(t *testing.T) {
		// Port 1 is never a mongod, so every operation fails in server
		// selection rather than reaching a server.
		dead := connect(t, "mongodb://localhost:1", func(o *options.ClientOptions) {
			o.SetServerSelectionTimeout(2 * time.Second).SetTimeout(5 * time.Second)
		})
		_, err := dead.Database("TestDB").Collection("dead").InsertOne(t.Context(), bson.M{"a": 1})
		require.Error(t, err)
		t.Logf("server selection error: %v", err)
		require.Contains(t, err.Error(), "server selection error")
		require.True(t, isConnPoolError(err), "an unselectable server must be classified as a pool error: %v", err)
	})
}
