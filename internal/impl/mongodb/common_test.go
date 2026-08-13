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

package mongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIsConnPoolError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "handshake auth failure",
			err:  errors.New(`connection(cluster0-shard-00-01.mongodb.net:27017[-12]) error occurred during connection handshake: auth error: sasl conversation error: unable to authenticate using mechanism "MONGODB-AWS": (AuthenticationFailed) Authentication failed.`),
			want: true,
		},
		{
			name: "pool cleared",
			err:  errors.New("connection pool for cluster0-shard-00-01.mongodb.net:27017 was cleared because another operation failed with: connection() error occurred during connection handshake: auth error"),
			want: true,
		},
		{
			name: "server selection",
			err:  errors.New("server selection error: context deadline exceeded, current topology: { ... }"),
			want: true,
		},
		{
			name: "network error label",
			err:  mongo.CommandError{Labels: []string{"NetworkError"}, Message: "socket was unexpectedly closed"},
			want: true,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "duplicate key",
			err:  mongo.CommandError{Code: 11000, Message: "E11000 duplicate key error collection: foo.bar"},
			want: false,
		},
		{
			name: "validation failure",
			err:  errors.New("document validation failure"),
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, isConnPoolError(test.err))
		})
	}
}

// TestIntegrationConnPoolErrorShapes is a drift canary for isConnPoolError.
//
// TestIsConnPoolError above pins the classifier against error strings captured
// by hand from mongo-driver v2.5.0, which means a driver bump that reworded any
// of those messages would leave that test passing while the classifier silently
// stopped recognising real failures. This test feeds isConnPoolError errors
// produced by the live driver against a real server instead, so the same bump
// fails CI. The literal substring assertions are deliberate: they name which
// message shape moved.
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

func parseInputConf(t *testing.T, conf string) *service.ParsedConfig {
	t.Helper()
	parsed, err := mongoConfigSpec().ParseYAML(conf, service.NewEnvironment())
	require.NoError(t, err)
	return parsed
}

const inputConfBoilerplate = `
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`

func TestAWSAuthRejectsStaticCredentials(t *testing.T) {
	parsed := parseInputConf(t, inputConfBoilerplate+`
username: foouser
password: foopass
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "username and password cannot be set when aws.enabled is true")
}

func TestAWSAuthNotImportedError(t *testing.T) {
	parsed := parseInputConf(t, inputConfBoilerplate+`
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "does not import components/aws")
}

func TestAWSAuthRejectsURLCredentials(t *testing.T) {
	parsed := parseInputConf(t, `
url: "mongodb://foouser:foopass@localhost:27017"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
aws:
  enabled: true
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "credentials embedded in the url cannot be combined with aws.enabled")
}

func TestAWSAuthAllowsAuthMechanismInURL(t *testing.T) {
	// A url which only names the mechanism - the shape Atlas suggests for IAM
	// users - carries no credentials and must be accepted. Stand in for the aws
	// bundle so construction can complete without importing it here.
	prevOptFn := AWSOptFn
	t.Cleanup(func() { AWSOptFn = prevOptFn })
	AWSOptFn = func(*service.ParsedConfig, *service.Logger) (CredentialBuilder, error) {
		return func(context.Context) (*options.Credential, error) {
			return &options.Credential{AuthMechanism: "MONGODB-AWS"}, nil
		}, nil
	}

	parsed := parseInputConf(t, `
url: "mongodb://localhost:27017/?authSource=%24external&authMechanism=MONGODB-AWS"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
aws:
  enabled: true
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}

func TestInvalidURLRejected(t *testing.T) {
	parsed := parseInputConf(t, `
url: "not-a-mongo-uri"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`)
	_, err := newMongoInput(parsed, service.MockResources().Logger())
	require.ErrorContains(t, err, "invalid url")
}

func TestAWSAuthDisabledKeepsStaticCredentials(t *testing.T) {
	parsed := parseInputConf(t, inputConfBoilerplate+`
username: foouser
password: foopass
aws:
  enabled: false
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}

// stubAWSOptFn swaps in a no-op AWSOptFn for the duration of the test, since
// the mongodb package does not import the aws subpackage and would otherwise
// hit the "does not import components/aws" stub error before the checks under
// test get a chance to run.
func stubAWSOptFn(t *testing.T) {
	t.Helper()
	prev := AWSOptFn
	t.Cleanup(func() { AWSOptFn = prev })
	AWSOptFn = func(*service.ParsedConfig, *service.Logger) (CredentialBuilder, error) {
		return func(context.Context) (*options.Credential, error) {
			return &options.Credential{AuthMechanism: "MONGODB-AWS"}, nil
		}, nil
	}
}

const processorConfBoilerplate = `
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
operation: find-one
filter_map: |
  root.a = this.a
`

func TestAWSAuthProcessorRejectsRoleAssumption(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := ProcessorSpec().ParseYAML(processorConfBoilerplate+`
aws:
  enabled: true
  role: arn:aws:iam::123456789012:role/foo
`, service.NewEnvironment())
	require.NoError(t, err)

	_, err = ProcessorFromParsed(conf, service.MockResources())
	require.ErrorContains(t, err, "cannot be used with the mongodb processor")
}

func TestAWSAuthCacheRejectsRoleAssumption(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := mongodbCacheConfig().ParseYAML(`
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
key_field: "k"
value_field: "v"
aws:
  enabled: true
  role: arn:aws:iam::123456789012:role/foo
`, service.NewEnvironment())
	require.NoError(t, err)

	_, err = newMongodbCacheFromConfig(conf, service.MockResources().Logger())
	require.ErrorContains(t, err, "cannot be used with the mongodb cache")
}

func TestAWSAuthCacheAcceptsAmbientCredentials(t *testing.T) {
	stubAWSOptFn(t)
	conf, err := mongodbCacheConfig().ParseYAML(`
url: "mongodb://localhost:27017"
database: "foo"
collection: "bar"
key_field: "k"
value_field: "v"
aws:
  enabled: true
`, service.NewEnvironment())
	require.NoError(t, err)

	cache, err := newMongodbCacheFromConfig(conf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, cache.Close(t.Context()))
}

func TestAWSAuthInputAcceptsRoleAssumption(t *testing.T) {
	stubAWSOptFn(t)
	parsed := parseInputConf(t, inputConfBoilerplate+`
aws:
  enabled: true
  role: arn:aws:iam::123456789012:role/foo
`)
	input, err := newMongoInput(parsed, service.MockResources().Logger())
	require.NoError(t, err)
	require.NoError(t, input.Close(t.Context()))
}
