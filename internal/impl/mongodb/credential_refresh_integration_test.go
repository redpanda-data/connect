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

// These tests pin the credential-refresh-on-reconnect guarantee against a live
// server: a component configured to assume a role must re-run its credential
// builder on every reconnect, because MongoDB never re-authenticates an
// established pool connection and a role-derived STS session can expire while
// the client is idle. The regression they guard is the one fixed in cdd8cff,
// where a successful ping let the old client (and its stale credential) be
// reused.
//
// The file is package mongodb rather than mongodb_test because it drives
// Connect() on the components directly. Nothing else can express "the framework
// reconnected": a reconnect is not observable through the stream API, and
// counting builder invocations requires reaching the component that owns the
// client.
//
// Auth is deliberately absent from the container. The counting stub returns a
// (nil, nil) credential, which ClientConfig.Connect treats as "apply no auth"
// (`if cred != nil { opt.SetAuth(*cred) }` - verified in common.go), so the
// handshake succeeds against a mongod running without --auth. A real
// MONGODB-AWS credential could not authenticate against a test container at
// all, and any credential at all would fail the handshake here.

package mongodb

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// countingCredentialBuilder records how many times the credential builder was
// invoked, which is once per client construction.
type countingCredentialBuilder struct {
	n atomic.Int64
}

// build is a CredentialBuilder. Returning a nil credential is legal - see the
// file comment - and is what lets these tests run against a no-auth container.
func (c *countingCredentialBuilder) build(context.Context) (*options.Credential, error) {
	c.n.Add(1)
	return nil, nil
}

func (c *countingCredentialBuilder) count() int64 { return c.n.Load() }

// stubCountingAWSOptFn swaps AWSOptFn for one handing back the counting builder,
// so that `aws.enabled` configs resolve without AWS being involved. Role
// assumption is still detected from the config by ClientConfigFromParsed, which
// is independent of the builder.
func stubCountingAWSOptFn(t *testing.T) *countingCredentialBuilder {
	t.Helper()
	counter := &countingCredentialBuilder{}
	prev := AWSOptFn
	t.Cleanup(func() { AWSOptFn = prev })
	AWSOptFn = func(*service.ParsedConfig, *service.Logger) (CredentialBuilder, error) {
		return counter.build, nil
	}
	return counter
}

// startMongoWithoutAuth runs a standalone mongod with no credentials, so its URI
// carries no userinfo and unauthenticated clients are accepted.
func startMongoWithoutAuth(t *testing.T) string {
	t.Helper()
	ctr, err := testcontainers.Run(t.Context(), "mongo:7",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)
	uri := "mongodb://localhost:" + mp.Port()

	require.Eventually(t, func() bool {
		client, err := mongo.Connect(options.Client().
			SetConnectTimeout(10 * time.Second).
			SetTimeout(30 * time.Second).
			SetServerSelectionTimeout(30 * time.Second).
			ApplyURI(uri))
		if err != nil {
			return false
		}
		defer func() { _ = client.Disconnect(context.Background()) }()
		return client.Ping(t.Context(), nil) == nil
	}, time.Minute, time.Second, "the mongod never became reachable")

	return uri
}

// awsConfigFor renders the `aws` block for a role-assuming or an ambient
// configuration. Only the presence of a role changes AssumesRole, which is what
// selects the refresh behaviour under test.
func awsConfigFor(assumesRole bool) string {
	if assumesRole {
		return `
aws:
  enabled: true
  region: us-east-1
  role: arn:aws:iam::123456789012:role/foo
`
	}
	return `
aws:
  enabled: true
  region: us-east-1
`
}

// TestIntegrationMongoInputRefreshesCredentialsOnReconnect covers the input.
//
// The input gates Connect on its cursor (`if m.cursor != nil { return nil }`),
// so the cursor is cleared between the two calls to model the state a reconnect
// reaches. Only the cursor is cleared, not the client: a real cursor failure
// tears the client down too, which would force a rebuild for any configuration
// and so could not tell the two apart. Leaving a live client is what puts the
// question under test - reuse it, or rebuild for fresh credentials?
func TestIntegrationMongoInputRefreshesCredentialsOnReconnect(t *testing.T) {
	integration.CheckSkip(t)
	uri := startMongoWithoutAuth(t)

	newInput := func(t *testing.T, assumesRole bool) (*mongoInput, *countingCredentialBuilder) {
		t.Helper()
		counter := stubCountingAWSOptFn(t)
		conf, err := mongoConfigSpec().ParseYAML(`
url: `+uri+`
database: testdb
collection: testcoll
operation: find
query: 'root = {}'
`+awsConfigFor(assumesRole), nil)
		require.NoError(t, err)
		cc, err := ClientConfigFromParsed(conf, service.MockResources().Logger())
		require.NoError(t, err)
		require.Equal(t, assumesRole, cc.AssumesRole())
		return &mongoInput{
			cc:         cc,
			collection: "testcoll",
			operation:  "find",
			query:      bson.D{},
			logger:     service.MockResources().Logger(),
		}, counter
	}

	t.Run("a role-assuming input rebuilds its client", func(t *testing.T) {
		in, counter := newInput(t, true)
		t.Cleanup(func() { _ = in.Close(context.Background()) })

		require.NoError(t, in.Connect(t.Context()))
		require.Equal(t, int64(1), counter.count(), "the first connect must resolve credentials once")
		require.NotNil(t, in.client, "the first connect must have produced a live client")

		in.cursor = nil
		require.NoError(t, in.Connect(t.Context()))
		require.Equal(t, int64(2), counter.count(),
			"a reconnect must rebuild the client so the STS session is re-resolved")
	})

	t.Run("an ambient input keeps its client", func(t *testing.T) {
		in, counter := newInput(t, false)
		t.Cleanup(func() { _ = in.Close(context.Background()) })

		require.NoError(t, in.Connect(t.Context()))
		require.Equal(t, int64(1), counter.count())
		before := in.client

		in.cursor = nil
		require.NoError(t, in.Connect(t.Context()))
		require.Equal(t, int64(1), counter.count(),
			"the ambient chain refreshes itself, so the fast path must reuse the client")
		require.Same(t, before, in.client, "the client must not have been rebuilt")
	})
}

// TestIntegrationMongoOutputRefreshesCredentialsOnReconnect covers the output,
// whose Connect is the plain form of the same rule: rebuild when a role is
// assumed, otherwise keep a client whose ping still succeeds. No cursor stands in
// the way here, so the two Connect calls are exactly what the framework does.
func TestIntegrationMongoOutputRefreshesCredentialsOnReconnect(t *testing.T) {
	integration.CheckSkip(t)
	uri := startMongoWithoutAuth(t)

	newOutput := func(t *testing.T, assumesRole bool) (*outputWriter, *countingCredentialBuilder) {
		t.Helper()
		counter := stubCountingAWSOptFn(t)
		conf, err := outputSpec().ParseYAML(`
url: `+uri+`
database: testdb
collection: testcoll
operation: insert-one
document_map: 'root = this'
`+awsConfigFor(assumesRole), nil)
		require.NoError(t, err)
		w, err := newOutputWriter(conf, service.MockResources())
		require.NoError(t, err)
		require.Equal(t, assumesRole, w.cc.AssumesRole())
		return w, counter
	}

	t.Run("a role-assuming output rebuilds its client", func(t *testing.T) {
		w, counter := newOutput(t, true)
		t.Cleanup(func() { _ = w.Close(context.Background()) })

		require.NoError(t, w.Connect(t.Context()))
		require.Equal(t, int64(1), counter.count())

		require.NoError(t, w.Connect(t.Context()))
		require.Equal(t, int64(2), counter.count(),
			"a reconnect must rebuild the client so the STS session is re-resolved")
	})

	t.Run("an ambient output keeps its client", func(t *testing.T) {
		w, counter := newOutput(t, false)
		t.Cleanup(func() { _ = w.Close(context.Background()) })

		require.NoError(t, w.Connect(t.Context()))
		require.Equal(t, int64(1), counter.count())
		before := w.client

		require.NoError(t, w.Connect(t.Context()))
		require.Equal(t, int64(1), counter.count(),
			"a successful ping must keep the client when credentials cannot expire that way")
		require.Same(t, before, w.client)
	})
}

// TestIntegrationMongoCredentialBuilderMayReturnNoCredential is the assertion the
// two tests above depend on: a builder returning (nil, nil) leaves the client
// unauthenticated rather than producing an invalid credential. If this contract
// ever changes, the counting stub stops being usable and both tests would fail
// for a reason unrelated to what they assert.
func TestIntegrationMongoCredentialBuilderMayReturnNoCredential(t *testing.T) {
	integration.CheckSkip(t)
	uri := startMongoWithoutAuth(t)

	counter := stubCountingAWSOptFn(t)
	conf, err := mongoConfigSpec().ParseYAML(`
url: `+uri+`
database: testdb
collection: testcoll
operation: find
query: 'root = {}'
`+awsConfigFor(true), nil)
	require.NoError(t, err)
	cc, err := ClientConfigFromParsed(conf, service.MockResources().Logger())
	require.NoError(t, err)
	require.NotNil(t, cc.credBuilder, "the aws block must have produced a builder")

	client, db, err := cc.Connect(t.Context())
	require.NoError(t, err, "a nil credential must not be turned into an auth attempt")
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	require.Equal(t, int64(1), counter.count())
	require.NoError(t, client.Ping(t.Context(), nil))
	require.Equal(t, "testdb", db.Name())

	// And the connection is genuinely usable, not merely constructed.
	_, err = db.Collection("testcoll").InsertOne(t.Context(), bson.M{"_id": 1})
	require.NoError(t, err)
	if err != nil && strings.Contains(err.Error(), "auth") {
		t.Fatalf("unexpected authentication involvement: %v", err)
	}
}
