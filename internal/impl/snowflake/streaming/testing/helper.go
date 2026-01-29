/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package testing

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestEnvironment holds all the components needed for testing
type TestEnvironment struct {
	FakeGCS *FakeGCSContainer
	Server  *MockSnowflakeServer
	T       *testing.T
}

// Setup creates a complete test environment with fake-gcs-server and mock Snowflake server
func Setup(t *testing.T) *TestEnvironment {
	t.Helper()
	ctx := context.Background()

	// Start fake-gcs-server container
	fakeGCS, err := StartFakeGCS(ctx)
	require.NoError(t, err, "failed to start fake-gcs-server container")

	// Create mock Snowflake server
	server := NewMockSnowflakeServer(fakeGCS)

	env := &TestEnvironment{
		FakeGCS: fakeGCS,
		Server:  server,
		T:       t,
	}

	// Register cleanup
	t.Cleanup(func() {
		server.Close()
		if err := fakeGCS.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate fake-gcs-server: %v", err)
		}
	})

	return env
}

// GenerateTestPrivateKey generates a test RSA private key
func GenerateTestPrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return privateKey
}

// GetLogger returns a test logger
func GetLogger(t *testing.T) *service.Logger {
	t.Helper()
	logger := service.MockResources().Logger()
	return logger
}
