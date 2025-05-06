// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreamstest

import (
	"flag"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
)

var (
	realSpannerProjectID  = flag.String("spanner.project_id", "", "GCP project ID for Spanner tests")
	realSpannerInstanceID = flag.String("spanner.instance_id", "", "Spanner instance ID for tests")
	realSpannerDatabaseID = flag.String("spanner.database_id", "", "Spanner database ID for tests")
)

// CheckSkipReal skips the test if the real Spanner environment is not configured.
// It checks if the required environment variables for real Spanner tests are set.
func CheckSkipReal(t *testing.T) {
	if *realSpannerProjectID == "" || *realSpannerInstanceID == "" || *realSpannerDatabaseID == "" {
		t.Skip("skipping real tests")
	}
}

func realSpannerDatabasePath() string {
	return fmt.Sprintf("projects/%s/instances/%s/databases/%s", *realSpannerProjectID, *realSpannerInstanceID, *realSpannerDatabaseID)
}

// RealHelper provides utilities for testing with a real Spanner instance.
// It manages the lifecycle of Spanner client and admin connections.
type RealHelper struct {
	admin  *adminapi.DatabaseAdminClient
	client *spanner.Client
}

// MakeRealHelper creates a RealHelper for the real spanner test environment.
func MakeRealHelper(t *testing.T) RealHelper {
	client, err := spanner.NewClient(t.Context(), realSpannerDatabasePath())
	if err != nil {
		t.Fatal(err)
	}

	admin, err := adminapi.NewDatabaseAdminClient(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	return RealHelper{
		admin:  admin,
		client: client,
	}
}

// Close closes all connections managed by the RealHelper.
func (r RealHelper) Close() error {
	if err := r.admin.Close(); err != nil {
		return err
	}

	r.client.Close()

	return nil
}

// DatabaseAdminClient returns the database admin client.
func (r RealHelper) DatabaseAdminClient() *adminapi.DatabaseAdminClient {
	return r.admin
}

// Client returns the Spanner client.
func (r RealHelper) Client() *spanner.Client {
	return r.client
}

// ProjectID returns the project ID for the real Spanner instance.
func (r RealHelper) ProjectID() string {
	return *realSpannerProjectID
}

// InstanceID returns the instance ID for the real Spanner instance.
func (r RealHelper) InstanceID() string {
	return *realSpannerInstanceID
}

// DatabaseID returns the database ID for the real Spanner instance.
func (r RealHelper) DatabaseID() string {
	return *realSpannerDatabaseID
}
