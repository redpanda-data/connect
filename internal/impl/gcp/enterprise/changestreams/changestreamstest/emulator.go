// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreamstest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"cloud.google.com/go/spanner"
	adminapi "cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startSpannerEmulator(t *testing.T) (addr string) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("Starting emulator")
	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "gcr.io/cloud-spanner-emulator/emulator",
		Tag:        "latest",
		Env: []string{
			"SPANNER_EMULATOR_HOST=0.0.0.0:9010",
		},
		ExposedPorts: []string{"9010/tcp"},
	}, func(cfg *docker.HostConfig) {
		cfg.AutoRemove = true
		cfg.RestartPolicy = docker.RestartPolicy{
			Name: "no",
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	closeFn := func() {
		if err := pool.Purge(res); err != nil {
			t.Errorf("Failed to purge resource: %v", err)
		}
		t.Log("Emulator stopped")
	}

	addr = "localhost:" + res.GetPort("9010/tcp")

	if err := pool.Retry(func() error {
		t.Logf("Waiting for emulator to be ready at %s", addr)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()

		return nil
	}); err != nil {
		closeFn()
		t.Fatal(err)
	}

	t.Cleanup(closeFn)
	return
}

const (
	// EmulatorProjectID is the project ID used for testing with the emulator.
	EmulatorProjectID = "test-project"
	// EmulatorInstanceID is the instance ID used for testing with the emulator
	EmulatorInstanceID = "test-instance"
)

func createInstance(ctx context.Context, conn *grpc.ClientConn) (string, error) {
	adm, err := instance.NewInstanceAdminClient(ctx,
		option.WithGRPCConn(conn),
		option.WithoutAuthentication(),
	)
	if err != nil {
		return "", err
	}
	// Do not close as it will close the grpc connection

	op, err := adm.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + EmulatorProjectID,
		InstanceId: EmulatorInstanceID,
		Instance: &instancepb.Instance{
			Config:          "projects/" + EmulatorProjectID + "/instanceConfigs/regional-europe-central2",
			DisplayName:     EmulatorInstanceID,
			ProcessingUnits: 100,
		},
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}

// EmulatorHelper provides utilities for working with the Spanner emulator in tests.
type EmulatorHelper struct {
	*adminapi.DatabaseAdminClient
	instanceName string

	t    *testing.T
	conn *grpc.ClientConn
}

// MakeEmulatorHelper creates a new helper for interacting with the Spanner emulator in tests.
func MakeEmulatorHelper(t *testing.T) EmulatorHelper {
	t.Helper()

	// Create a gRPC connection to the emulator
	conn, err := grpc.NewClient(startSpannerEmulator(t),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	ctx := t.Context()

	// Create an instance
	instanceName, err := createInstance(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}

	// Create the database admin client with the gRPC connection
	adm, err := adminapi.NewDatabaseAdminClient(ctx,
		option.WithGRPCConn(conn),
		option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}

	return EmulatorHelper{
		DatabaseAdminClient: adm,
		instanceName:        instanceName,

		t:    t,
		conn: conn,
	}
}

// CreateTestDatabase creates a new test database with the given name and returns a client connected to it.
func (e EmulatorHelper) CreateTestDatabase(dbName string, opts ...func(*adminpb.CreateDatabaseRequest)) *spanner.Client {
	c, err := e.createTestDatabase(dbName, opts...)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// CreateTestDatabaseWithDialect creates a new test database with the given name and dialect, and returns a client connected to it.
func (e EmulatorHelper) CreateTestDatabaseWithDialect(dbName string, dialect adminpb.DatabaseDialect, opts ...func(*adminpb.CreateDatabaseRequest)) *spanner.Client {
	opts = append(opts, func(req *adminpb.CreateDatabaseRequest) {
		req.DatabaseDialect = dialect
	})

	c, err := e.createTestDatabase(dbName, opts...)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

func (e EmulatorHelper) createTestDatabase(dbName string, opts ...func(*adminpb.CreateDatabaseRequest)) (*spanner.Client, error) {
	req := &adminpb.CreateDatabaseRequest{
		Parent:          e.instanceName,
		CreateStatement: "CREATE DATABASE " + dbName,
	}
	for _, o := range opts {
		o(req)
	}

	e.t.Logf("Creating test database %q", dbName)
	ctx := e.t.Context()
	op, err := e.CreateDatabase(ctx, req)
	if err != nil {
		return nil, err
	}
	if _, err := op.Wait(ctx); err != nil {
		return nil, err
	}
	c, err := spanner.NewClient(ctx, e.fullDatabaseName(dbName), option.WithGRPCConn(e.conn))
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (e EmulatorHelper) fullDatabaseName(dbName string) string {
	return fmt.Sprintf("%s/databases/%s", e.instanceName, dbName)
}

// Conn returns the gRPC client connection to the emulator.
func (e EmulatorHelper) Conn() *grpc.ClientConn {
	return e.conn
}

func (e EmulatorHelper) Close() error {
	return errors.Join(e.DatabaseAdminClient.Close(), e.conn.Close())
}
