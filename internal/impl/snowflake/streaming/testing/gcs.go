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
	"fmt"
	"os"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
)

const (
	defaultBucket     = "snowflake-test"
	defaultPathPrefix = "stage/"
)

// FakeGCSContainer wraps the fake-gcs-server test container
type FakeGCSContainer struct {
	container  testcontainers.Container
	endpoint   string
	bucket     string
	pathPrefix string
}

// StartFakeGCS starts a fake-gcs-server container for testing
func StartFakeGCS(ctx context.Context) (*FakeGCSContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server:latest",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-scheme", "http", "-port", "4443", "-external-url", "http://localhost:4443"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("4443/tcp"),
			wait.ForLog("server started").WithStartupTimeout(30*time.Second),
		),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start fake-gcs-server container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get container host: %w", err)
	}

	mappedPort, err := container.MappedPort(ctx, "4443")
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to get mapped port: %w", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, mappedPort.Port())

	// Set STORAGE_EMULATOR_HOST so the GCS SDK uses our fake server
	os.Setenv("STORAGE_EMULATOR_HOST", fmt.Sprintf("%s:%s", host, mappedPort.Port()))

	gc := &FakeGCSContainer{
		container:  container,
		endpoint:   endpoint,
		bucket:     defaultBucket,
		pathPrefix: defaultPathPrefix,
	}

	// Create the bucket
	if err := gc.createBucket(ctx); err != nil {
		_ = container.Terminate(ctx)
		return nil, fmt.Errorf("failed to create bucket: %w", err)
	}

	return gc, nil
}

// createBucket creates the default bucket in fake-gcs-server
func (gc *FakeGCSContainer) createBucket(ctx context.Context) error {
	client, err := gcs.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return err
	}
	defer client.Close()

	bucket := client.Bucket(gc.bucket)
	return bucket.Create(ctx, "test-project", nil)
}

// Terminate stops and removes the fake-gcs-server container
func (gc *FakeGCSContainer) Terminate(ctx context.Context) error {
	os.Unsetenv("STORAGE_EMULATOR_HOST")
	if gc.container != nil {
		return gc.container.Terminate(ctx)
	}
	return nil
}

// Endpoint returns the GCS endpoint
func (gc *FakeGCSContainer) Endpoint() string {
	return gc.endpoint
}

// Bucket returns the bucket name
func (gc *FakeGCSContainer) Bucket() string {
	return gc.bucket
}

// PathPrefix returns the path prefix
func (gc *FakeGCSContainer) PathPrefix() string {
	return gc.pathPrefix
}

// GCSClient returns a configured GCS client for the fake-gcs-server instance
func (*FakeGCSContainer) GCSClient(ctx context.Context) (*gcs.Client, error) {
	return gcs.NewClient(ctx, option.WithoutAuthentication())
}
