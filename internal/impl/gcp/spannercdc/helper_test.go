package spannercdc

import (
	"context"
	"fmt"
	"os"
	"strconv"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func NewTestContainer(ctx context.Context, image string, envVars map[string]string, ports []string, waitfor wait.Strategy, cmdArgs ...string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		SkipReaper:   true,
		Image:        image,
		Env:          envVars,
		ExposedPorts: ports,
		WaitingFor:   waitfor,
		Cmd:          cmdArgs,
	}

	// picks up local test env to clean up containers
	if skipReaper := os.Getenv("SKIP_REAPER"); skipReaper != "" {
		shouldSkipReaper, err := strconv.ParseBool(skipReaper)
		if err != nil {
			return nil, err
		}
		req.SkipReaper = shouldSkipReaper
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func CreateInstance(ctx context.Context, parentProjectID, instanceID string) (string, error) {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer instanceAdminClient.Close()

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + parentProjectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:          "projects/model/instanceConfigs/regional-us-central1",
			DisplayName:     instanceID,
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

func DeleteInstance(ctx context.Context, instanceName string) error {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

	return instanceAdminClient.DeleteInstance(ctx, &instancepb.DeleteInstanceRequest{
		Name: instanceName,
	})
}

func CreateDatabase(ctx context.Context, parentInstanceName, databaseID string) (string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parentInstanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
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
