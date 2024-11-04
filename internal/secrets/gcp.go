package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type gcpSecretsManager struct {
	client    *secretmanager.Client
	projectID string
	logger    *slog.Logger
}

func newGCPSecretsManager(ctx context.Context, logger *slog.Logger, url *url.URL) (secretAPI, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create secretmanager client: %w", err)
	}

	return &gcpSecretsManager{
		client:    client,
		projectID: url.Host,
		logger:    logger,
	}, nil
}

func (g *gcpSecretsManager) getSecretValue(ctx context.Context, key string) (string, bool) {
	resp, err := g.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: g.getSecretID(key),
	})

	if err != nil {
		if status.Code(err) != codes.NotFound {
			g.logger.With("error", err, "key", key).Error("Failed to look up secret")
		}
		return "", false
	}

	value := string(resp.Payload.Data)
	return value, true
}

func (g *gcpSecretsManager) getSecretID(key string) string {
	return fmt.Sprintf("projects/%v/secrets/%v/versions/latest", g.projectID, key)
}
