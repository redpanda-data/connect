package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type azSecretsManager struct {
	client *azsecrets.Client
	logger *slog.Logger
}

func newAzSecretsManager(_ context.Context, logger *slog.Logger, url *url.URL) (secretAPI, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain Azure credentials: %w", err)
	}

	client, err := azsecrets.NewClient("https://"+url.Host, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create secretmanager client: %w", err)
	}

	return &azSecretsManager{
		client: client,
		logger: logger,
	}, nil
}

func (a *azSecretsManager) getSecretValue(ctx context.Context, key string) (string, bool) {
	resp, err := a.client.GetSecret(ctx, key, "", nil)

	if err != nil {
		if status.Code(err) != codes.NotFound {
			a.logger.With("error", err, "key", key).Error("Failed to look up secret")
		}
		return "", false
	}

	return *resp.Value, true
}
