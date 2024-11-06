package secrets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

type awsSecretsManager struct {
	client *secretsmanager.Client
	logger *slog.Logger
}

func newAWSSecretsManager(ctx context.Context, logger *slog.Logger, url *url.URL) (secretAPI, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(getRegion(url.Host)))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &awsSecretsManager{
		client: secretsmanager.NewFromConfig(cfg),
		logger: logger,
	}, nil
}

func (a *awsSecretsManager) getSecretValue(ctx context.Context, key string) (string, bool) {
	value, err := a.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &key,
	})
	if err != nil {
		var nf *types.ResourceNotFoundException
		if !errors.As(err, &nf) {
			a.logger.With("error", err, "key", key).Error("Failed to look up secret")
		}
		return "", false
	}

	return *value.SecretString, true
}

func getRegion(host string) string {
	endpoint := strings.TrimPrefix(host, "secretsmanager.")
	region := strings.TrimSuffix(endpoint, ".amazonaws.com")

	return region
}
