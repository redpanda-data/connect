package secrets

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

type awsSecretsManager struct {
	client *secretsmanager.SecretsManager
	logger *slog.Logger
}

func newAWSSecretsManager(_ context.Context, logger *slog.Logger, url *url.URL) (secretAPI, error) {
	s, err := session.NewSession(&aws.Config{
		Region: getRegion(url.Host),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create secret manager session: %w", err)
	}

	return &awsSecretsManager{
		client: secretsmanager.New(s, aws.NewConfig()),
		logger: logger,
	}, nil
}

func (a *awsSecretsManager) getSecretValue(key string) (string, bool) {
	value, err := a.client.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretId: &key,
	})
	if err != nil {
		var notFound *secretsmanager.ResourceNotFoundException
		if !errors.As(err, &notFound) {
			// An error that isn't due to key-not-found gets logged
			a.logger.With("error", err, "key", key).Error("Failed to look up secret")
		}
		return "", false
	}

	return *value.SecretString, true
}

func getRegion(host string) *string {
	endpoint := strings.TrimPrefix(host, "secretsmanager.")
	region := strings.TrimSuffix(endpoint, ".amazonaws.com")

	return &region
}
