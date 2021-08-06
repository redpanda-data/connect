package gcp

import (
	"context"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// BigQueryClientConfig provides different configurations to create the bigquery.Client
type BigQueryClientConfig struct {
	// url is the client connection URL that overrides the default endpoint to be used for a service.
	url string
}

// NewBigQueryClient creates a bigquery.Client instance.
func NewBigQueryClient(ctx context.Context, projectID string, config BigQueryClientConfig) (*bigquery.Client, error) {
	if config.url == "" {
		return bigquery.NewClient(ctx, projectID)
	}

	return bigquery.NewClient(ctx, projectID, option.WithEndpoint(config.url))
}
