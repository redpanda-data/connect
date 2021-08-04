package gcp

import (
	"context"

	"cloud.google.com/go/bigquery"
)

// NewBigQueryClient creates a bigquery.Client instance.
func NewBigQueryClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	return bigquery.NewClient(ctx, projectID)
}
