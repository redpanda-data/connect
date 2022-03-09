package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/Masterminds/squirrel"

	"github.com/benthosdev/benthos/v4/public/service"
)

type bigqueryIterator interface {
	Next(dst interface{}) error
}

type bqClient interface {
	RunQuery(ctx context.Context, options *bqQueryBuilderOptions) (bigqueryIterator, error)
	Close() error
}

func wrapBQClient(client *bigquery.Client, logger *service.Logger) bqClient {
	return &wrappedBQClient{wrapped: client, logger: logger}
}

type wrappedBQClient struct {
	wrapped *bigquery.Client
	logger  *service.Logger
}

func (client *wrappedBQClient) RunQuery(ctx context.Context, options *bqQueryBuilderOptions) (bigqueryIterator, error) {
	query, err := buildBQQuery(client.wrapped, options)
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	client.logger.With("job_id", job.ID()).Debug("running bigquery job")

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait on job: %w", err)
	}

	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("failed to complete job successfully: %w", err)
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read rows: %w", err)
	}

	return it, nil
}

func (client *wrappedBQClient) Close() error {
	return client.wrapped.Close()
}

type bqQueryParts struct {
	table   string
	columns []string
	where   string
	prefix  string
	suffix  string
}

type bqQueryBuilderOptions struct {
	queryParts *bqQueryParts
	jobLabels  map[string]string
	args       []interface{}
}

func buildBQQuery(client *bigquery.Client, options *bqQueryBuilderOptions) (*bigquery.Query, error) {
	queryParts := options.queryParts

	builder := squirrel.
		Select(queryParts.columns...).
		From(fmt.Sprintf("`%s`", queryParts.table)).
		Where(queryParts.where, options.args...)

	if queryParts.prefix != "" {
		builder = builder.Prefix(queryParts.prefix)
	}
	if queryParts.suffix != "" {
		builder = builder.Suffix(queryParts.suffix)
	}

	qs, args, err := builder.PlaceholderFormat(squirrel.Question).ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query string: %w", err)
	}

	query := client.Query(qs)
	query.Labels = options.jobLabels

	bqparams := make([]bigquery.QueryParameter, 0, len(args))
	for _, arg := range args {
		bqparams = append(bqparams, bigquery.QueryParameter{Value: arg})
	}

	query.Parameters = bqparams

	return query, nil
}
