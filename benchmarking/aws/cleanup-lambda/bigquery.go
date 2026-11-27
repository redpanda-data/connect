// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// The runner's bigquery sink writes to a per-run dataset bench_<session> in
// the bench GCP project and deletes it at teardown. A run that dies before
// teardown leaves the dataset behind; the reaper deletes any bench_ dataset
// older than the TTL. BigQuery datasets are not in the AWS tagging index, so
// they are found by listing and selected by name and creation time.

const bigQueryDatasetPrefix = "bench_"

// bigQueryDatasetRe is the shape of a runner dataset. It also keeps anything
// that is not a plain identifier out of the delete call.
var (
	bigQueryDatasetRe = regexp.MustCompile(`^bench_[a-z0-9_]+$`)
	bigQueryProjectRe = regexp.MustCompile(`^[a-z0-9][a-z0-9:.-]*$`)
)

// errBigQuerySecretMissing means the credentials secret does not exist yet
// (or the lambda cannot read it): there is nothing to sweep.
var errBigQuerySecretMissing = errors.New("bigquery secret does not exist")

type bigQueryDatasetInfo struct {
	ID      string
	Created time.Time
}

// bigQueryReaperAPI is the reaper's slice of BigQuery. Tests fake it.
type bigQueryReaperAPI interface {
	// ListDatasets lists the project's datasets whose ID starts with prefix.
	ListDatasets(ctx context.Context, project, prefix string) ([]bigQueryDatasetInfo, error)
	// DeleteDataset deletes a dataset with its contents.
	DeleteDataset(ctx context.Context, project, id string) error
}

// bigQueryOpener connects to the bench project and returns its project ID.
// errBigQuerySecretMissing when the secret does not exist or cannot be read.
type bigQueryOpener func(ctx context.Context) (bigQueryReaperAPI, string, error)

// sweepBigQueryDatasets deletes stale bench_ datasets into report. A nil
// opener (no secret configured) or an unreadable secret skips the sweep; any
// other failure is recorded, not fatal.
func sweepBigQueryDatasets(ctx context.Context, open bigQueryOpener, now time.Time, ttl time.Duration, report *SweepReport) {
	if open == nil {
		slog.Info("bigquery sweep skipped: no bigquery secret configured")
		return
	}
	api, project, err := open(ctx)
	if errors.Is(err, errBigQuerySecretMissing) {
		slog.Warn("bigquery sweep skipped: the bigquery secret is missing or unreadable", "err", err)
		return
	}
	if err != nil {
		report.record("bigquery", "connect", false, err)
		return
	}
	if c, ok := api.(io.Closer); ok {
		defer c.Close()
	}
	if !bigQueryProjectRe.MatchString(project) {
		report.record("bigquery", "config", false, fmt.Errorf("project %q is not a plain project ID", project))
		return
	}
	datasets, err := api.ListDatasets(ctx, project, bigQueryDatasetPrefix)
	if err != nil {
		report.record("bigquery-datasets", project, false, err)
		return
	}
	for _, d := range datasets {
		// A zero creation time means the metadata gave none; it is not
		// "infinitely old", so the dataset is left for a later sweep.
		if !bigQueryDatasetRe.MatchString(d.ID) || d.Created.IsZero() || !olderThanTTL(d.Created, now, ttl) {
			if d.Created.IsZero() && bigQueryDatasetRe.MatchString(d.ID) {
				slog.Warn("bigquery dataset skipped: no creation time", "dataset", d.ID)
			}
			continue
		}
		id := project + "." + d.ID
		err := api.DeleteDataset(ctx, project, d.ID)
		report.record("bigquery-dataset", id, err == nil, err)
	}
}

// newBigQueryOpener reads the credentials secret (JSON {project,
// credentials_json, location?}) on each sweep. Its errors go to SNS and
// Slack, so they name the problem, never the secret's content.
func newBigQueryOpener(sm secretsGetter, secretID string, connect func(ctx context.Context, project string, credentialsJSON []byte) (bigQueryReaperAPI, error)) bigQueryOpener {
	return func(ctx context.Context) (bigQueryReaperAPI, string, error) {
		out, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &secretID})
		if _, ok := errors.AsType[*smtypes.ResourceNotFoundException](err); ok {
			return nil, "", fmt.Errorf("%w: %s not found", errBigQuerySecretMissing, secretID)
		}
		// Before the secret exists, a grant on the suffixed ARN can answer
		// AccessDenied instead of ResourceNotFound.
		if ae, ok := errors.AsType[smithy.APIError](err); ok && ae.ErrorCode() == "AccessDeniedException" {
			return nil, "", fmt.Errorf("%w: access denied reading %s", errBigQuerySecretMissing, secretID)
		}
		if err != nil {
			return nil, "", fmt.Errorf("get bigquery secret: %w", err)
		}
		if out.SecretString == nil {
			return nil, "", errors.New("bigquery secret has no SecretString")
		}
		var sec struct {
			Project         string `json:"project"`
			CredentialsJSON string `json:"credentials_json"`
		}
		if json.Unmarshal([]byte(*out.SecretString), &sec) != nil {
			return nil, "", errors.New("bigquery secret is not a JSON object of strings")
		}
		if sec.Project == "" || sec.CredentialsJSON == "" {
			return nil, "", errors.New("bigquery secret needs project and credentials_json")
		}
		var key struct {
			Type       string `json:"type"`
			PrivateKey string `json:"private_key"`
		}
		if json.Unmarshal([]byte(sec.CredentialsJSON), &key) != nil || key.Type != "service_account" || key.PrivateKey == "" {
			return nil, "", errors.New("bigquery secret credentials_json is not a service-account key JSON")
		}
		api, err := connect(ctx, sec.Project, []byte(sec.CredentialsJSON))
		if err != nil {
			return nil, "", fmt.Errorf("open bigquery: %w", err)
		}
		return api, sec.Project, nil
	}
}

// connectGoBigQuery is the real connector. Its errors are replaced, as the
// client's can echo option details.
func connectGoBigQuery(ctx context.Context, project string, credentialsJSON []byte) (bigQueryReaperAPI, error) {
	c, err := bigquery.NewClient(ctx, project, option.WithAuthCredentialsJSON(option.ServiceAccount, credentialsJSON))
	if err != nil {
		return nil, errors.New("create bigquery client failed")
	}
	return &goBigQuery{c: c}, nil
}

type goBigQuery struct{ c *bigquery.Client }

func (g *goBigQuery) ListDatasets(ctx context.Context, project, prefix string) ([]bigQueryDatasetInfo, error) {
	it := g.c.Datasets(ctx)
	it.ProjectID = project
	var out []bigQueryDatasetInfo
	for {
		ds, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(ds.DatasetID, prefix) {
			continue
		}
		md, err := ds.Metadata(ctx)
		var ge *googleapi.Error
		if errors.As(err, &ge) && ge.Code == http.StatusNotFound {
			continue // deleted since the listing
		}
		if err != nil {
			return nil, fmt.Errorf("dataset %s metadata: %w", ds.DatasetID, err)
		}
		out = append(out, bigQueryDatasetInfo{ID: ds.DatasetID, Created: md.CreationTime})
	}
}

func (g *goBigQuery) DeleteDataset(ctx context.Context, project, id string) error {
	return g.c.DatasetInProject(project, id).DeleteWithContents(ctx)
}

func (g *goBigQuery) Close() error { return g.c.Close() }
