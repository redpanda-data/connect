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
	"net/http"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// The bigquery sink writes to a per-run dataset, bench_<session>, in the
// bench GCP project. The output creates its own table (create_disposition
// CREATE_IF_NEEDED is forced by the plan's template), so only the dataset is
// provisioned.
//
// Key handling mirrors the snowflake sink: the orchestrator reads the
// credentials secret with its own AWS credentials and keeps the service
// account key in memory for its own API calls. Connect on the runner host
// never receives the key from the orchestrator: the bench script fetches the
// same secret on the host (the host role may read exactly that secret),
// writes credentials_json to a 0600 file, and Connect finds it as
// application-default credentials through GOOGLE_APPLICATION_CREDENTIALS
// (the gcp_bigquery output has no credentials-file field, and credentials_json
// is stripped from the config). The key is never in a placeholder, a rendered
// config, an SSM command, a Plan or a log line.
const (
	// bigQuerySecretID is the Secrets Manager secret holding the bench
	// project's credentials, as JSON {project, credentials_json, location?}.
	// credentials_json is a service-account key JSON string.
	bigQuerySecretID = "redpanda-connect-bench/bigquery"
	bigQueryKeyDir   = "/opt/bench/secrets"
	bigQueryKeyFile  = bigQueryKeyDir + "/bigquery.json"
	// bigQueryTable is the table the output creates in the run's dataset.
	bigQueryTable = "t"
	// bigQueryDefaultLocation is the dataset location when the secret sets
	// none.
	bigQueryDefaultLocation = "US"
	// Streamed rows become visible to queries with a delay, so a count is
	// re-read every bigQueryPollInterval until two reads agree or
	// bigQueryPollMax has passed.
	bigQueryPollInterval = 10 * time.Second
	bigQueryPollMax      = 90 * time.Second
)

// BigQueryAPI is the orchestrator's slice of BigQuery. The real adapter wraps
// cloud.google.com/go/bigquery; tests fake it.
type BigQueryAPI interface {
	// CreateDataset creates the dataset in location. An existing dataset is
	// not an error.
	CreateDataset(ctx context.Context, project, dataset, location string) error
	// DeleteDataset deletes the dataset with its contents. ErrBigQueryNotFound
	// when it does not exist.
	DeleteDataset(ctx context.Context, project, dataset string) error
	// QueryInt runs a query returning one integer. ErrBigQueryNotFound when
	// the queried table or dataset does not exist.
	QueryInt(ctx context.Context, project, location, query string) (int64, error)
}

// ErrBigQueryNotFound is the BigQueryAPI's answer for a missing dataset or
// table (HTTP 404).
var ErrBigQueryNotFound = errors.New("bigquery object not found")

// BigQueryTarget is the non-secret part of the bench project's credentials.
type BigQueryTarget struct {
	Project, Location string
}

var (
	bigQueryDatasetRe  = regexp.MustCompile(`^[a-z0-9_]+$`)
	bigQueryProjectRe  = regexp.MustCompile(`^[a-z0-9][a-z0-9:.-]*$`)
	bigQueryLocationRe = regexp.MustCompile(`^[A-Za-z0-9-]+$`)
)

// validBigQueryDataset admits only lower-case letters, digits and '_', so a
// name interpolated into a query cannot change the statement.
func validBigQueryDataset(s string) error {
	if !bigQueryDatasetRe.MatchString(s) {
		return fmt.Errorf("bigquery dataset %q must match %s", s, bigQueryDatasetRe)
	}
	return nil
}

// bigQueryDataset is the run's dataset: bench_<session>.
func bigQueryDataset(session string) string {
	return "bench_" + scratchName(session)
}

type bigQuerySink struct {
	api    BigQueryAPI
	target BigQueryTarget
	region string
	// dataset is the run's dataset, from SinkDeps.SessionID; Count reads it.
	dataset string
	// seenRows is set once a count has read rows: from then on the table
	// must exist.
	seenRows bool
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
}

func newBigQuerySink(deps SinkDeps) (*bigQuerySink, error) {
	if deps.BigQuery == nil {
		return nil, errors.New("bigquery sink: no bigquery client")
	}
	t := deps.BigQueryTarget
	if t.Location == "" {
		t.Location = bigQueryDefaultLocation
	}
	if !bigQueryProjectRe.MatchString(t.Project) {
		return nil, fmt.Errorf("bigquery sink: project must match %s", bigQueryProjectRe)
	}
	if !bigQueryLocationRe.MatchString(t.Location) {
		return nil, fmt.Errorf("bigquery sink: location must match %s", bigQueryLocationRe)
	}
	return &bigQuerySink{
		api:     deps.BigQuery,
		target:  t,
		region:  deps.Region,
		dataset: bigQueryDataset(deps.SessionID),
		now:     time.Now,
		sleep:   sleepCtx,
	}, nil
}

func (s *bigQuerySink) Provision(ctx context.Context, session string) (map[string]string, error) {
	ds := bigQueryDataset(session)
	if err := validBigQueryDataset(ds); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNothingProvisioned, err)
	}
	s.dataset = ds
	if err := s.api.CreateDataset(ctx, s.target.Project, ds, s.target.Location); err != nil {
		return nil, fmt.Errorf("bigquery create dataset %s.%s: %w", s.target.Project, ds, err)
	}
	return map[string]string{
		"BENCH_BQ_PROJECT":          s.target.Project,
		"BENCH_BQ_DATASET":          ds,
		"BENCH_BQ_TABLE":            bigQueryTable,
		"BENCH_BQ_CREDENTIALS_FILE": bigQueryKeyFile,
	}, nil
}

// Count is the table's row count once it stops moving: streamed rows become
// visible with a delay, so it re-reads every 10 s until two reads agree or
// 90 s have passed, and returns the last read. The output creates the table
// on its first write, so a missing table counts as empty until rows have been
// seen, and is an error after.
func (s *bigQuerySink) Count(ctx context.Context, _ string) (int64, error) {
	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s.%s`", s.target.Project, s.dataset, bigQueryTable)
	read := func() (int64, error) {
		n, err := s.api.QueryInt(ctx, s.target.Project, s.target.Location, q)
		if errors.Is(err, ErrBigQueryNotFound) && !s.seenRows {
			return 0, nil
		}
		if errors.Is(err, ErrBigQueryNotFound) {
			return 0, fmt.Errorf("bigquery %q: table missing after rows were written: %w", q, err)
		}
		if err != nil {
			return 0, fmt.Errorf("bigquery %q: %w", q, err)
		}
		if n > 0 {
			s.seenRows = true
		}
		return n, nil
	}
	start := s.now()
	last, err := read()
	if err != nil {
		return 0, err
	}
	for s.now().Sub(start) < bigQueryPollMax {
		if err := s.sleep(ctx, bigQueryPollInterval); err != nil {
			return 0, err
		}
		n, err := read()
		if err != nil {
			return 0, err
		}
		if n == last {
			return n, nil
		}
		last = n
	}
	return last, nil
}

// Teardown deletes the run's dataset with everything in it. A dataset that
// is already gone is success.
func (s *bigQuerySink) Teardown(ctx context.Context, session string) error {
	ds := bigQueryDataset(session)
	if err := validBigQueryDataset(ds); err != nil {
		return err
	}
	err := s.api.DeleteDataset(ctx, s.target.Project, ds)
	if err != nil && !errors.Is(err, ErrBigQueryNotFound) {
		return fmt.Errorf("bigquery delete dataset %s.%s: %w", s.target.Project, ds, err)
	}
	return nil
}

// HostSetup is the bench-script prologue that puts the key on the runner
// host: the host fetches the secret itself, so the key never passes through
// the orchestrator or an SSM parameter, and writes only credentials_json to a
// 0600 file that the EXIT trap removes when the point's script ends. Nothing
// it runs prints the secret.
func (s *bigQuerySink) HostSetup() string {
	return fmt.Sprintf(`mkdir -p %[1]s
chmod 0700 %[1]s
trap 'rm -f %[2]s' EXIT
rm -f %[2]s
(umask 077 && aws secretsmanager get-secret-value --region %[3]q --secret-id %[4]q --query SecretString --output text | jq -er .credentials_json > %[2]s)
chmod 0600 %[2]s`, bigQueryKeyDir, bigQueryKeyFile, s.region, bigQuerySecretID)
}

// HostEnv points Connect's application-default credentials at the host file.
// It is a path, never key material.
func (*bigQuerySink) HostEnv() map[string]string {
	return map[string]string{"GOOGLE_APPLICATION_CREDENTIALS": bigQueryKeyFile}
}

// bigQuerySecret is the credentials secret's JSON. It is only ever parsed,
// never printed or wrapped into an error.
type bigQuerySecret struct {
	Project         string `json:"project"`
	CredentialsJSON string `json:"credentials_json"`
	Location        string `json:"location"`
}

// bigQueryOpener builds the orchestrator's BigQuery client from the service
// account key.
type bigQueryOpener func(ctx context.Context, project string, credentialsJSON []byte) (BigQueryAPI, error)

// loadBigQuery reads the credentials secret and opens the orchestrator's
// client. Errors name the secret and the problem, never its content.
func loadBigQuery(ctx context.Context, sm SecretsManagerClient, open bigQueryOpener) (BigQueryAPI, BigQueryTarget, error) {
	id := bigQuerySecretID
	out, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &id})
	if err != nil {
		return nil, BigQueryTarget{}, fmt.Errorf("get bigquery secret %q: %w", id, err)
	}
	if out.SecretString == nil {
		return nil, BigQueryTarget{}, fmt.Errorf("bigquery secret %q has no SecretString", id)
	}
	var sec bigQuerySecret
	if json.Unmarshal([]byte(*out.SecretString), &sec) != nil {
		return nil, BigQueryTarget{}, fmt.Errorf("bigquery secret %q is not a JSON object of strings", id)
	}
	if sec.Location == "" {
		sec.Location = bigQueryDefaultLocation
	}
	for k, v := range map[string]string{"project": sec.Project, "credentials_json": sec.CredentialsJSON} {
		if v == "" {
			return nil, BigQueryTarget{}, fmt.Errorf("bigquery secret %q: %s is missing or empty", id, k)
		}
	}
	// These land in the rendered config and in queries, so they must be
	// plain names.
	if !bigQueryProjectRe.MatchString(sec.Project) {
		return nil, BigQueryTarget{}, fmt.Errorf("bigquery secret %q: project must match %s", id, bigQueryProjectRe)
	}
	if !bigQueryLocationRe.MatchString(sec.Location) {
		return nil, BigQueryTarget{}, fmt.Errorf("bigquery secret %q: location must match %s", id, bigQueryLocationRe)
	}
	var key struct {
		Type       string `json:"type"`
		PrivateKey string `json:"private_key"`
	}
	if json.Unmarshal([]byte(sec.CredentialsJSON), &key) != nil || key.Type != "service_account" || key.PrivateKey == "" {
		return nil, BigQueryTarget{}, fmt.Errorf("bigquery secret %q: credentials_json is not a service-account key JSON", id)
	}
	api, err := open(ctx, sec.Project, []byte(sec.CredentialsJSON))
	if err != nil {
		return nil, BigQueryTarget{}, fmt.Errorf("open bigquery: %w", err)
	}
	return api, BigQueryTarget{Project: sec.Project, Location: sec.Location}, nil
}

// openGoBigQuery is the real opener.
func openGoBigQuery(ctx context.Context, project string, credentialsJSON []byte) (BigQueryAPI, error) {
	c, err := bigquery.NewClient(ctx, project, option.WithAuthCredentialsJSON(option.ServiceAccount, credentialsJSON))
	if err != nil {
		// The client's errors can echo option details; keep them out.
		return nil, errors.New("create bigquery client failed")
	}
	return &goBigQuery{c: c}, nil
}

type goBigQuery struct{ c *bigquery.Client }

func isGoogleStatus(err error, code int) bool {
	var ge *googleapi.Error
	return errors.As(err, &ge) && ge.Code == code
}

func (g *goBigQuery) CreateDataset(ctx context.Context, project, dataset, location string) error {
	err := g.c.DatasetInProject(project, dataset).Create(ctx, &bigquery.DatasetMetadata{Location: location})
	if isGoogleStatus(err, http.StatusConflict) {
		return nil
	}
	return err
}

func (g *goBigQuery) DeleteDataset(ctx context.Context, project, dataset string) error {
	err := g.c.DatasetInProject(project, dataset).DeleteWithContents(ctx)
	if isGoogleStatus(err, http.StatusNotFound) {
		return ErrBigQueryNotFound
	}
	return err
}

func (g *goBigQuery) QueryInt(ctx context.Context, _, location, query string) (int64, error) {
	q := g.c.Query(query)
	q.Location = location
	it, err := q.Read(ctx)
	if err == nil {
		var row []bigquery.Value
		if err = it.Next(&row); err == nil {
			if len(row) != 1 {
				return 0, errors.New("bigquery: count query returned an unexpected shape")
			}
			n, ok := row[0].(int64)
			if !ok {
				return 0, errors.New("bigquery: count is not an integer")
			}
			return n, nil
		}
	}
	if isGoogleStatus(err, http.StatusNotFound) || (err != nil && strings.Contains(err.Error(), "notFound")) {
		return 0, ErrBigQueryNotFound
	}
	return 0, err
}

func (g *goBigQuery) Close() error { return g.c.Close() }
