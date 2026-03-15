// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// output_salesforce.go implements a Redpanda Connect output component that writes
// messages to Salesforce using either the sObject Collections REST API (realtime mode)
// or the Bulk API 2.0 (bulk mode).
//
// Each call to WriteBatch receives a batch of messages. In realtime mode the batch is
// split into chunks of up to 200 records and written synchronously. In bulk mode a
// full async job lifecycle is executed: create → upload CSV → close → poll → results.

package salesforce

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
)

const (
	sinkModeRealtime = "realtime"
	sinkModeBulk     = "bulk"

	realtimeMaxChunkSize = 200 // Salesforce sObject Collections API limit
	bulkPollInterval     = 5 * time.Second
)

// salesforceSinkOutput is the Redpanda Connect output implementation for writing to Salesforce.
type salesforceSinkOutput struct {
	log    *service.Logger
	client *salesforcehttp.Client

	sobject         string
	operation       string // insert | update | upsert | delete
	externalIDField string // required for upsert
	mode            string // realtime | bulk
	allOrNone       bool   // realtime only: fail whole batch on any error
}

func init() {
	if err := service.RegisterBatchOutput(
		"salesforce_sink", newSalesforceSinkConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			out, err := newSalesforceSinkOutput(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			// Max in-flight: 1 to ensure ordering; users can increase via pipeline threads.
			return out, service.BatchPolicy{Count: 200, Period: "1s"}, 1, nil
		},
	); err != nil {
		panic(err)
	}
}

func newSalesforceSinkConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Writes messages to Salesforce using either the sObject Collections REST API or the Bulk API 2.0.").
		Description(`Consumes batches of messages and writes them to a Salesforce SObject.

**Realtime mode** (default) uses the sObject Collections API, which is synchronous and supports up to 200 records per call.
Use this for low-latency writes where immediate feedback is required.

**Bulk mode** uses the Bulk API 2.0, which is asynchronous and designed for large data volumes.
The output creates a job, uploads all records as CSV, then polls until the job completes.

## Operations

| Operation | Description                                       |
|-----------|---------------------------------------------------|
| insert    | Create new records                                |
| update    | Update existing records by Salesforce ID          |
| upsert    | Insert or update by an external ID field          |
| delete    | Delete records by Salesforce ID                   |

## Input format

Each message must be a JSON object whose keys map to Salesforce field API names.
For **upsert**, the object must include the external ID field configured via ` + "`external_id_field`" + `.
For **update** and **delete**, the object must include the Salesforce record ` + "`Id`" + `.

## Consuming from multiple Kafka topics

Use a ` + "`broker`" + ` input to fan-in from multiple topics and apply per-topic transformations:

` + "```yaml" + `
input:
  broker:
    inputs:
      - kafka_franz:
          seed_brokers: [localhost:9092]
          topics: [salesforce-account]
          consumer_group: sf-sink-account
        processors:
          - mapping: 'root.sobject = "Account"'
      - kafka_franz:
          seed_brokers: [localhost:9092]
          topics: [salesforce-contact]
          consumer_group: sf-sink-contact
        processors:
          - mapping: 'root.sobject = "Contact"'

pipeline:
  processors:
    - mapping: |
        root = this.without("sobject")

output:
  salesforce_sink:
    org_url: "${SALESFORCE_ORG_URL}"
    client_id: "${SALESFORCE_CLIENT_ID}"
    client_secret: "${SALESFORCE_CLIENT_SECRET}"
    sobject: Account
    operation: upsert
    external_id_field: External_Id__c
    mode: realtime
` + "```").
		Field(service.NewStringField("org_url").
			Description("Salesforce instance base URL (e.g., https://your-domain.salesforce.com)")).
		Field(service.NewStringField("client_id").
			Description("Client ID for the Salesforce Connected App")).
		Field(service.NewStringField("client_secret").
			Description("Client Secret for the Salesforce Connected App").
			Secret()).
		Field(service.NewStringField("restapi_version").
			Description("Salesforce REST API version to use (e.g., v65.0)").
			Default("v65.0")).
		Field(service.NewDurationField("request_timeout").
			Description("HTTP request timeout").
			Default("30s")).
		Field(service.NewIntField("max_retries").
			Description("Maximum number of retries on 429 Too Many Requests").
			Default(10)).
		Field(service.NewStringField("sobject").
			Description("Salesforce SObject API name to write to (e.g., Account, Contact, MyObject__c)")).
		Field(service.NewStringField("operation").
			Description("Write operation: insert, update, upsert, or delete").
			Default("upsert")).
		Field(service.NewStringField("external_id_field").
			Description("External ID field name used for upsert operations").
			Default("")).
		Field(service.NewStringField("mode").
			Description("Write mode: realtime (sObject Collections API, ≤200 records/call) or bulk (Bulk API 2.0, async)").
			Default(sinkModeRealtime)).
		Field(service.NewBoolField("all_or_none").
			Description("Realtime mode only: if true, the entire batch is rolled back if any record fails").
			Default(false))
}

func newSalesforceSinkOutput(conf *service.ParsedConfig, mgr *service.Resources) (*salesforceSinkOutput, error) {
	orgURL, err := conf.FieldString("org_url")
	if err != nil {
		return nil, err
	}
	if _, err := url.ParseRequestURI(orgURL); err != nil {
		return nil, errors.New("org_url is not a valid URL")
	}

	clientID, err := conf.FieldString("client_id")
	if err != nil {
		return nil, err
	}

	clientSecret, err := conf.FieldString("client_secret")
	if err != nil {
		return nil, err
	}

	apiVersion, err := conf.FieldString("restapi_version")
	if err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration("request_timeout")
	if err != nil {
		return nil, err
	}

	maxRetries, err := conf.FieldInt("max_retries")
	if err != nil {
		return nil, err
	}

	sobject, err := conf.FieldString("sobject")
	if err != nil {
		return nil, err
	}
	if sobject == "" {
		return nil, errors.New("sobject must not be empty")
	}

	operation, err := conf.FieldString("operation")
	if err != nil {
		return nil, err
	}
	switch operation {
	case "insert", "update", "upsert", "delete":
	default:
		return nil, fmt.Errorf("invalid operation %q: must be insert, update, upsert, or delete", operation)
	}

	externalIDField, err := conf.FieldString("external_id_field")
	if err != nil {
		return nil, err
	}
	if operation == "upsert" && externalIDField == "" {
		return nil, errors.New("external_id_field is required when operation is upsert")
	}

	mode, err := conf.FieldString("mode")
	if err != nil {
		return nil, err
	}
	if mode != sinkModeRealtime && mode != sinkModeBulk {
		return nil, fmt.Errorf("invalid mode %q: must be realtime or bulk", mode)
	}

	allOrNone, err := conf.FieldBool("all_or_none")
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{Timeout: timeout}
	sfClient, err := salesforcehttp.NewClient(
		orgURL, clientID, clientSecret, apiVersion,
		maxRetries, 2000, httpClient, mgr.Logger(), mgr.Metrics(),
	)
	if err != nil {
		return nil, err
	}

	out := &salesforceSinkOutput{
		log:             mgr.Logger(),
		client:          sfClient,
		sobject:         sobject,
		operation:       operation,
		externalIDField: externalIDField,
		mode:            mode,
		allOrNone:       allOrNone,
	}
	out.log.Infof("salesforce_sink: initialised (sobject=%s operation=%s mode=%s)", sobject, operation, mode)
	return out, nil
}

// Connect authenticates eagerly so that write failures are not caused by a missing token.
func (s *salesforceSinkOutput) Connect(ctx context.Context) error {
	if err := s.client.RefreshToken(ctx); err != nil {
		return fmt.Errorf("salesforce_sink: failed to authenticate: %w", err)
	}
	s.log.Infof("salesforce_sink: connected (sobject=%s operation=%s mode=%s)", s.sobject, s.operation, s.mode)
	return nil
}

// WriteBatch dispatches to the configured mode.
func (s *salesforceSinkOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	s.log.Infof("salesforce_sink: received batch of %d message(s) (sobject=%s)", len(batch), s.sobject)
	for i, msg := range batch {
		raw, _ := msg.AsBytes()
		s.log.Infof("salesforce_sink: message[%d]: %s", i, string(raw))
	}

	records, err := batchToRecords(batch)
	if err != nil {
		return fmt.Errorf("salesforce_sink: failed to parse batch: %w", err)
	}

	switch s.mode {
	case sinkModeRealtime:
		return s.writeRealtime(ctx, records)
	case sinkModeBulk:
		return s.writeBulk(ctx, records)
	default:
		return fmt.Errorf("salesforce_sink: unknown mode %q", s.mode)
	}
}

func (*salesforceSinkOutput) Close(_ context.Context) error {
	return nil
}

// ---------------------------------------------------------------------------
// Realtime mode — sObject Collections REST API
// ---------------------------------------------------------------------------

// collectionsResponse is one element in the array returned by the collections API.
type collectionsResponse struct {
	ID      string               `json:"id"`
	Success bool                 `json:"success"`
	Errors  []salesforceAPIError `json:"errors"`
}

type salesforceAPIError struct {
	StatusCode string   `json:"statusCode"`
	Message    string   `json:"message"`
	Fields     []string `json:"fields"`
}

func (s *salesforceSinkOutput) writeRealtime(ctx context.Context, records []map[string]any) error {
	// Chunk into groups of realtimeMaxChunkSize
	for start := 0; start < len(records); start += realtimeMaxChunkSize {
		end := start + realtimeMaxChunkSize
		if end > len(records) {
			end = len(records)
		}
		if err := s.writeRealtimeChunk(ctx, records[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *salesforceSinkOutput) writeRealtimeChunk(ctx context.Context, records []map[string]any) error {
	// Inject the `attributes` wrapper required by sObject Collections
	wrapped := make([]map[string]any, len(records))
	for i, rec := range records {
		r := make(map[string]any, len(rec)+1)
		for k, v := range rec {
			r[k] = v
		}
		r["attributes"] = map[string]string{"type": s.sobject}
		wrapped[i] = r
	}

	payload := map[string]any{
		"allOrNone": s.allOrNone,
		"records":   wrapped,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal collections payload: %w", err)
	}

	// upsert and update use PATCH; insert uses POST; delete uses POST with _HttpMethod=DELETE override.
	path := s.realtimePath()
	var respBody []byte
	switch s.operation {
	case "upsert", "update":
		respBody, err = s.client.PatchJSON(ctx, path, body)
	default:
		respBody, err = s.client.PostJSON(ctx, path, body)
	}
	if err != nil {
		return fmt.Errorf("salesforce_sink realtime write failed: %w", err)
	}

	// Parse per-record results
	var results []collectionsResponse
	if err := json.Unmarshal(respBody, &results); err != nil {
		return fmt.Errorf("salesforce_sink: failed to parse collections response: %w", err)
	}

	var errs []string
	for i, res := range results {
		if !res.Success {
			for _, e := range res.Errors {
				errs = append(errs, fmt.Sprintf("record[%d]: %s - %s", i, e.StatusCode, e.Message))
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("salesforce_sink: %d record(s) failed: %s", len(errs), strings.Join(errs, "; "))
	}
	return nil
}

// realtimePath returns the REST API path for the configured operation.
func (s *salesforceSinkOutput) realtimePath() string {
	base := "/services/data/" + s.client.APIVersion() + "/composite/sobjects"
	switch s.operation {
	case "upsert":
		return base + "/" + s.sobject + "/" + s.externalIDField
	case "delete":
		return base + "?_HttpMethod=DELETE"
	default:
		return base
	}
}

// ---------------------------------------------------------------------------
// Bulk mode — Bulk API 2.0
// ---------------------------------------------------------------------------

type bulkJobRequest struct {
	Object              string `json:"object"`
	Operation           string `json:"operation"`
	ExternalIDFieldName string `json:"externalIdFieldName,omitempty"`
	ContentType         string `json:"contentType"`
	LineEnding          string `json:"lineEnding"`
}

type bulkJobResponse struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func (s *salesforceSinkOutput) writeBulk(ctx context.Context, records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}

	// 1. Create job
	jobID, err := s.bulkCreateJob(ctx)
	if err != nil {
		return fmt.Errorf("salesforce_sink bulk: create job: %w", err)
	}
	s.log.Infof("salesforce_sink bulk: created job %s", jobID)

	// 2. Upload CSV
	csvData, err := recordsToCSV(records)
	if err != nil {
		return fmt.Errorf("salesforce_sink bulk: build CSV: %w", err)
	}
	if err := s.bulkUploadCSV(ctx, jobID, csvData); err != nil {
		return fmt.Errorf("salesforce_sink bulk: upload CSV: %w", err)
	}

	// 3. Close job (signals Salesforce to start processing)
	if err := s.bulkCloseJob(ctx, jobID); err != nil {
		return fmt.Errorf("salesforce_sink bulk: close job: %w", err)
	}

	// 4. Poll until complete
	if err := s.bulkPollUntilDone(ctx, jobID); err != nil {
		return fmt.Errorf("salesforce_sink bulk: poll: %w", err)
	}

	s.log.Infof("salesforce_sink bulk: job %s completed successfully", jobID)
	return nil
}

func (s *salesforceSinkOutput) bulkCreateJob(ctx context.Context) (string, error) {
	req := bulkJobRequest{
		Object:      s.sobject,
		Operation:   s.operation,
		ContentType: "CSV",
		LineEnding:  "LF",
	}
	if s.operation == "upsert" {
		req.ExternalIDFieldName = s.externalIDField
	}

	body, err := json.Marshal(req)
	if err != nil {
		return "", err
	}

	path := "/services/data/" + s.client.APIVersion() + "/jobs/ingest"
	respBody, err := s.client.PostJSON(ctx, path, body)
	if err != nil {
		return "", err
	}

	var resp bulkJobResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return "", fmt.Errorf("failed to parse job create response: %w", err)
	}
	return resp.ID, nil
}

func (s *salesforceSinkOutput) bulkUploadCSV(ctx context.Context, jobID string, csvData []byte) error {
	path := "/services/data/" + s.client.APIVersion() + "/jobs/ingest/" + jobID + "/batches"
	_, err := s.client.PutCSV(ctx, path, csvData)
	return err
}

func (s *salesforceSinkOutput) bulkCloseJob(ctx context.Context, jobID string) error {
	body, _ := json.Marshal(map[string]string{"state": "UploadComplete"})
	path := "/services/data/" + s.client.APIVersion() + "/jobs/ingest/" + jobID
	_, err := s.client.PatchJSON(ctx, path, body)
	return err
}

func (s *salesforceSinkOutput) bulkPollUntilDone(ctx context.Context, jobID string) error {
	path := "/services/data/" + s.client.APIVersion() + "/jobs/ingest/" + jobID

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(bulkPollInterval):
		}

		respBody, err := s.client.GetJSON(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to poll job status: %w", err)
		}

		var job bulkJobResponse
		if err := json.Unmarshal(respBody, &job); err != nil {
			return fmt.Errorf("failed to parse job status: %w", err)
		}

		switch job.State {
		case "JobComplete":
			return nil
		case "Failed", "Aborted":
			return fmt.Errorf("bulk job %s ended with state %q", jobID, job.State)
		default:
			s.log.Debugf("salesforce_sink bulk: job %s state=%s", jobID, job.State)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// batchToRecords converts a MessageBatch into a slice of JSON maps.
func batchToRecords(batch service.MessageBatch) ([]map[string]any, error) {
	records := make([]map[string]any, 0, len(batch))
	for i, msg := range batch {
		raw, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message[%d]: failed to read bytes: %w", i, err)
		}
		var rec map[string]any
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("message[%d]: failed to parse JSON: %w", i, err)
		}
		records = append(records, rec)
	}
	return records, nil
}

// recordsToCSV serialises a slice of maps into a CSV byte slice.
// All keys from the first record are used as headers; missing fields in subsequent
// records are left empty.
func recordsToCSV(records []map[string]any) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Collect ordered headers from the first record
	headers := make([]string, 0, len(records[0]))
	for k := range records[0] {
		headers = append(headers, k)
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)

	if err := w.Write(headers); err != nil {
		return nil, err
	}
	for _, rec := range records {
		row := make([]string, len(headers))
		for i, h := range headers {
			if v, ok := rec[h]; ok && v != nil {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		if err := w.Write(row); err != nil {
			return nil, err
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
