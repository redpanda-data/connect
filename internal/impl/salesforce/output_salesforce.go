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
// Messages are routed to the correct SObject configuration based on the "topic" field
// set by the per-topic processor. Each topic_mapping entry defines the SObject, operation,
// external ID field, and write mode for a given Kafka topic.

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
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
)

const (
	sinkModeRealtime = "realtime"
	sinkModeBulk     = "bulk"

	realtimeMaxChunkSize = 200 // Salesforce sObject Collections API limit
	bulkPollInterval     = 5 * time.Second
	defaultMaxBulkJobs    = 10   // max concurrent in-flight bulk jobs
	defaultBulkBatchSize  = 1000 // records per bulk job
)

// inFlightBulkJob tracks a bulk job submitted to Salesforce that is still being polled.
type inFlightBulkJob struct {
	jobID string
	errCh chan error // buffered(1); receives exactly one value when polling completes
}

// topicMapping holds the Salesforce write config for a single Kafka topic.
type topicMapping struct {
	sobject         string
	operation       string
	externalIDField string
	mode            string
	allOrNone       bool
}

// salesforceSinkOutput is the Redpanda Connect output implementation for writing to Salesforce.
type salesforceSinkOutput struct {
	log    *service.Logger
	client *salesforcehttp.Client

	// topicMappings maps Kafka topic name → Salesforce write config.
	topicMappings map[string]topicMapping

	// writableFields caches per-SObject updateable field sets, loaded lazily on first write.
	writableFieldsMu sync.RWMutex
	writableFields   map[string]map[string]struct{}
	blockedFields    map[string]map[string]struct{}

	// bulkJobs tracks in-flight bulk jobs being polled in background goroutines.
	bulkJobsMu  sync.Mutex
	bulkJobs    []*inFlightBulkJob
	maxBulkJobs int
}

func init() {
	if err := service.RegisterBatchOutput(
		"salesforce_sink", newSalesforceSinkConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			batchSize, err := conf.FieldInt("bulk_batch_size")
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			out, err := newSalesforceSinkOutput(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			return out, service.BatchPolicy{Count: batchSize, Period: "5s"}, 1, nil
		},
	); err != nil {
		panic(err)
	}
}

func newSalesforceSinkConfigSpec() *service.ConfigSpec {
	topicMappingSpec := service.NewObjectListField("topic_mappings",
		service.NewStringField("topic").
			Description("Kafka topic name to match against the message's 'topic' field"),
		service.NewStringField("sobject").
			Description("Salesforce SObject API name (e.g., Account, Contact, MyObject__c)"),
		service.NewStringField("operation").
			Description("Write operation: insert, update, upsert, or delete").
			Default("upsert"),
		service.NewStringField("external_id_field").
			Description("External ID field name, required for upsert").
			Default(""),
		service.NewStringField("mode").
			Description("Write mode: realtime (sObject Collections API) or bulk (Bulk API 2.0)").
			Default(sinkModeRealtime),
		service.NewBoolField("all_or_none").
			Description("Realtime only: roll back the entire batch if any record fails").
			Default(false),
	).Description("Per-topic Salesforce write configuration. Each entry maps a Kafka topic to an SObject and write settings.")

	return service.NewConfigSpec().
		Summary("Writes messages to Salesforce, routing each Kafka topic to its own SObject configuration.").
		Description(`Consumes batches of messages and writes them to Salesforce.

Each message must have a ` + "`topic`" + ` field (set by the per-topic processor) and a ` + "`data`" + ` field
containing the Salesforce record fields. The ` + "`topic`" + ` is used to look up the correct
` + "`topic_mappings`" + ` entry which defines the SObject, operation, and write mode.

**Realtime mode** uses the sObject Collections REST API (synchronous, up to 200 records/call).
**Bulk mode** uses the Bulk API 2.0 (asynchronous, polls until complete).

` + "```yaml" + `
input:
  broker:
    inputs:
      - kafka_franz:
          seed_brokers: [localhost:9092]
          topics: [salesforce-account]
          consumer_group: sf-sink-account
        processors:
          - mapping: |
              root.topic = @kafka_topic
              root.data = this
      - kafka_franz:
          seed_brokers: [localhost:9092]
          topics: [salesforce-contact]
          consumer_group: sf-sink-contact
        processors:
          - mapping: |
              root.topic = @kafka_topic
              root.data = this

output:
  salesforce_sink:
    org_url: "${SALESFORCE_ORG_URL}"
    client_id: "${SALESFORCE_CLIENT_ID}"
    client_secret: "${SALESFORCE_CLIENT_SECRET}"
    topic_mappings:
      - topic: salesforce-account
        sobject: Account
        operation: upsert
        external_id_field: External_Id__c
        mode: realtime
      - topic: salesforce-contact
        sobject: Contact
        operation: upsert
        external_id_field: External_Id__c
        mode: bulk
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
		Field(service.NewIntField("bulk_batch_size").
			Description("Number of records per bulk job. Also controls the output batch size.").
			Default(defaultBulkBatchSize)).
		Field(service.NewIntField("max_concurrent_bulk_jobs").
			Description("Maximum number of bulk jobs polling concurrently in the background.").
			Default(defaultMaxBulkJobs)).
		Field(topicMappingSpec)
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

	maxConcurrentBulkJobs, err := conf.FieldInt("max_concurrent_bulk_jobs")
	if err != nil {
		return nil, err
	}

	mappingConfs, err := conf.FieldObjectList("topic_mappings")
	if err != nil {
		return nil, err
	}
	if len(mappingConfs) == 0 {
		return nil, errors.New("topic_mappings must not be empty")
	}

	topicMappings := make(map[string]topicMapping, len(mappingConfs))
	for _, mc := range mappingConfs {
		topic, err := mc.FieldString("topic")
		if err != nil {
			return nil, err
		}
		sobject, err := mc.FieldString("sobject")
		if err != nil {
			return nil, err
		}
		operation, err := mc.FieldString("operation")
		if err != nil {
			return nil, err
		}
		switch operation {
		case "insert", "update", "upsert", "delete":
		default:
			return nil, fmt.Errorf("topic %q: invalid operation %q: must be insert, update, upsert, or delete", topic, operation)
		}
		externalIDField, err := mc.FieldString("external_id_field")
		if err != nil {
			return nil, err
		}
		if operation == "upsert" && externalIDField == "" {
			return nil, fmt.Errorf("topic %q: external_id_field is required when operation is upsert", topic)
		}
		mode, err := mc.FieldString("mode")
		if err != nil {
			return nil, err
		}
		if mode != sinkModeRealtime && mode != sinkModeBulk {
			return nil, fmt.Errorf("topic %q: invalid mode %q: must be realtime or bulk", topic, mode)
		}
		allOrNone, err := mc.FieldBool("all_or_none")
		if err != nil {
			return nil, err
		}
		topicMappings[topic] = topicMapping{
			sobject:         sobject,
			operation:       operation,
			externalIDField: externalIDField,
			mode:            mode,
			allOrNone:       allOrNone,
		}
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
		log:            mgr.Logger(),
		client:         sfClient,
		topicMappings:  topicMappings,
		writableFields: make(map[string]map[string]struct{}),
		blockedFields:  make(map[string]map[string]struct{}),
		maxBulkJobs:    maxConcurrentBulkJobs,
	}
	for topic, m := range topicMappings {
		out.log.Infof("salesforce_sink: topic=%s → sobject=%s operation=%s mode=%s", topic, m.sobject, m.operation, m.mode)
	}
	return out, nil
}

// Connect authenticates eagerly so that write failures are not caused by a missing token.
func (s *salesforceSinkOutput) Connect(ctx context.Context) error {
	if err := s.client.RefreshToken(ctx); err != nil {
		return fmt.Errorf("salesforce_sink: failed to authenticate: %w", err)
	}
	s.log.Infof("salesforce_sink: connected to Salesforce")
	return nil
}

// WriteBatch groups messages by topic and dispatches each group to the correct SObject config.
func (s *salesforceSinkOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Group messages by topic.
	groups := make(map[string][]map[string]any)
	for i, msg := range batch {
		raw, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("message[%d]: failed to read bytes: %w", i, err)
		}
		var envelope map[string]any
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return fmt.Errorf("message[%d]: failed to parse JSON: %w", i, err)
		}

		topic, _ := envelope["topic"].(string)

		// Unwrap "data" field as the actual Salesforce record.
		record := envelope
		if inner, ok := envelope["data"].(map[string]any); ok {
			record = inner
		}
		groups[topic] = append(groups[topic], record)
	}

	// Write each group using its topic mapping.
	for topic, records := range groups {
		mapping, ok := s.topicMappings[topic]
		if !ok {
			s.log.Warnf("salesforce_sink: no mapping for topic %q, skipping %d record(s)", topic, len(records))
			continue
		}
		s.log.Infof("salesforce_sink: writing %d record(s) to %s (mode=%s)", len(records), mapping.sobject, mapping.mode)
		switch mapping.mode {
		case sinkModeRealtime:
			if err := s.writeRealtime(ctx, records, mapping); err != nil {
				return err
			}
		case sinkModeBulk:
			if err := s.writeBulk(ctx, records, mapping); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *salesforceSinkOutput) Close(ctx context.Context) error {
	s.bulkJobsMu.Lock()
	jobs := s.bulkJobs
	s.bulkJobs = nil
	s.bulkJobsMu.Unlock()

	var firstErr error
	for _, job := range jobs {
		select {
		case err := <-job.errCh:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return firstErr
}

// getWritableFields returns the cached updateable field set for an SObject, fetching it on first call.
func (s *salesforceSinkOutput) getWritableFields(ctx context.Context, sobject string) (map[string]struct{}, error) {
	s.writableFieldsMu.RLock()
	fields, ok := s.writableFields[sobject]
	s.writableFieldsMu.RUnlock()
	if ok {
		return fields, nil
	}

	fields, err := s.client.DescribeWritableFields(ctx, sobject)
	if err != nil {
		return nil, err
	}

	s.writableFieldsMu.Lock()
	s.writableFields[sobject] = fields
	s.writableFieldsMu.Unlock()

	s.log.Infof("salesforce_sink: %s has %d updateable fields", sobject, len(fields))
	return fields, nil
}

// blockFields removes fields from the writable cache for sobject and logs all blocked fields so far.
// Called when Salesforce rejects fields with INVALID_FIELD_FOR_INSERT_UPDATE.
func (s *salesforceSinkOutput) blockFields(sobject string, newBlocked []string) {
	s.writableFieldsMu.Lock()
	defer s.writableFieldsMu.Unlock()
	fields := s.writableFields[sobject]
	var newlyBlocked []string
	for _, f := range newBlocked {
		if _, already := s.blockedFields[sobject][f]; !already {
			delete(fields, f)
			if s.blockedFields[sobject] == nil {
				s.blockedFields[sobject] = make(map[string]struct{})
			}
			s.blockedFields[sobject][f] = struct{}{}
			newlyBlocked = append(newlyBlocked, f)
		}
	}
	if len(newlyBlocked) > 0 {
		blocked := make([]string, 0, len(s.blockedFields[sobject]))
		for f := range s.blockedFields[sobject] {
			blocked = append(blocked, f)
		}
		s.log.Warnf("salesforce_sink: %s: profile-blocked fields (total %d): %v", sobject, len(blocked), blocked)
	}
}

// filterRecord returns a copy of rec containing only updateable fields.
func filterRecord(rec map[string]any, writable map[string]struct{}) map[string]any {
	out := make(map[string]any, len(writable))
	for k, v := range rec {
		if _, ok := writable[k]; ok {
			out[k] = v
		}
	}
	return out
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

func (s *salesforceSinkOutput) writeRealtime(ctx context.Context, records []map[string]any, m topicMapping) error {
	for start := 0; start < len(records); start += realtimeMaxChunkSize {
		end := start + realtimeMaxChunkSize
		if end > len(records) {
			end = len(records)
		}
		if err := s.writeRealtimeChunk(ctx, records[start:end], m); err != nil {
			return err
		}
	}
	return nil
}

func (s *salesforceSinkOutput) writeRealtimeChunk(ctx context.Context, records []map[string]any, m topicMapping) error {
	writable, err := s.getWritableFields(ctx, m.sobject)
	if err != nil {
		return fmt.Errorf("salesforce_sink: could not describe %s: %w", m.sobject, err)
	}

	wrapped := make([]map[string]any, len(records))
	for i, rec := range records {
		r := filterRecord(rec, writable)
		r["attributes"] = map[string]string{"type": m.sobject}
		wrapped[i] = r
	}

	payload := map[string]any{
		"allOrNone": m.allOrNone,
		"records":   wrapped,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal collections payload: %w", err)
	}

	path := realtimePath(m, s.client.APIVersion())
	var respBody []byte
	switch m.operation {
	case "upsert", "update":
		respBody, err = s.client.PatchJSON(ctx, path, body)
	default:
		respBody, err = s.client.PostJSON(ctx, path, body)
	}
	if err != nil {
		return fmt.Errorf("salesforce_sink realtime write failed: %w", err)
	}

	var results []collectionsResponse
	if err := json.Unmarshal(respBody, &results); err != nil {
		return fmt.Errorf("salesforce_sink: failed to parse collections response: %w", err)
	}

	// Collect profile-blocked fields reported by Salesforce and retry once with them removed.
	var blocked []string
	for _, res := range results {
		for _, e := range res.Errors {
			if e.StatusCode == "INVALID_FIELD_FOR_INSERT_UPDATE" {
				blocked = append(blocked, e.Fields...)
			}
		}
	}
	if len(blocked) > 0 {
		s.blockFields(m.sobject, blocked)
		return s.writeRealtimeChunk(ctx, records, m)
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

func realtimePath(m topicMapping, apiVersion string) string {
	base := "/services/data/" + apiVersion + "/composite/sobjects"
	switch m.operation {
	case "upsert":
		return base + "/" + m.sobject + "/" + m.externalIDField
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
	ID                   string `json:"id"`
	State                string `json:"state"`
	NumberRecordsFailed  int    `json:"numberRecordsFailed"`
	NumberRecordsProcessed int  `json:"numberRecordsProcessed"`
}

func (s *salesforceSinkOutput) writeBulk(ctx context.Context, records []map[string]any, m topicMapping) error {
	if len(records) == 0 {
		return nil
	}

	writable, err := s.getWritableFields(ctx, m.sobject)
	if err != nil {
		return fmt.Errorf("salesforce_sink: could not describe %s: %w", m.sobject, err)
	}
	filtered := make([]map[string]any, len(records))
	for i, rec := range records {
		filtered[i] = filterRecord(rec, writable)
	}
	records = filtered

	// Surface errors from any previously completed background jobs.
	if err := s.drainCompletedBulkJobs(); err != nil {
		return err
	}

	// If at max concurrency, block on the oldest in-flight job before submitting a new one.
	s.bulkJobsMu.Lock()
	for len(s.bulkJobs) >= s.maxBulkJobs {
		oldest := s.bulkJobs[0]
		s.bulkJobsMu.Unlock()
		select {
		case err := <-oldest.errCh:
			s.bulkJobsMu.Lock()
			s.bulkJobs = s.bulkJobs[1:]
			if err != nil {
				s.bulkJobsMu.Unlock()
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.bulkJobsMu.Unlock()

	// Submit: create → upload CSV → close (set UploadComplete). These are fast HTTP calls.
	jobID, err := s.bulkCreateJob(ctx, m)
	if err != nil {
		return fmt.Errorf("salesforce_sink bulk: create job: %w", err)
	}
	s.log.Infof("salesforce_sink bulk: created job %s (%d records)", jobID, len(records))

	csvData, err := recordsToCSV(records)
	if err != nil {
		return fmt.Errorf("salesforce_sink bulk: build CSV: %w", err)
	}
	if err := s.bulkUploadCSV(ctx, jobID, csvData); err != nil {
		return fmt.Errorf("salesforce_sink bulk: upload CSV: %w", err)
	}
	if err := s.bulkCloseJob(ctx, jobID); err != nil {
		return fmt.Errorf("salesforce_sink bulk: close job: %w", err)
	}

	// Poll in background so the next WriteBatch can proceed immediately.
	job := &inFlightBulkJob{jobID: jobID, errCh: make(chan error, 1)}
	go func() {
		if err := s.bulkPollUntilDone(context.Background(), jobID); err != nil {
			s.log.Errorf("salesforce_sink bulk: job %s failed: %v", jobID, err)
			job.errCh <- err
		} else {
			s.log.Infof("salesforce_sink bulk: job %s completed successfully", jobID)
			job.errCh <- nil
		}
	}()

	s.bulkJobsMu.Lock()
	s.bulkJobs = append(s.bulkJobs, job)
	s.bulkJobsMu.Unlock()
	return nil
}

// drainCompletedBulkJobs collects results from any finished background jobs without blocking.
// Returns the first error encountered so the caller can surface it before submitting new work.
func (s *salesforceSinkOutput) drainCompletedBulkJobs() error {
	s.bulkJobsMu.Lock()
	defer s.bulkJobsMu.Unlock()

	var firstErr error
	remaining := make([]*inFlightBulkJob, 0, len(s.bulkJobs))
	for _, job := range s.bulkJobs {
		select {
		case err := <-job.errCh:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		default:
			remaining = append(remaining, job) // still running
		}
	}
	s.bulkJobs = remaining
	return firstErr
}

func (s *salesforceSinkOutput) bulkCreateJob(ctx context.Context, m topicMapping) (string, error) {
	req := bulkJobRequest{
		Object:      m.sobject,
		Operation:   m.operation,
		ContentType: "CSV",
		LineEnding:  "LF",
	}
	if m.operation == "upsert" {
		req.ExternalIDFieldName = m.externalIDField
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

func (s *salesforceSinkOutput) bulkFetchFailedResults(ctx context.Context, jobID string, failed, processed int) error {
	path := "/services/data/" + s.client.APIVersion() + "/jobs/ingest/" + jobID + "/failedResults"
	body, err := s.client.GetJSON(ctx, path)
	if err != nil {
		return fmt.Errorf("bulk job %s: %d/%d record(s) failed (could not fetch details: %w)", jobID, failed, processed, err)
	}

	r := csv.NewReader(strings.NewReader(string(body)))
	rows, err := r.ReadAll()
	if err != nil || len(rows) < 2 {
		return fmt.Errorf("bulk job %s: %d/%d record(s) failed (could not parse failed results)", jobID, failed, processed)
	}

	// First row is headers; sf__Error is always present in the Bulk API response.
	headers := rows[0]
	errorIdx := -1
	for i, h := range headers {
		if h == "sf__Error" {
			errorIdx = i
			break
		}
	}

	var errs []string
	for i, row := range rows[1:] {
		msg := "unknown error"
		if errorIdx >= 0 && errorIdx < len(row) {
			msg = row[errorIdx]
		}
		errs = append(errs, fmt.Sprintf("record[%d]: %s", i, msg))
	}
	return fmt.Errorf("salesforce_sink bulk: job %s: %d/%d record(s) failed: %s", jobID, failed, processed, strings.Join(errs, "; "))
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
			if job.NumberRecordsFailed > 0 {
				return s.bulkFetchFailedResults(ctx, jobID, job.NumberRecordsFailed, job.NumberRecordsProcessed)
			}
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

// recordsToCSV serialises a slice of maps into a CSV byte slice.
func recordsToCSV(records []map[string]any) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

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
	return buf.Bytes(), w.Error()
}
