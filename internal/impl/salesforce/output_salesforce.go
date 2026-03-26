// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	sinkModeRealtime = "realtime"
	sinkModeBulk     = "bulk"

	realtimeMaxChunkSize = 200 // Salesforce sObject Collections API limit
	defaultMaxBulkJobs   = 10   // max concurrent in-flight bulk jobs
	defaultBulkBatchSize = 1000 // records per bulk job
)

const (
	sfsFieldOrgURL                = "org_url"
	sfsFieldClientID              = "client_id"
	sfsFieldClientSecret          = "client_secret"
	sfsFieldRESTAPIVersion        = "restapi_version"
	sfsFieldRequestTimeout        = "request_timeout"
	sfsFieldMaxRetries            = "max_retries"
	sfsFieldBulkBatchSize         = "bulk_batch_size"
	sfsFieldMaxConcurrentBulkJobs = "max_concurrent_bulk_jobs"
	sfsFieldBulkPollInterval      = "bulk_poll_interval"
	sfsFieldMaxInFlight           = "max_in_flight"
	sfsFieldTopicMappings         = "topic_mappings"
	sfsTMFieldTopic               = "topic"
	sfsTMFieldSObject             = "sobject"
	sfsTMFieldOperation           = "operation"
	sfsTMFieldExternalIDField     = "external_id_field"
	sfsTMFieldMode                = "mode"
	sfsTMFieldAllOrNone           = "all_or_none"
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
	bulkJobsMu       sync.Mutex
	bulkJobs         []*inFlightBulkJob
	bulkSem          chan struct{} // capacity = max_concurrent_bulk_jobs; held while a job is in flight
	bulkPollInterval time.Duration
	shutdownCtx    context.Context //nolint:containedctx // lifecycle context for background bulk-poll goroutines
	shutdownCancel context.CancelFunc
	closeOnce      sync.Once
}

func init() {
	service.MustRegisterBatchOutput(
		"salesforce_sink", newSalesforceSinkConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			batchSize, err := conf.FieldInt(sfsFieldBulkBatchSize)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			maxInFlight, err := conf.FieldInt(sfsFieldMaxInFlight)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			out, err := newSalesforceSinkOutput(conf, mgr)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			return out, service.BatchPolicy{Count: batchSize, Period: "5s"}, maxInFlight, nil
		},
	)
}

func newSalesforceSinkConfigSpec() *service.ConfigSpec {
	topicMappingSpec := service.NewObjectListField(sfsFieldTopicMappings,
		service.NewStringField(sfsTMFieldTopic).
			Description("Kafka topic name to match against the message's 'topic' field"),
		service.NewStringField(sfsTMFieldSObject).
			Description("Salesforce SObject API name (e.g., Account, Contact, MyObject__c)"),
		service.NewStringField(sfsTMFieldOperation).
			Description("Write operation: insert, update, upsert, or delete").
			Default("upsert"),
		service.NewStringField(sfsTMFieldExternalIDField).
			Description("External ID field name, required for upsert").
			Default(""),
		service.NewStringField(sfsTMFieldMode).
			Description("Write mode: realtime (sObject Collections API) or bulk (Bulk API 2.0)").
			Default(sinkModeRealtime),
		service.NewBoolField(sfsTMFieldAllOrNone).
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
		Field(service.NewStringField(sfsFieldOrgURL).
			Description("Salesforce instance base URL (e.g., https://your-domain.salesforce.com)")).
		Field(service.NewStringField(sfsFieldClientID).
			Description("Client ID for the Salesforce Connected App")).
		Field(service.NewStringField(sfsFieldClientSecret).
			Description("Client Secret for the Salesforce Connected App").
			Secret()).
		Field(service.NewStringField(sfsFieldRESTAPIVersion).
			Description("Salesforce REST API version to use (e.g., v65.0)").
			Default("v65.0")).
		Field(service.NewDurationField(sfsFieldRequestTimeout).
			Description("HTTP request timeout").
			Default("30s")).
		Field(service.NewIntField(sfsFieldMaxRetries).
			Description("Maximum number of retries on 429 Too Many Requests").
			Default(10)).
		Field(service.NewIntField(sfsFieldBulkBatchSize).
			Description("Number of records per bulk job. Also controls the output batch size.").
			Default(defaultBulkBatchSize)).
		Field(service.NewIntField(sfsFieldMaxConcurrentBulkJobs).
			Description("Maximum number of bulk jobs polling concurrently in the background. Each in-flight job buffers its CSV payload in memory; lower this value if memory usage is a concern.").
			Default(defaultMaxBulkJobs)).
		Field(service.NewDurationField(sfsFieldBulkPollInterval).
			Description("How often to poll Salesforce for bulk job completion status.").
			Default("5s")).
		Field(service.NewIntField(sfsFieldMaxInFlight).
			Description("Maximum number of batches to send concurrently. Increasing this improves realtime write throughput.").
			Default(1)).
		Field(topicMappingSpec)
}

func newSalesforceSinkOutput(conf *service.ParsedConfig, mgr *service.Resources) (*salesforceSinkOutput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	orgURL, err := conf.FieldString(sfsFieldOrgURL)
	if err != nil {
		return nil, err
	}
	if _, err := url.ParseRequestURI(orgURL); err != nil {
		return nil, errors.New("org_url is not a valid URL")
	}

	clientID, err := conf.FieldString(sfsFieldClientID)
	if err != nil {
		return nil, err
	}

	clientSecret, err := conf.FieldString(sfsFieldClientSecret)
	if err != nil {
		return nil, err
	}

	apiVersion, err := conf.FieldString(sfsFieldRESTAPIVersion)
	if err != nil {
		return nil, err
	}

	timeout, err := conf.FieldDuration(sfsFieldRequestTimeout)
	if err != nil {
		return nil, err
	}

	maxRetries, err := conf.FieldInt(sfsFieldMaxRetries)
	if err != nil {
		return nil, err
	}

	maxConcurrentBulkJobs, err := conf.FieldInt(sfsFieldMaxConcurrentBulkJobs)
	if err != nil {
		return nil, err
	}

	bulkPollInterval, err := conf.FieldDuration(sfsFieldBulkPollInterval)
	if err != nil {
		return nil, err
	}

	mappingConfs, err := conf.FieldObjectList(sfsFieldTopicMappings)
	if err != nil {
		return nil, err
	}
	if len(mappingConfs) == 0 {
		return nil, errors.New("topic_mappings must not be empty")
	}

	topicMappings := make(map[string]topicMapping, len(mappingConfs))
	for _, mc := range mappingConfs {
		topic, err := mc.FieldString(sfsTMFieldTopic)
		if err != nil {
			return nil, err
		}
		sobject, err := mc.FieldString(sfsTMFieldSObject)
		if err != nil {
			return nil, err
		}
		operation, err := mc.FieldString(sfsTMFieldOperation)
		if err != nil {
			return nil, err
		}
		switch operation {
		case "insert", "update", "upsert", "delete":
		default:
			return nil, fmt.Errorf("topic %q: invalid operation %q: must be insert, update, upsert, or delete", topic, operation)
		}
		externalIDField, err := mc.FieldString(sfsTMFieldExternalIDField)
		if err != nil {
			return nil, err
		}
		if operation == "upsert" && externalIDField == "" {
			return nil, fmt.Errorf("topic %q: external_id_field is required when operation is upsert", topic)
		}
		mode, err := mc.FieldString(sfsTMFieldMode)
		if err != nil {
			return nil, err
		}
		if mode != sinkModeRealtime && mode != sinkModeBulk {
			return nil, fmt.Errorf("topic %q: invalid mode %q: must be realtime or bulk", topic, mode)
		}
		allOrNone, err := mc.FieldBool(sfsTMFieldAllOrNone)
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

	sfClient, err := salesforcehttp.NewClient(salesforcehttp.ClientConfig{
		OrgURL:         orgURL,
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		APIVersion:     apiVersion,
		MaxRetries:     maxRetries,
		QueryBatchSize: 2000,
		HTTPClient:     &http.Client{Timeout: timeout},
		Logger:         mgr.Logger(),
		Metrics:        mgr.Metrics(),
	})
	if err != nil {
		return nil, err
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	out := &salesforceSinkOutput{
		log:            mgr.Logger(),
		client:         sfClient,
		topicMappings:  topicMappings,
		writableFields: make(map[string]map[string]struct{}),
		blockedFields:  make(map[string]map[string]struct{}),
		bulkSem:          make(chan struct{}, maxConcurrentBulkJobs),
		bulkPollInterval: bulkPollInterval,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}
	for topic, m := range topicMappings {
		out.log.Infof("salesforce_sink: topic=%s → sobject=%s operation=%s mode=%s", topic, m.sobject, m.operation, m.mode)
	}
	return out, nil
}

// Connect authenticates eagerly so that write failures are not caused by a missing token.
func (s *salesforceSinkOutput) Connect(ctx context.Context) error {
	if err := s.client.RefreshToken(ctx); err != nil {
		return fmt.Errorf("salesforce_sink: auth connection: %w", err)
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
			return fmt.Errorf("message[%d]:  read bytes: %w", i, err)
		}
		var envelope map[string]any
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return fmt.Errorf("message[%d]: parse JSON: %w", i, err)
		}

		topic, ok := envelope["topic"].(string)
		if !ok || topic == "" {
			return fmt.Errorf("message[%d]: missing or non-string \"topic\" field", i)
		}

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
		s.log.Debugf("salesforce_sink: writing %d record(s) to %s (mode=%s)", len(records), mapping.sobject, mapping.mode)
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
	var firstErr error
	s.closeOnce.Do(func() {
		s.shutdownCancel()

		s.bulkJobsMu.Lock()
		jobs := s.bulkJobs
		s.bulkJobs = nil
		s.bulkJobsMu.Unlock()

		for _, job := range jobs {
			select {
			case err := <-job.errCh:
				if err != nil && firstErr == nil {
					firstErr = err
				}
			case <-ctx.Done():
				firstErr = ctx.Err()
				return
			}
		}
	})
	return firstErr
}

// getWritableFields returns a copy of the cached updateable field set for an SObject, fetching it on first call.
// A copy is returned so callers can iterate safely while blockFields mutates the underlying map concurrently.
func (s *salesforceSinkOutput) getWritableFields(ctx context.Context, sobject string) (map[string]struct{}, error) {
	s.writableFieldsMu.RLock()
	fields, ok := s.writableFields[sobject]
	s.writableFieldsMu.RUnlock()
	if ok {
		return copyStringSet(fields), nil
	}

	fields, err := s.client.DescribeWritableFields(ctx, sobject)
	if err != nil {
		return nil, err
	}

	s.writableFieldsMu.Lock()
	s.writableFields[sobject] = fields
	s.writableFieldsMu.Unlock()

	s.log.Infof("salesforce_sink: %s has %d updateable fields", sobject, len(fields))
	return copyStringSet(fields), nil
}

func copyStringSet(m map[string]struct{}) map[string]struct{} {
	cp := make(map[string]struct{}, len(m))
	for k := range m {
		cp[k] = struct{}{}
	}
	return cp
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
		end := min(start+realtimeMaxChunkSize, len(records))
		if err := s.writeRealtimeChunk(ctx, records[start:end], m, 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *salesforceSinkOutput) writeRealtimeChunk(ctx context.Context, records []map[string]any, m topicMapping, retries int) error {
	writable, err := s.getWritableFields(ctx, m.sobject)
	if err != nil {
		return fmt.Errorf("salesforce_sink: could not describe %s: %w", m.sobject, err)
	}

	var respBody []byte

	if m.operation == "delete" {
		// DELETE /composite/sobjects?ids=id1,id2&allOrNone=false — no body.
		ids := make([]string, 0, len(records))
		for _, rec := range records {
			if id, ok := rec["Id"].(string); ok && id != "" {
				ids = append(ids, id)
			}
		}
		basePath, _ := url.JoinPath("/services/data", s.client.APIVersion(), "composite/sobjects")
		rawURL := s.client.OrgURL() + basePath + "?ids=" + strings.Join(ids, ",") + "&allOrNone=" + strconv.FormatBool(m.allOrNone)
		respBody, err = s.client.DeleteJSON(ctx, rawURL)
	} else {
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
		var body []byte
		body, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal collections payload: %w", err)
		}
		path := realtimePath(m, s.client.APIVersion())
		switch m.operation {
		case "upsert", "update":
			respBody, err = s.client.PatchJSON(ctx, path, body)
		default:
			respBody, err = s.client.PostJSON(ctx, path, body)
		}
	}
	if err != nil {
		return fmt.Errorf("salesforce_sink realtime write failed: %w", err)
	}

	var results []collectionsResponse
	if err := json.Unmarshal(respBody, &results); err != nil {
		return fmt.Errorf("salesforce_sink: parse collections response: %w", err)
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
		// Only retry for idempotent operations (upsert/update). For insert, already-succeeded
		// records have been committed by Salesforce and re-sending would create duplicates.
		// When allOrNone=false the batch is partially committed, so retrying the full chunk
		// is never safe for insert regardless of retries remaining.
		canRetry := retries > 0 && (m.operation != "insert" || m.allOrNone)
		if canRetry {
			return s.writeRealtimeChunk(ctx, records, m, retries-1)
		}
		s.log.Warnf("salesforce_sink: %s: giving up after profile-blocked field retry; some records may have failed", m.sobject)
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
		if m.allOrNone {
			// allOrNone=true: Salesforce rolled back the entire batch; propagate the error.
			return fmt.Errorf("salesforce_sink: %d record(s) failed: %s", len(errs), strings.Join(errs, "; "))
		}
		// allOrNone=false: already-committed records must not be retried. Log failures
		// as warnings and acknowledge the batch so Benthos does not cause duplicate writes.
		s.log.Warnf("salesforce_sink: %s: %d record(s) failed (partial, batch acknowledged): %s",
			m.sobject, len(errs), strings.Join(errs, "; "))
	}
	return nil
}

func realtimePath(m topicMapping, apiVersion string) string {
	switch m.operation {
	case "upsert":
		p, _ := url.JoinPath("/services/data", apiVersion, "composite/sobjects", m.sobject, m.externalIDField)
		return p
	default:
		p, _ := url.JoinPath("/services/data", apiVersion, "composite/sobjects")
		return p
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
	ID                     string `json:"id"`
	State                  string `json:"state"`
	NumberRecordsFailed    int    `json:"numberRecordsFailed"`
	NumberRecordsProcessed int    `json:"numberRecordsProcessed"`
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

	// Acquire a slot — blocks if at capacity, returns if ctx is cancelled.
	// Sending into a buffered channel is atomic: no TOCTOU between check and reserve.
	select {
	case s.bulkSem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Submit: create → upload CSV → close (set UploadComplete). These are fast HTTP calls.
	jobID, err := s.bulkCreateJob(ctx, m)
	if err != nil {
		<-s.bulkSem
		return fmt.Errorf("salesforce_sink bulk: create job: %w", err)
	}
	s.log.Infof("salesforce_sink bulk: created job %s (%d records)", jobID, len(records))

	csvData, err := recordsToCSV(records)
	if err != nil {
		if abortErr := s.bulkAbortJob(context.Background(), jobID); abortErr != nil {
			s.log.Warnf("salesforce_sink bulk: failed to abort job %s: %v", jobID, abortErr)
		}
		<-s.bulkSem
		return fmt.Errorf("salesforce_sink bulk: build CSV: %w", err)
	}
	if err := s.bulkUploadCSV(ctx, jobID, csvData); err != nil {
		if abortErr := s.bulkAbortJob(context.Background(), jobID); abortErr != nil {
			s.log.Warnf("salesforce_sink bulk: failed to abort job %s: %v", jobID, abortErr)
		}
		<-s.bulkSem
		return fmt.Errorf("salesforce_sink bulk: upload CSV: %w", err)
	}
	if err := s.bulkCloseJob(ctx, jobID); err != nil {
		if abortErr := s.bulkAbortJob(context.Background(), jobID); abortErr != nil {
			s.log.Warnf("salesforce_sink bulk: failed to abort job %s: %v", jobID, abortErr)
		}
		<-s.bulkSem
		return fmt.Errorf("salesforce_sink bulk: close job: %w", err)
	}

	// Poll in background so the next WriteBatch can proceed immediately.
	job := &inFlightBulkJob{jobID: jobID, errCh: make(chan error, 1)}
	go func() {
		defer func() { <-s.bulkSem }()
		if err := s.bulkPollUntilDone(s.shutdownCtx, jobID); err != nil {
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

	path, _ := url.JoinPath("/services/data", s.client.APIVersion(), "jobs/ingest")
	respBody, err := s.client.PostJSON(ctx, path, body)
	if err != nil {
		return "", err
	}

	var resp bulkJobResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return "", fmt.Errorf("parse job create response: %w", err)
	}
	return resp.ID, nil
}

func (s *salesforceSinkOutput) bulkUploadCSV(ctx context.Context, jobID string, csvData []byte) error {
	path, _ := url.JoinPath("/services/data", s.client.APIVersion(), "jobs/ingest", jobID, "batches")
	_, err := s.client.PutCSV(ctx, path, csvData)
	return err
}

func (s *salesforceSinkOutput) bulkAbortJob(ctx context.Context, jobID string) error {
	body, err := json.Marshal(map[string]string{"state": "Aborted"})
	if err != nil {
		return fmt.Errorf("internal error marshalling abort-job request: %w", err)
	}
	path, _ := url.JoinPath("/services/data", s.client.APIVersion(), "jobs/ingest", jobID)
	_, err = s.client.PatchJSON(ctx, path, body)
	return err
}

func (s *salesforceSinkOutput) bulkCloseJob(ctx context.Context, jobID string) error {
	body, err := json.Marshal(map[string]string{"state": "UploadComplete"})
	if err != nil {
		return fmt.Errorf("internal error marshalling close-job request: %w", err)
	}
	path, _ := url.JoinPath("/services/data", s.client.APIVersion(), "jobs/ingest", jobID)
	_, err = s.client.PatchJSON(ctx, path, body)
	return err
}

func (s *salesforceSinkOutput) bulkFetchFailedResults(ctx context.Context, jobID string, failed, processed int) error {
	path, _ := url.JoinPath("/services/data", s.client.APIVersion(), "jobs/ingest", jobID, "failedResults")
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
	path, _ := url.JoinPath("/services/data", s.client.APIVersion(), "jobs/ingest", jobID)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.bulkPollInterval):
		}

		respBody, err := s.client.GetJSON(ctx, path)
		if err != nil {
			return fmt.Errorf("poll job status: %w", err)
		}

		var job bulkJobResponse
		if err := json.Unmarshal(respBody, &job); err != nil {
			return fmt.Errorf("parse job status: %w", err)
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
	sort.Strings(headers)

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
