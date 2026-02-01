// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/confx"

	"github.com/twmb/franz-go/pkg/sr"
)

// Versions represents which schema versions to migrate
type Versions string

// Supported versions
const (
	VersionsLatest Versions = "latest"
	VersionsAll    Versions = "all"
)

// String returns the string representation of the versions setting
func (v Versions) String() string {
	return string(v)
}

// ParseVersions parses a string into a Versions setting
func ParseVersions(s string) (Versions, error) {
	switch s {
	case string(VersionsLatest):
		return VersionsLatest, nil
	case string(VersionsAll):
		return VersionsAll, nil
	default:
		return "", fmt.Errorf("invalid versions setting: %s", s)
	}
}

const (
	srObjectField = "schema_registry"

	// Schema registry fields
	srFieldURL     = "url"
	srFieldTimeout = "timeout"
	srFieldTLS     = "tls"

	// Schema registry migrator fields
	srFieldEnabled        = "enabled"
	srFieldInterval       = "interval"
	srFieldInclude        = "include"
	srFieldExclude        = "exclude"
	srFieldSubject        = "subject"
	srFieldVersions       = "versions"
	srFieldIncludeDeleted = "include_deleted"
	srFieldTranslateIDs   = "translate_ids"
	srFieldNormalize      = "normalize"
	srFieldStrict         = "strict"
	srFieldWorkers        = "workers"
	srFieldBatchSize      = "batch_size"

	defaultWorkers      = 10
	defaultBatchSize    = 100
	progressLogInterval = 1000
)

func schemaRegistryField(extraFields ...*service.ConfigField) *service.ConfigField {
	fields := append(
		[]*service.ConfigField{
			service.NewStringField(srFieldURL).
				Description("The base URL of the schema registry service. Required for schema migration functionality.").
				Example("http://localhost:8081").
				Example("https://schema-registry.example.com:8081"),
			service.NewDurationField(srFieldTimeout).
				Description("HTTP client timeout for schema registry requests.").
				Default("5s").
				Optional(),
			service.NewTLSToggledField(srFieldTLS),
		},
		service.NewHTTPRequestAuthSignerFields()...)
	fields = append(fields, extraFields...)

	return service.NewObjectField(srObjectField, fields...).
		Description("Configuration for schema registry integration. Enables migration of schema subjects, versions, and compatibility settings between clusters.")
}

func schemaRegistryMigratorFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBoolField(srFieldEnabled).
			Description("Whether schema registry migration is enabled. When disabled, no schema operations are performed.").
			Default(true),
		service.NewDurationField(srFieldInterval).
			Description("How often to synchronise schema registry subjects. Set to 0s for one-time sync at startup only.").
			Example("0s     # One-time sync only").
			Example("5m     # Sync every 5 minutes").
			Example("30m    # Sync every 30 minutes").
			Default("5m"),
		service.NewStringListField(srFieldInclude).
			Description("Regular expressions for schema subjects to include in migration. " +
				"If empty, all subjects are included (unless excluded). " +
				"Note: the migrator consumer group is always ignored.").
			Example(`["prod-.*", "staging-.*"]`).
			Example(`["user-.*", "order-.*"]`).
			Optional(),
		service.NewStringListField(srFieldExclude).
			Description("Regular expressions for schema subjects to exclude from migration. " +
				"Takes precedence over include patterns. " +
				"Note: the migrator consumer group is always ignored.").
			Example(`[".*-test", ".*-temp"]`).
			Example(`["dev-.*", "local-.*"]`).
			Optional(),
		service.NewInterpolatedStringField(srFieldSubject).
			Description("Template for transforming subject names during migration. Use interpolation to rename subjects systematically.").
			Example(`prod_${! metadata("schema_registry_subject") }`).
			Example(`${! metadata("schema_registry_subject") | replace("dev_", "prod_") }`).
			Optional(),
		service.NewStringEnumField(srFieldVersions, VersionsLatest.String(), VersionsAll.String()).
			Description("Which schema versions to migrate. 'latest' migrates only the current version, 'all' migrates complete version history for better compatibility.").
			Default(VersionsAll.String()),
		service.NewBoolField(srFieldIncludeDeleted).
			Description("Whether to include soft-deleted schemas in migration. Useful for complete migration but may not be supported by all schema registries.").
			Default(false),
		service.NewBoolField(srFieldTranslateIDs).
			Description("Whether to translate schema IDs during migration.").
			Default(false),
		service.NewBoolField(srFieldNormalize).
			Description("Whether to normalize schemas when creating them in the destination registry.").
			Default(false),
		service.NewBoolField(srFieldStrict).
			Description("Error on unknown schema IDs. Only relevant when translate_ids is true. " +
				"When false (default), unknown schema IDs are passed through unchanged, " +
				"allowing migration of topics with mixed message formats. " +
				"Note: messages with 0-byte prefixes (e.g., protobuf) cannot be distinguished from schema registry headers and may fail when strict is enabled.").
			Default(false).
			LintRule(`root = if this && !this.schema_registry.translate_ids { "strict is only relevant when translate_ids is true" }`),
		service.NewIntField(srFieldWorkers).
			Description("Number of parallel workers for schema sync operations. Higher values improve throughput for large schema counts.").
			Default(defaultWorkers),
		service.NewIntField(srFieldBatchSize).
			Description("Number of subjects to fetch and sync per batch. Schemas are streamed in batches rather than fetched all at once, reducing memory usage and providing real-time progress for large migrations.").
			Default(defaultBatchSize),
	}
}

func schemaRegistryClientAndURLFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*sr.Client, string, error) {
	if !pConf.Contains("schema_registry") {
		return nil, "", nil
	}
	pConf = pConf.Namespace(srObjectField)

	// If the enabled flag exists and is set to false, short-circuit without creating a client.
	if pConf.Contains(srFieldEnabled) {
		enabled, err := pConf.FieldBool(srFieldEnabled)
		if err != nil {
			return nil, "", err
		}
		if !enabled {
			return nil, "", nil
		}
	}

	srURL, err := pConf.FieldURL(srFieldURL)
	if err != nil {
		return nil, "", err
	}

	timeout, err := pConf.FieldDuration(srFieldTimeout)
	if err != nil {
		return nil, "", err
	}

	reqSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, "", err
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(srFieldTLS)
	if err != nil {
		return nil, "", err
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	opts := []sr.ClientOpt{
		sr.HTTPClient(&http.Client{Timeout: timeout}),
		sr.UserAgent("franz-go"),
		sr.URLs(srURL.String()),
	}

	if tlsConf != nil {
		opts = append(opts, sr.DialTLSConfig(tlsConf))
	}
	if reqSigner != nil {
		opts = append(opts, sr.PreReq(func(req *http.Request) error { return reqSigner(mgr.FS(), req) }))
	}
	client, err := sr.NewClient(opts...)
	return client, srURL.String(), err
}

// SchemaRegistryMigratorConfig configures subject selection, transformation,
// and copy behaviour for schema registry migration.
type SchemaRegistryMigratorConfig struct {
	// Enabled toggles schema registry migration.
	Enabled bool
	// Interval controls how often to synchronise schemas. Zero means one-shot.
	Interval time.Duration
	confx.RegexpFilter
	// NameResolver sets per-subject names using an interpolated template.
	NameResolver *service.InterpolatedString
	// CompatibilityLevel sets per-subject compatibility level.
	CompatibilityLevel *service.InterpolatedString
	// Versions selects which schema versions to migrate (latest or all).
	Versions Versions
	// IncludeDeleted also copies soft-deleted subjects and marks them deleted
	// in the target.
	IncludeDeleted bool
	// TranslateIDs enables schema ID translation during migration.
	TranslateIDs bool
	// Normalize toggles schema normalization on create.
	Normalize bool
	// Strict controls if DestinationSchemaID should error if the
	// source schema ID is unknown.
	Strict bool
	// Serverless narrows the set of schema configuration keys to those
	// supported by serverless clusters.
	Serverless bool
	// Workers is the number of parallel workers for schema sync operations.
	Workers int
	// BatchSize is the number of subjects to fetch and sync per batch.
	BatchSize int
}

// initFromParsed initializes the schema registry migrator with configuration from parsed config.
func (m *SchemaRegistryMigratorConfig) initFromParsed(pConf *service.ParsedConfig) error {
	if !pConf.Contains("schema_registry") {
		return nil
	}

	var err error

	// Enabled flag
	if m.Enabled, err = pConf.FieldBool(srObjectField, srFieldEnabled); err != nil {
		return fmt.Errorf("parse enabled setting: %w", err)
	}

	// Parse interval
	if m.Interval, err = pConf.FieldDuration(srObjectField, srFieldInterval); err != nil {
		return fmt.Errorf("parse interval setting: %w", err)
	}

	// Parse include regex patterns
	if pConf.Contains(srObjectField, srFieldInclude) {
		patterns, err := pConf.FieldStringList(srObjectField, srFieldInclude)
		if err != nil {
			return fmt.Errorf("parse include patterns: %w", err)
		}
		m.Include, err = confx.ParseRegexpPatterns(patterns)
		if err != nil {
			return fmt.Errorf("invalid include regex patterns: %w", err)
		}
	}

	// Parse exclude regex patterns
	if pConf.Contains(srObjectField, srFieldExclude) {
		patterns, err := pConf.FieldStringList(srObjectField, srFieldExclude)
		if err != nil {
			return fmt.Errorf("parse exclude patterns: %w", err)
		}
		m.Exclude, err = confx.ParseRegexpPatterns(patterns)
		if err != nil {
			return fmt.Errorf("invalid exclude regex patterns: %w", err)
		}
	}

	// Parse subject transform
	if pConf.Contains(srObjectField, srFieldSubject) {
		if m.NameResolver, err = pConf.FieldInterpolatedString(srObjectField, srFieldSubject); err != nil {
			return fmt.Errorf("parse subject transform: %w", err)
		}
	}

	// Parse versions setting
	{
		var versionsStr string
		if versionsStr, err = pConf.FieldString(srObjectField, srFieldVersions); err != nil {
			return fmt.Errorf("parse versions setting: %w", err)
		}
		if m.Versions, err = ParseVersions(versionsStr); err != nil {
			return fmt.Errorf("parse versions setting: %w", err)
		}
	}

	// Parse boolean flags
	if m.IncludeDeleted, err = pConf.FieldBool(srObjectField, srFieldIncludeDeleted); err != nil {
		return fmt.Errorf("parse soft_delete setting: %w", err)
	}
	if m.TranslateIDs, err = pConf.FieldBool(srObjectField, srFieldTranslateIDs); err != nil {
		return fmt.Errorf("parse translate_ids setting: %w", err)
	}
	if m.Normalize, err = pConf.FieldBool(srObjectField, srFieldNormalize); err != nil {
		return fmt.Errorf("parse normalize setting: %w", err)
	}
	if m.Strict, err = pConf.FieldBool(srObjectField, srFieldStrict); err != nil {
		return fmt.Errorf("parse strict setting: %w", err)
	}
	if m.Workers, err = pConf.FieldInt(srObjectField, srFieldWorkers); err != nil {
		return fmt.Errorf("parse workers setting: %w", err)
	}
	if m.Workers <= 0 {
		m.Workers = defaultWorkers
	}
	if m.BatchSize, err = pConf.FieldInt(srObjectField, srFieldBatchSize); err != nil {
		return fmt.Errorf("parse batch_size setting: %w", err)
	}
	if m.BatchSize <= 0 {
		m.BatchSize = defaultBatchSize
	}

	// Use serverless from migrator config
	m.Serverless, err = pConf.FieldBool(rmoFieldServerless)
	if err != nil {
		return fmt.Errorf("get serverless field: %w", err)
	}

	return nil
}

type schemaInfo struct {
	Subject string
	Version int
	ID      int
}

func schemaInfoFromSubjectSchema(ss sr.SubjectSchema) schemaInfo {
	return schemaInfo{
		Subject: ss.Subject,
		Version: ss.Version,
		ID:      ss.ID,
	}
}

// schemaRegistryMigrator coordinates migration between a source and destination
// Schema Registry.
//
// Responsibilities:
//   - Manage configuration and source/destination Schema Registry clients.
//   - List and filter subjects (by include/exclude) and select versions to migrate.
//   - Copy schemas to the destination (fixed IDs or translated IDs).
//   - Apply per-subject compatibility on the destination.
//   - Run one-off Sync and periodic SyncLoop.
type schemaRegistryMigrator struct {
	conf    SchemaRegistryMigratorConfig
	src     *sr.Client
	srcURL  string
	dst     *sr.Client
	dstURL  string
	metrics *schemaRegistryMetrics
	log     *service.Logger

	mu            sync.RWMutex
	knownSubjects map[schemaInfo]struct{} // source schema info set
	knownSchemas  map[int]schemaInfo      // source schema ID -> destination schema info
}

// ListSubjectSchemas returns a list of all source subject schemas Filtered by
// the migrator configuration and sorted by the source schema ID.
func (m *schemaRegistryMigrator) ListSubjectSchemas(ctx context.Context) ([]sr.SubjectSchema, error) {
	if m.src == nil {
		return nil, errors.New("source schema registry client not configured")
	}
	return m.listSubjectSchemas(ctx, m.src)
}

func (m *schemaRegistryMigrator) listSubjectSchemas(ctx context.Context, client *sr.Client) ([]sr.SubjectSchema, error) {
	if m.conf.IncludeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	res, err := m.listSubjectSchemasBulk(ctx, client)
	if err != nil {
		m.log.Debugf("Schema migration: AllSchemas not available (%v), falling back to per-subject fetch", err)
		res, err = m.listSubjectSchemasIterative(ctx, client)
		if err != nil {
			return nil, err
		}
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].ID < res[j].ID
	})

	return res, nil
}

func (m *schemaRegistryMigrator) listSubjectSchemasBulk(ctx context.Context, client *sr.Client) ([]sr.SubjectSchema, error) {
	if m.conf.Versions == VersionsLatest {
		ctx = sr.WithParams(ctx, sr.LatestOnly)
	}

	m.log.Info("Schema migration: fetching all schemas from source registry")
	all, err := client.AllSchemas(ctx)
	if err != nil {
		return nil, err
	}
	m.log.Infof("Schema migration: fetched %d schemas from source registry", len(all))

	var res []sr.SubjectSchema
	for _, s := range all {
		if m.conf.Matches(s.Subject) {
			res = append(res, s)
		}
	}
	m.log.Infof("Schema migration: %d schemas after filtering", len(res))

	return res, nil
}

func (m *schemaRegistryMigrator) listSubjectSchemasIterative(ctx context.Context, client *sr.Client) ([]sr.SubjectSchema, error) {
	subs, err := client.Subjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list subjects: %w", err)
	}
	subs = m.conf.Filtered(subs)
	m.log.Infof("Schema migration: found %d subjects after filtering", len(subs))

	var res []sr.SubjectSchema
	switch m.conf.Versions {
	case VersionsLatest:
		const latestVersion = -1
		for i, s := range subs {
			schema, err := client.SchemaByVersion(ctx, s, latestVersion)
			if err != nil {
				return nil, fmt.Errorf("get latest schema for subject %q: %w", s, err)
			}
			res = append(res, schema)
			if (i+1)%progressLogInterval == 0 {
				m.log.Infof("Schema migration: fetched %d/%d subjects", i+1, len(subs))
			}
		}
	case VersionsAll:
		for i, s := range subs {
			vers, err := client.SubjectVersions(ctx, s)
			if err != nil {
				return nil, fmt.Errorf("get versions for subject %q: %w", s, err)
			}
			for _, v := range vers {
				schema, err := client.SchemaByVersion(ctx, s, v)
				if err != nil {
					return nil, fmt.Errorf("get schema for subject %q version %d: %w", s, v, err)
				}
				res = append(res, schema)
			}
			if (i+1)%progressLogInterval == 0 {
				m.log.Infof("Schema migration: fetched %d/%d subjects", i+1, len(subs))
			}
		}
	default:
		return nil, fmt.Errorf("unsupported versions mode: %q", m.conf.Versions)
	}
	m.log.Infof("Schema migration: fetched %d schemas total", len(res))

	return res, nil
}

// SyncLoop runs the schema registry sync in a loop at the configured interval
// until ctx is done. If interval is <= 0, the loop is not started.
func (m *schemaRegistryMigrator) SyncLoop(ctx context.Context) {
	if !m.enabled() {
		m.log.Info("Schema migration: schema registry sync disabled")
		return
	}
	if m.conf.Interval <= 0 {
		m.log.Info("Schema migration: schema registry sync disabled (interval <= 0)")
		return
	}

	m.log.Infof("Schema migration: starting schema registry sync loop every %s", m.conf.Interval)

	t := time.NewTicker(m.conf.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			m.log.Infof("Schema migration: stopping schema registry sync loop")
			return
		case <-t.C:
			if err := m.Sync(ctx); err != nil {
				m.log.Errorf("Schema migration: sync error: %v", err)
			}
		}
	}
}

// Sync syncs the source schema registry with the destination schema registry.
// It uses a streaming approach - fetching and syncing schemas in batches rather
// than loading all schemas into memory at once. This provides real-time progress
// and reduces memory usage for large migrations.
//
// For serverless schema registries, it automatically handles IMPORT mode by
// temporarily switching subject to IMPORT mode and restoring the original mode
// after migration completes.
func (m *schemaRegistryMigrator) Sync(ctx context.Context) error {
	if !m.enabled() {
		m.log.Info("Schema migration: schema registry sync disabled")
		return nil
	}

	m.log.Info("Schema migration: syncing schema registry")

	if err := m.validateSchemaRegistries(ctx); err != nil {
		return err
	}

	subjects, err := m.listSubjects(ctx)
	if err != nil {
		return fmt.Errorf("list subjects: %w", err)
	}

	if len(subjects) == 0 {
		m.log.Info("Schema migration: no subjects to sync")
		return nil
	}

	// Sample first batch to detect references - if found, sort all subjects by ID
	hasReferences := false
	sampleSize := min(m.conf.BatchSize, len(subjects))
	sampleSchemas, err := m.fetchSchemasForSubjects(ctx, subjects[:sampleSize])
	if err != nil {
		return fmt.Errorf("fetch sample schemas: %w", err)
	}
	for _, s := range sampleSchemas {
		if len(s.References) > 0 {
			hasReferences = true
			break
		}
	}

	if hasReferences {
		m.log.Info("Schema migration: references detected, sorting subjects by schema ID for ordering")
		subjects, _, err = m.listSubjectsSortedByMinID(ctx)
		if err != nil {
			return fmt.Errorf("sort subjects by ID: %w", err)
		}
	}

	m.log.Infof("Schema migration: streaming %d subjects in batches of %d (workers=%d, has_references=%v)",
		len(subjects), m.conf.BatchSize, m.conf.Workers, hasReferences)

	var totalSynced, totalSkipped int
	for batchStart := 0; batchStart < len(subjects); batchStart += m.conf.BatchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batchEnd := min(batchStart+m.conf.BatchSize, len(subjects))
		batchSubjects := subjects[batchStart:batchEnd]

		schemas, err := m.fetchSchemasForSubjects(ctx, batchSubjects)
		if err != nil {
			return fmt.Errorf("fetch schemas for batch: %w", err)
		}

		var toSync []sr.SubjectSchema
		m.mu.RLock()
		for _, s := range schemas {
			srcInfo := schemaInfoFromSubjectSchema(s)
			if _, ok := m.knownSubjects[srcInfo]; !ok {
				toSync = append(toSync, s)
			} else {
				totalSkipped++
			}
		}
		m.mu.RUnlock()

		if len(toSync) == 0 {
			continue
		}

		synced, err := m.syncBatch(ctx, toSync, hasReferences)
		if err != nil {
			return err
		}
		totalSynced += synced

		m.log.Infof("Schema migration: progress %d/%d subjects, %d schemas synced, %d skipped (already exists)",
			batchEnd, len(subjects), totalSynced, totalSkipped)
	}

	m.log.Infof("Schema migration: completed - %d schemas synced, %d skipped", totalSynced, totalSkipped)
	return nil
}

func (m *schemaRegistryMigrator) listSubjects(ctx context.Context) ([]string, error) {
	if m.conf.IncludeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	subs, err := m.src.Subjects(ctx)
	if err != nil {
		return nil, err
	}

	return m.conf.Filtered(subs), nil
}

type subjectMinID struct {
	subject string
	minID   int
}

func (m *schemaRegistryMigrator) listSubjectsSortedByMinID(ctx context.Context) ([]string, bool, error) {
	if m.conf.IncludeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	subs, err := m.src.Subjects(ctx)
	if err != nil {
		return nil, false, err
	}
	subs = m.conf.Filtered(subs)

	if len(subs) == 0 {
		return nil, false, nil
	}

	m.log.Infof("Schema migration: fetching schema IDs for %d subjects to determine ordering", len(subs))

	var items []subjectMinID
	var hasReferences bool
	for i, s := range subs {
		schema, err := m.src.SchemaByVersion(ctx, s, -1) // latest version
		if err != nil {
			return nil, false, fmt.Errorf("get schema for subject %q: %w", s, err)
		}
		items = append(items, subjectMinID{subject: s, minID: schema.ID})
		if len(schema.References) > 0 {
			hasReferences = true
		}
		if (i+1)%progressLogInterval == 0 {
			m.log.Infof("Schema migration: fetched IDs for %d/%d subjects", i+1, len(subs))
		}
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].minID < items[j].minID
	})

	sorted := make([]string, len(items))
	for i, item := range items {
		sorted[i] = item.subject
	}

	return sorted, hasReferences, nil
}

func (m *schemaRegistryMigrator) fetchSchemasForSubjects(ctx context.Context, subjects []string) ([]sr.SubjectSchema, error) {
	if m.conf.IncludeDeleted {
		ctx = sr.WithParams(ctx, sr.ShowDeleted)
	}

	var res []sr.SubjectSchema
	switch m.conf.Versions {
	case VersionsLatest:
		const latestVersion = -1
		for _, s := range subjects {
			schema, err := m.src.SchemaByVersion(ctx, s, latestVersion)
			if err != nil {
				return nil, fmt.Errorf("get latest schema for subject %q: %w", s, err)
			}
			res = append(res, schema)
		}
	case VersionsAll:
		for _, s := range subjects {
			vers, err := m.src.SubjectVersions(ctx, s)
			if err != nil {
				return nil, fmt.Errorf("get versions for subject %q: %w", s, err)
			}
			for _, v := range vers {
				schema, err := m.src.SchemaByVersion(ctx, s, v)
				if err != nil {
					return nil, fmt.Errorf("get schema for subject %q version %d: %w", s, v, err)
				}
				res = append(res, schema)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported versions mode: %q", m.conf.Versions)
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].ID < res[j].ID
	})

	return res, nil
}

func (m *schemaRegistryMigrator) syncBatch(ctx context.Context, toSync []sr.SubjectSchema, forceSequential bool) (int, error) {
	if forceSequential || len(toSync) <= m.conf.Workers {
		return m.syncBatchSequential(ctx, toSync)
	}

	return m.syncBatchParallel(ctx, toSync)
}

func (m *schemaRegistryMigrator) syncBatchSequential(ctx context.Context, toSync []sr.SubjectSchema) (int, error) {
	var synced int
	for _, s := range toSync {
		select {
		case <-ctx.Done():
			return synced, ctx.Err()
		default:
		}

		info, err := m.syncSubjectSchema(ctx, s)
		if err != nil {
			return synced, fmt.Errorf("sync subject schema %s version %d: %w", s.Subject, s.Version, err)
		}

		m.mu.Lock()
		if existing, ok := m.knownSchemas[s.ID]; ok {
			if existing.ID != info.ID {
				m.mu.Unlock()
				return synced, fmt.Errorf("schema ID mapping conflict: source ID %d maps to both destination IDs %d and %d",
					s.ID, existing.ID, info.ID)
			}
		}
		srcInfo := schemaInfoFromSubjectSchema(s)
		m.knownSubjects[srcInfo] = struct{}{}
		m.knownSchemas[s.ID] = info
		m.mu.Unlock()

		if err := m.syncSubjectCompatibility(ctx, s.Subject); err != nil {
			return synced, fmt.Errorf("sync subject compatibility %s: %w", s.Subject, err)
		}

		synced++
	}

	return synced, nil
}

func (m *schemaRegistryMigrator) syncBatchParallel(ctx context.Context, toSync []sr.SubjectSchema) (int, error) {
	type syncResult struct {
		src  sr.SubjectSchema
		info schemaInfo
		err  error
	}

	work := make(chan sr.SubjectSchema, len(toSync))
	results := make(chan syncResult, len(toSync))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < m.conf.Workers; i++ {
		wg.Go(func() {
			for s := range work {
				select {
				case <-ctx.Done():
					return
				default:
				}
				info, err := m.syncSubjectSchema(ctx, s)
				if err == nil {
					err = m.syncSubjectCompatibility(ctx, s.Subject)
				}
				results <- syncResult{src: s, info: info, err: err}
			}
		})
	}

	for _, s := range toSync {
		work <- s
	}
	close(work)

	go func() {
		wg.Wait()
		close(results)
	}()

	var synced int
	for res := range results {
		if res.err != nil {
			cancel()
			return synced, fmt.Errorf("sync subject schema %s version %d: %w", res.src.Subject, res.src.Version, res.err)
		}

		m.mu.Lock()
		if existing, ok := m.knownSchemas[res.src.ID]; ok {
			if existing.ID != res.info.ID {
				m.mu.Unlock()
				cancel()
				return synced, fmt.Errorf("schema ID mapping conflict: source ID %d maps to both destination IDs %d and %d",
					res.src.ID, existing.ID, res.info.ID)
			}
		}
		srcInfo := schemaInfoFromSubjectSchema(res.src)
		m.knownSubjects[srcInfo] = struct{}{}
		m.knownSchemas[res.src.ID] = res.info
		m.mu.Unlock()
		synced++
	}

	return synced, nil
}

func (m *schemaRegistryMigrator) enabled() bool {
	return m.conf.Enabled && (m.src != nil || m.dst != nil)
}

func (m *schemaRegistryMigrator) validateSchemaRegistries(ctx context.Context) error {
	if m.src == nil {
		return errors.New("source schema registry client not configured")
	}
	if m.dst == nil {
		return errors.New("destination schema registry client not configured")
	}
	if m.srcURL == m.dstURL {
		return fmt.Errorf("source and destination schema registry URLs must be different: %s", m.srcURL)
	}
	mode, err := srGlobalMode(ctx, m.dst)
	if err != nil {
		return err
	}
	m.log.Debugf("Schema migration: destination schema registry mode=%s", mode)
	if mode != sr.ModeReadWrite && mode != sr.ModeImport {
		return fmt.Errorf("schema registry instance mode must be READWRITE or IMPORT, got %q", mode)
	}

	return nil
}

func (m *schemaRegistryMigrator) resolveSubject(subject string, version int) (string, error) {
	if m.conf.NameResolver == nil {
		return subject, nil
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("schema_registry_subject", subject)
	msg.MetaSetMut("schema_registry_version", strconv.Itoa(version))

	dstSubject, err := m.conf.NameResolver.TryString(msg)
	if err != nil {
		return "", fmt.Errorf("resolve destination subject: %s", err)
	}
	if dstSubject == "" {
		return "", errors.New("resolved empty destination subject")
	}
	return dstSubject, nil
}

func (m *schemaRegistryMigrator) syncSubjectSchema(ctx context.Context, ss sr.SubjectSchema) (schemaInfo, error) {
	dstSubject, err := m.resolveSubject(ss.Subject, ss.Version)
	if err != nil {
		return schemaInfo{}, err
	}
	if dstSubject != ss.Subject {
		m.log.Debugf("Schema migration: resolved subject=%s version=%d => subject=%s",
			ss.Subject, ss.Version, dstSubject)
	}

	// Ensure subject is in IMPORT mode for serverless registries
	if m.conf.Serverless {
		restoreMode, err := m.ensureSubjectImportMode(ctx, dstSubject)
		if err != nil {
			return schemaInfo{}, fmt.Errorf("ensure IMPORT mode for subject %s: %w", dstSubject, err)
		}
		defer restoreMode()
	}

	if m.conf.Normalize {
		ctx = sr.WithParams(ctx, sr.Normalize)
	}

	sch := ss.Schema // shallow copy
	// In serverless, the schema registry does not store schema metadata
	if m.conf.Serverless {
		sch.SchemaMetadata = nil
		sch.SchemaRuleSet = nil
	}

	var info schemaInfo
	t0 := time.Now()
	if m.conf.TranslateIDs {
		// If the schema already exists (and is identical), this returns
		// the existing schema
		dss, err := m.dst.CreateSchema(ctx, dstSubject, sch)
		if err != nil {
			m.metrics.IncSchemaCreateErrors()
			return schemaInfo{}, fmt.Errorf("create schema: %w", err)
		}

		info = schemaInfoFromSubjectSchema(dss)
		m.log.Infof("Schema migration: schema created with translated id: subject=%s version=%d id=%d => subject=%s version=%d id=%d",
			ss.Subject, ss.Version, ss.ID, info.Subject, info.Version, info.ID)
	} else {
		dss, err := m.dst.CreateSchemaWithIDAndVersion(ctx, dstSubject, sch, ss.ID, ss.Version)
		if err != nil {
			const conflictPattern = `Schema already registered with id \d+ instead of input id \d+`
			if ok, _ := regexp.MatchString(conflictPattern, err.Error()); ok {
				return schemaInfo{}, fmt.Errorf("create schema: %w - try enabling translate-ids", err)
			}

			// This is a workaround for Allow POSTing the same schemas with
			// a fixed ID multiple times [1]. We manually check if the schema
			// already exists and if it is identical to the one we're trying to
			// create.
			//
			// [1] https://github.com/redpanda-data/redpanda/issues/26331
			if s, _ := m.dst.SchemaByID(sr.WithParams(ctx, sr.ShowDeleted), ss.ID); !schemaEquals(s, sch) {
				m.metrics.IncSchemaCreateErrors()
				return schemaInfo{}, fmt.Errorf("create schema: %w", err)
			}

			// If the schema already exists (and is identical), use the source
			// schema ID and version...
			m.log.Warnf("Schema migration: schema subject=%s version=%d id=%d could not be created (server error: %s) - using existing schema with the same ID, if this is not the desired behavior, try enabling translate-ids",
				ss.Subject, ss.Version, ss.ID, err.Error())

			dss = ss
			dss.Subject = dstSubject
		}

		info = schemaInfoFromSubjectSchema(dss)
		m.log.Infof("Schema migration: schema created with fixed id: subject=%s version=%d id=%d",
			info.Subject, info.Version, info.ID)
	}
	m.metrics.ObserveSchemaCreateLatency(time.Since(t0))
	m.metrics.IncSchemasCreated()

	return info, nil
}

func schemaEquals(a, b sr.Schema) bool {
	if a.Schema != b.Schema {
		if a.Type != b.Type {
			return false
		}
		if !schemaStringEquals(a.Schema, b.Schema, a.Type) {
			return false
		}
	}

	return cmp.Equal(a, b, cmpopts.IgnoreFields(sr.Schema{}, "Schema"))
}

// schemaStringEquals compares two schema strings for equality, ignoring
// newlines and leading/trailing spaces in the schemas.
//
// For JSON and Avro schemas, the function parses the schemas as JSON and
// compares the resulting maps. For Protobuf schemas, the function removes
// newlines and leading/trailing spaces from the schemas and compares the
// resulting strings.
func schemaStringEquals(a, b string, st sr.SchemaType) bool {
	switch st {
	case sr.TypeAvro, sr.TypeJSON:
		// Parse the schemas as JSON
		var as, bs map[string]any
		if err := json.Unmarshal([]byte(a), &as); err != nil {
			return false
		}
		if err := json.Unmarshal([]byte(b), &bs); err != nil {
			return false
		}
		if !cmp.Equal(as, bs) {
			return false
		}
	case sr.TypeProtobuf:
		// Remove newlines and leading/trailing spaces from the schemas
		as := strings.TrimSpace(strings.ReplaceAll(a, "\n", ""))
		bs := strings.TrimSpace(strings.ReplaceAll(b, "\n", ""))
		if as != bs {
			return false
		}
	default:
		return false
	}

	return true
}

func (m *schemaRegistryMigrator) syncSubjectCompatibility(ctx context.Context, subject string) error {
	var cl sr.CompatibilityLevel
	res := m.src.Compatibility(ctx, subject)
	if res[0].Err == nil && res[0].Level != 0 {
		cl = res[0].Level
	}
	if cl == 0 {
		m.log.Debugf("Schema migration: no explicit compatibility level to apply for subject=%s", subject)
		return nil
	}

	dstSubject, err := m.resolveSubject(subject, 0)
	if err != nil {
		return err
	}

	t0 := time.Now()
	set := m.dst.SetCompatibility(ctx, sr.SetCompatibility{Level: cl}, dstSubject)
	if set[0].Err != nil {
		m.metrics.IncCompatUpdateErrors()
		return fmt.Errorf("set destination subject compatibility for %q: %w", dstSubject, set[0].Err)
	}
	m.metrics.ObserveCompatUpdateLatency(time.Since(t0))
	m.metrics.IncCompatUpdates()

	m.log.Infof("Schema migration: set compatibility level=%s subject=%s", cl, dstSubject)

	return nil
}

var noMode sr.Mode = -1

func srGlobalMode(ctx context.Context, client *sr.Client) (sr.Mode, error) {
	res := client.Mode(ctx)
	if res[0].Err != nil {
		return noMode, fmt.Errorf("fetch schema registry mode: %w", res[0].Err)
	}
	return res[0].Mode, nil
}

// ensureSubjectImportMode checks if the destination subject is in IMPORT mode.
// If not in IMPORT mode, it switches to IMPORT mode and returns a function to
// restore the original mode.
func (m *schemaRegistryMigrator) ensureSubjectImportMode(ctx context.Context, subject string) (func(), error) {
	noop := func() {}

	// Check global mode first, if global mode is IMPORT, subject is implicitly IMPORT
	mode, err := srGlobalMode(ctx, m.dst)
	if err != nil {
		return noop, err
	}
	if mode == sr.ModeImport {
		return noop, nil
	}

	mode, err = srSubjectMode(ctx, m.dst, subject)
	if err != nil {
		if strings.Contains(err.Error(), "does not have subject-level mode configured") {
			mode = noMode
		} else {
			return noop, err
		}
	} else if mode == sr.ModeImport {
		return noop, nil
	}

	m.log.Infof("Schema migration: setting subject=%s mode to %s for migration", subject, sr.ModeImport)
	if err := srSetSubjectMode(ctx, m.dst, sr.ModeImport, subject); err != nil {
		if strings.Contains(err.Error(), "Invalid mode. Valid values are") {
			m.log.Warnf("Schema migration: destination schema registry does not support IMPORT mode for subject=%s, proceeding without mode change", subject)
			return noop, nil
		}
		return noop, fmt.Errorf("failed to set IMPORT mode: %w", err)
	}

	return func() {
		if mode == noMode {
			m.log.Infof("Schema migration: resetting subject=%s mode", subject)
		} else {
			m.log.Infof("Schema migration: restoring subject=%s mode to %s", subject, mode)
		}

		if err := srSetSubjectMode(context.Background(), m.dst, mode, subject); err != nil {
			m.log.Errorf("Schema migration: failed to restore subject=%s: %v", subject, err)
		}
	}, nil
}

func srSubjectMode(ctx context.Context, client *sr.Client, subject string) (sr.Mode, error) {
	res := client.Mode(ctx, subject)
	if res[0].Err != nil {
		return 0, fmt.Errorf("fetch subject mode: %w", res[0].Err)
	}
	return res[0].Mode, nil
}

func srSetSubjectMode(ctx context.Context, client *sr.Client, mode sr.Mode, subject string) error {
	if mode == noMode {
		res := client.ResetMode(ctx, subject)
		if res[0].Err != nil {
			return fmt.Errorf("reset subject mode: %w", res[0].Err)
		}
	} else {
		res := client.SetMode(ctx, mode, subject)
		if res[0].Err != nil {
			return fmt.Errorf("set subject mode to %s: %w", mode, res[0].Err)
		}
	}
	return nil
}

// DestinationSchemaID attempts to fetch the destination schema ID for the
// provided source schema ID.
func (m *schemaRegistryMigrator) DestinationSchemaID(schemaID int) (int, error) {
	if !m.enabled() {
		return schemaID, nil
	}

	// Try reading from cache
	m.mu.RLock()
	info, ok := m.knownSchemas[schemaID]
	m.mu.RUnlock()
	if ok {
		return info.ID, nil
	}

	// Schema not found in cache
	if m.conf.Strict {
		return 0, fmt.Errorf("schema ID %d not found in registry", schemaID)
	}

	return schemaID, nil
}

type schemaRegistryMetrics struct {
	schemasCreated      *service.MetricCounter
	schemaCreateErrors  *service.MetricCounter
	schemaCreateLatency *service.MetricTimer
	compatUpdates       *service.MetricCounter
	compatUpdateErrors  *service.MetricCounter
	compatUpdateLatency *service.MetricTimer
}

func newSchemaRegistryMetrics(m *service.Metrics) *schemaRegistryMetrics {
	return &schemaRegistryMetrics{
		schemasCreated:      m.NewCounter("redpanda_migrator_sr_schemas_created_total"),
		schemaCreateErrors:  m.NewCounter("redpanda_migrator_sr_schema_create_errors_total"),
		schemaCreateLatency: m.NewTimer("redpanda_migrator_sr_schema_create_latency_ns"),
		compatUpdates:       m.NewCounter("redpanda_migrator_sr_compatibility_updates_total"),
		compatUpdateErrors:  m.NewCounter("redpanda_migrator_sr_compatibility_update_errors_total"),
		compatUpdateLatency: m.NewTimer("redpanda_migrator_sr_compatibility_update_latency_ns"),
	}
}

func (sm *schemaRegistryMetrics) IncSchemasCreated() {
	if sm == nil {
		return
	}
	sm.schemasCreated.Incr(1)
}

func (sm *schemaRegistryMetrics) IncSchemaCreateErrors() {
	if sm == nil {
		return
	}
	sm.schemaCreateErrors.Incr(1)
}

func (sm *schemaRegistryMetrics) ObserveSchemaCreateLatency(d time.Duration) {
	if sm == nil {
		return
	}
	sm.schemaCreateLatency.Timing(d.Nanoseconds())
}

func (sm *schemaRegistryMetrics) IncCompatUpdates() {
	if sm == nil {
		return
	}
	sm.compatUpdates.Incr(1)
}

func (sm *schemaRegistryMetrics) IncCompatUpdateErrors() {
	if sm == nil {
		return
	}
	sm.compatUpdateErrors.Incr(1)
}

func (sm *schemaRegistryMetrics) ObserveCompatUpdateLatency(d time.Duration) {
	if sm == nil {
		return
	}
	sm.compatUpdateLatency.Timing(d.Nanoseconds())
}
