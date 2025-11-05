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
	srFieldURL = "url"
	srFieldTLS = "tls"

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
)

func schemaRegistryField(extraFields ...*service.ConfigField) *service.ConfigField {
	fields := append(
		[]*service.ConfigField{
			service.NewStringField(srFieldURL).
				Description("The base URL of the schema registry service. Required for schema migration functionality.").
				Example("http://localhost:8081").
				Example("https://schema-registry.example.com:8081"),
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
			Description("Regular expressions for schema subjects to include in migration. If empty, all subjects are included (unless excluded).").
			Example(`["prod-.*", "staging-.*"]`).
			Example(`["user-.*", "order-.*"]`).
			Optional(),
		service.NewStringListField(srFieldExclude).
			Description("Regular expressions for schema subjects to exclude from migration. Takes precedence over include patterns.").
			Example(`[".*-test", ".*-temp"]`).
			Example(`["dev-.*", "local-.*"]`).
			Optional(),
		service.NewInterpolatedStringField(srFieldSubject).
			Description("Template for transforming subject names during migration. Use interpolation to rename subjects systematically.").
			Example(`"prod_${! metadata('schema_registry_subject') }"`).
			Example(`"${! metadata('schema_registry_subject') | replace('dev_', 'prod_') }"`).
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
	}
}

func schemaRegistryClientFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*sr.Client, error) {
	if !pConf.Contains("schema_registry") {
		return nil, nil
	}
	pConf = pConf.Namespace(srObjectField)

	// If the enabled flag exists and is set to false, short-circuit without creating a client.
	if pConf.Contains(srFieldEnabled) {
		enabled, err := pConf.FieldBool(srFieldEnabled)
		if err != nil {
			return nil, err
		}
		if !enabled {
			return nil, nil
		}
	}

	srURL, err := pConf.FieldURL(srFieldURL)
	if err != nil {
		return nil, err
	}

	reqSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(srFieldTLS)
	if err != nil {
		return nil, err
	}
	if !tlsEnabled {
		tlsConf = nil
	}

	opts := []sr.ClientOpt{sr.URLs(srURL.String())}
	if tlsConf != nil {
		opts = append(opts, sr.DialTLSConfig(tlsConf))
	}
	if reqSigner != nil {
		opts = append(opts, sr.PreReq(func(req *http.Request) error { return reqSigner(mgr.FS(), req) }))
	}
	return sr.NewClient(opts...)
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
	dst     *sr.Client
	metrics *schemaRegistryMetrics
	log     *service.Logger

	mu           sync.RWMutex
	knownSchemas map[int]schemaInfo  // source schema ID -> destination schema info
	compatSet    map[string]struct{} // subject -> struct{}
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

	// List and filter subjects
	subs, err := client.Subjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("list subjects: %w", err)
	}
	subs = m.conf.Filtered(subs)

	// Get subject schemas
	var res []sr.SubjectSchema
	switch m.conf.Versions {
	case VersionsLatest:
		const latestVersion = -1
		for _, s := range subs {
			schema, err := client.SchemaByVersion(ctx, s, latestVersion)
			if err != nil {
				return nil, fmt.Errorf("get latest schema for subject %q: %w", s, err)
			}
			res = append(res, schema)
		}
	case VersionsAll:
		for _, s := range subs {
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
		}
	default:
		return nil, fmt.Errorf("unsupported versions mode: %q", m.conf.Versions)
	}

	// Sort by schema ID ascending
	sort.Slice(res, func(i, j int) bool {
		return res[i].ID < res[j].ID
	})

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
// It lists all subject schemas in the source schema registry, filters them by
// the migrator configuration, and then syncs each subject schema and its
// compatibility mode.
func (m *schemaRegistryMigrator) Sync(ctx context.Context) error {
	if !m.enabled() {
		m.log.Info("Schema migration: schema registry sync disabled")
		return nil
	}

	m.log.Info("Schema migration: syncing schema registry")

	if err := m.validateSchemaRegistries(ctx); err != nil {
		return err
	}

	all, err := m.listSubjectSchemas(ctx, m.src)
	if err != nil {
		return fmt.Errorf("list subject schemas: %w", err)
	}
	m.log.Debugf("Schema migration: found %d subject schemas", len(all))

	for _, s := range all {
		if err := m.syncSubjectSchemaIfNeeded(ctx, s); err != nil {
			return fmt.Errorf("sync subject schema %s version %d: %w", s.Subject, s.Version, err)
		}
		if err := m.syncSubjectCompatibilityIfNeeded(ctx, s.Subject); err != nil {
			return fmt.Errorf("sync subject compatibility %s: %w", s.Subject, err)
		}
	}

	return nil
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
	mode, err := srGlobalMode(ctx, m.dst)
	if err != nil {
		return err
	}
	m.log.Debugf("Schema migration: destination schema registry mode=%s", mode)
	if mode != "READWRITE" && mode != "IMPORT" {
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

func (m *schemaRegistryMigrator) syncSubjectSchemaIfNeeded(ctx context.Context, ss sr.SubjectSchema) error {
	m.mu.RLock()
	_, ok := m.knownSchemas[ss.ID]
	m.mu.RUnlock()
	if ok {
		m.log.Debugf("Schema migration: schema already synced, skipping: subject=%s version=%d id=%d",
			ss.Subject, ss.Version, ss.ID)
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncSubjectSchemaLocked(ctx, ss)
}

func (m *schemaRegistryMigrator) syncSubjectSchemaLocked(ctx context.Context, ss sr.SubjectSchema) error {
	if _, ok := m.knownSchemas[ss.ID]; ok {
		m.log.Debugf("Schema already synced (locked), skipping: subject=%s version=%d id=%d",
			ss.Subject, ss.Version, ss.ID)
		return nil
	}

	m.log.Debugf("Schema migration: subject=%s version=%d id=%d",
		ss.Subject, ss.Version, ss.ID)

	dstSubject, err := m.resolveSubject(ss.Subject, ss.Version)
	if err != nil {
		return err
	}
	if dstSubject != ss.Subject {
		m.log.Debugf("Schema migration: resolved subject=%s version=%d to %s",
			ss.Subject, ss.Version, dstSubject)
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
			return fmt.Errorf("create schema: %w", err)
		}

		info = schemaInfoFromSubjectSchema(dss)
		m.log.Infof("Schema migration: schema created with translated id: subject=%s version=%d id=%d as subject=%s version=%d id=%d",
			ss.Subject, ss.Version, ss.ID, info.Subject, info.Version, info.ID)
	} else {
		dss, err := m.dst.CreateSchemaWithIDAndVersion(ctx, dstSubject, sch, ss.ID, ss.Version)
		if err != nil {
			const conflictPattern = `Schema already registered with id \d+ instead of input id \d+`
			if ok, _ := regexp.MatchString(conflictPattern, err.Error()); ok {
				return fmt.Errorf("create schema: %w - try enabling translate-ids", err)
			}

			// This is a workaround for Allow POSTing the same schemas with
			// a fixed ID multiple times [1]. We manually check if the schema
			// already exists and if it is identical to the one we're trying to
			// create.
			//
			// [1] https://github.com/redpanda-data/redpanda/issues/26331
			if s, _ := m.dst.SchemaByID(sr.WithParams(ctx, sr.ShowDeleted), ss.ID); !schemaEquals(s, sch) {
				m.metrics.IncSchemaCreateErrors()
				return fmt.Errorf("create schema: %w", err)
			}

			// If the schema already exists (and is identical), use the source
			// schema ID and version...
			dss = ss
			dss.Subject = dstSubject
		}

		info = schemaInfoFromSubjectSchema(dss)
		m.log.Infof("Schema migration: schema created with fixed id: subject=%s id=%d version=%d",
			info.Subject, info.Version, info.ID)
	}
	m.metrics.ObserveSchemaCreateLatency(time.Since(t0))
	m.metrics.IncSchemasCreated()

	m.knownSchemas[ss.ID] = info

	return nil
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

func (m *schemaRegistryMigrator) syncSubjectCompatibilityIfNeeded(ctx context.Context, subject string) error {
	m.mu.RLock()
	_, ok := m.compatSet[subject]
	m.mu.RUnlock()
	if ok {
		m.log.Debugf("Schema migration: compatibility already set, skipping: subject=%s", subject)
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncSubjectCompatibilityLocked(ctx, subject)
}

func (m *schemaRegistryMigrator) syncSubjectCompatibilityLocked(ctx context.Context, subject string) error {
	if _, ok := m.compatSet[subject]; ok {
		m.log.Debugf("Schema migration: compatibility already set (locked), skipping: subject=%s", subject)
		return nil
	}

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

	m.log.Infof("Schema migration: set compatibility level %s for subject %s", cl, dstSubject)
	m.compatSet[subject] = struct{}{}

	return nil
}

func srGlobalMode(ctx context.Context, client *sr.Client) (string, error) {
	res := client.Mode(ctx)
	if res[0].Err != nil {
		return "", fmt.Errorf("fetch schema registry mode: %w", res[0].Err)
	}
	return res[0].Mode.String(), nil
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
