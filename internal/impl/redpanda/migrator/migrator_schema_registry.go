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
	srFieldInterval           = "interval"
	srFieldInclude            = "include"
	srFieldExclude            = "exclude"
	srFieldSubject            = "subject"
	srFieldCompatibilityLevel = "compatibility_level"
	srFieldVersions           = "versions"
	srFieldIncludeDeleted     = "include_deleted"
	srFieldTranslateIDs       = "translate_ids"
	srFieldNormalize          = "normalize"
)

func schemaRegistryField(extraFields ...*service.ConfigField) *service.ConfigField {
	fields := append(
		[]*service.ConfigField{
			service.NewStringField(srFieldURL).Description("The base URL of the schema registry service."),
			service.NewTLSToggledField(srFieldTLS),
		},
		service.NewHTTPRequestAuthSignerFields()...)
	fields = append(fields, extraFields...)

	return service.NewObjectField(srObjectField, fields...)
}

func schemaRegistryMigratorFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewDurationField(srFieldInterval).
			Description("How often to synchronise schema registry subjects. Set to 0s to perform a one-time sync.").
			Default("0s"),
		service.NewStringField(srFieldInclude).
			Description("Regular expression for subjects to include in migration.").
			Optional(),
		service.NewStringField(srFieldExclude).
			Description("Regular expression for subjects to exclude from migration.").
			Optional(),
		service.NewInterpolatedStringField(srFieldSubject).
			Description("Expression to rename subjects (e.g., 'dev_(.*)' -> 'prod_$1').").
			Optional(),
		service.NewInterpolatedStringField(srFieldCompatibilityLevel).
			Description("The compatibility level to set for subjects being migrated. Can be one of BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE.").
			Optional(),
		service.NewStringEnumField(srFieldVersions, VersionsLatest.String(), VersionsAll.String()).
			Description("Which schema versions to migrate. 'latest' only migrates the latest version, 'all' migrates all historical versions.").
			Default(VersionsAll.String()),
		service.NewBoolField(srFieldIncludeDeleted).
			Description("Whether to copy soft-deleted schemas during migration.").
			Default(false),
		service.NewBoolField(srFieldTranslateIDs).
			Description("Whether to translate schema IDs during migration.").
			Default(false),
		service.NewBoolField(srFieldNormalize).
			Description("Whether to normalize schemas when creating them in the destination registry.").
			Default(false),
	}
}

func schemaRegistryClientFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*sr.Client, error) {
	if !pConf.Contains("schema_registry") {
		return nil, nil
	}

	srURL, err := pConf.FieldURL(srObjectField, srFieldURL)
	if err != nil {
		return nil, err
	}

	// TODO(mmt): this does not work with objects
	reqSigner, err := pConf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := pConf.FieldTLSToggled(srObjectField, srFieldTLS)
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
	// Interval controls how often to synchronise schemas. Zero means one-shot.
	Interval time.Duration
	// Include filters subjects to include by regex. Nil matches all subjects.
	Include *regexp.Regexp
	// Exclude filters subjects to exclude by regex. Nil disables exclusion.
	Exclude *regexp.Regexp
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
	// Serverless narrows the set of schema configuration keys to those
	// supported by serverless clusters.
	Serverless bool
}

// initFromParsed initializes the schema registry migrator with configuration from parsed config.
func (m *SchemaRegistryMigratorConfig) initFromParsed(pConf *service.ParsedConfig) error {
	var err error

	// Parse interval
	if m.Interval, err = pConf.FieldDuration(srObjectField, srFieldInterval); err != nil {
		return fmt.Errorf("parse interval: %w", err)
	}

	// Parse include regex
	if pConf.Contains(srObjectField, srFieldInclude) {
		includeStr, err := pConf.FieldString(srObjectField, srFieldInclude)
		if err != nil {
			return fmt.Errorf("parse include pattern: %w", err)
		}
		if includeStr != "" {
			m.Include, err = regexp.Compile(includeStr)
			if err != nil {
				return fmt.Errorf("invalid include regex pattern: %w", err)
			}
		}
	}

	// Parse exclude regex
	if pConf.Contains(srObjectField, srFieldExclude) {
		excludeStr, err := pConf.FieldString(srObjectField, srFieldExclude)
		if err != nil {
			return fmt.Errorf("parse exclude pattern: %w", err)
		}
		if excludeStr != "" {
			m.Exclude, err = regexp.Compile(excludeStr)
			if err != nil {
				return fmt.Errorf("invalid exclude regex pattern: %w", err)
			}
		}
	}

	// Parse subject transform
	if pConf.Contains(srObjectField, srFieldSubject) {
		if m.NameResolver, err = pConf.FieldInterpolatedString(srObjectField, srFieldSubject); err != nil {
			return fmt.Errorf("parse subject transform: %w", err)
		}
	}

	// Parse compatibility level
	if pConf.Contains(srObjectField, srFieldCompatibilityLevel) {
		if m.CompatibilityLevel, err = pConf.FieldInterpolatedString(srObjectField, srFieldCompatibilityLevel); err != nil {
			return fmt.Errorf("parse subject compatibility level: %w", err)
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

	// Use serverless from migrator config
	m.Serverless, err = pConf.FieldBool(rmoFieldServerless)
	if err != nil {
		return fmt.Errorf("get serverless field: %w", err)
	}

	return nil
}

// filteredSubjects returns a list of subjects filtered by the migrator
// include and exclude patterns.
func (m *SchemaRegistryMigratorConfig) filteredSubjects(subs []string) []string {
	if m.Include == nil && m.Exclude == nil {
		return subs
	}

	filtered := make([]string, 0, len(subs))
	for _, s := range subs {
		if m.Include != nil && !m.Include.MatchString(s) {
			continue
		}
		if m.Exclude != nil && m.Exclude.MatchString(s) {
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
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

type schemaRegistryMigrator struct {
	conf SchemaRegistryMigratorConfig
	src  *sr.Client
	dst  *sr.Client
	log  *service.Logger

	mu           sync.RWMutex
	knownSchemas map[int]schemaInfo  // source schema ID -> destination schema info
	compatSet    map[string]struct{} // subject -> struct{}
}

// ListSubjectSchemas returns a list of all source subject schemas filtered by
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
	subs = m.conf.filteredSubjects(subs)

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

func (m *schemaRegistryMigrator) Sync(ctx context.Context) error {
	m.log.Info("Syncing schema registry")

	if err := m.validateSchemaRegistries(ctx); err != nil {
		return err
	}

	all, err := m.listSubjectSchemas(ctx, m.src)
	if err != nil {
		return fmt.Errorf("list subject schemas: %w", err)
	}
	m.log.Debugf("Found %d subject schemas", len(all))

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
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncSubjectSchemaLocked(ctx, ss)
}

func (m *schemaRegistryMigrator) syncSubjectSchemaLocked(ctx context.Context, ss sr.SubjectSchema) error {
	if _, ok := m.knownSchemas[ss.ID]; ok {
		return nil
	}

	dstSubject, err := m.resolveSubject(ss.Subject, ss.Version)
	if err != nil {
		return err
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
	if m.conf.TranslateIDs {
		// If the schema already exists (and is identical), this returns
		// the existing schema
		dss, err := m.dst.CreateSchema(ctx, dstSubject, sch)
		if err != nil {
			return fmt.Errorf("create schema: %w", err)
		}
		info = schemaInfoFromSubjectSchema(dss)
	} else {
		dss, err := m.dst.CreateSchemaWithIDAndVersion(ctx, dstSubject, sch, ss.ID, ss.Version)
		if err != nil {
			// This is a workaround for Allow POSTing the same schemas with
			// a fixed ID multiple times [1]. We manually check if the schema
			// already exists and if it is identical to the one we're trying to
			// create.
			//
			// [1] https://github.com/redpanda-data/redpanda/issues/26331
			if s, _ := m.dst.SchemaByID(sr.WithParams(ctx, sr.ShowDeleted), ss.ID); !schemaEquals(s, sch) {
				return fmt.Errorf("create schema: %w", err)
			}

			// If the schema already exists (and is identical), use the source
			// schema ID and version...
			dss = ss
			dss.Subject = dstSubject
		}
		info = schemaInfoFromSubjectSchema(dss)
	}
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
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.syncSubjectCompatibilityLocked(ctx, subject)
}

func (m *schemaRegistryMigrator) syncSubjectCompatibilityLocked(ctx context.Context, subject string) error {
	if _, ok := m.compatSet[subject]; ok {
		return nil
	}

	dstSubject, err := m.resolveSubject(subject, 0)
	if err != nil {
		return err
	}

	var cl sr.CompatibilityLevel
	res := m.src.Compatibility(ctx, subject)
	if res[0].Err == nil && res[0].Level != 0 {
		cl = res[0].Level
	}
	if cl == 0 {
		return nil
	}

	set := m.dst.SetCompatibility(ctx, sr.SetCompatibility{Level: cl}, dstSubject)
	if set[0].Err != nil {
		return fmt.Errorf("set destination subject compatibility for %q: %w", dstSubject, set[0].Err)
	}
	m.compatSet[dstSubject] = struct{}{}

	return nil
}

func srGlobalMode(ctx context.Context, client *sr.Client) (string, error) {
	res := client.Mode(ctx)
	if res[0].Err != nil {
		return "", fmt.Errorf("fetch schema registry mode: %w", res[0].Err)
	}
	return res[0].Mode.String(), nil
}
