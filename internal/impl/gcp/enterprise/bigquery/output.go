// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	bqwaFieldProject                = "project"
	bqwaFieldDataset                = "dataset"
	bqwaFieldTable                  = "table"
	bqwaFieldMessageFormat          = "message_format"
	bqwaFieldBatching               = "batching"
	bqwaFieldCredentialsJSON        = "credentials_json"
	bqwaFieldTargetPrincipal        = "target_principal"
	bqwaFieldDelegates              = "delegates"
	bqwaFieldStreamIdleTimeout      = "stream_idle_timeout"
	bqwaFieldStreamSweepInterval    = "stream_sweep_interval"
	bqwaFieldMaxCachedStreams       = "max_cached_streams"
	bqwaFieldWriteMode              = "write_mode"
	bqwaFieldChangeType             = "change_type"
	bqwaFieldChangeSequenceNumber   = "change_sequence_number"
	bqwaFieldPrimaryKeys            = "primary_keys"
	bqwaFieldAutoCreateTable        = "auto_create_table"
	bqwaFieldSchema                 = "schema"
	bqwaSchemaFieldName             = "name"
	bqwaSchemaFieldType             = "type"
	bqwaSchemaFieldMode             = "mode"
	bqwaSchemaFieldFields           = "fields"
	bqwaFieldTimePartitioning       = "time_partitioning"
	bqwaFieldClustering             = "clustering"
	bqwatpFieldType                 = "type"
	bqwatpFieldField                = "field"
	bqwatpFieldExpiration           = "expiration"
	bqwatpFieldRequireFilter        = "require_filter"
	bqwaFieldSchemaResolveTimeout   = "schema_resolve_timeout"
	bqwaFieldSchemaEvolutionTimeout = "schema_evolution_timeout"
	bqwaFieldEndpoint               = "endpoint"
	bqwaepFieldHTTP                 = "http"
	bqwaepFieldGRPC                 = "grpc"
)

func init() {
	service.MustRegisterBatchOutput("gcp_bigquery_write_api", bigQueryWriteAPISpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error,
		) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(bqwaFieldBatching); err != nil {
				return
			}
			out, err = bigQueryWriteAPIOutputFromConfig(conf, mgr)
			return
		})
}

func bigQueryWriteAPISpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.90.0").
		Categories("GCP", "Services").
		Summary("Streams data into BigQuery using the Storage Write API.").
		Description(`
Writes messages to a BigQuery table using the Storage Write API.
This provides higher throughput and lower latency than the legacy streaming API or load jobs.

Messages can be formatted as JSON (default) or raw protobuf bytes.
When using JSON format the component automatically fetches the table schema and converts each message to the corresponding proto representation.

WARNING: protojson encodes int64 and uint64 values as strings, bytes as base64-encoded strings, and timestamps as RFC 3339 strings.
JSON messages must follow these conventions (e.g. `+"`"+`"age": "30"`+"`"+`, `+"`"+`"data": "aGVsbG8="`+"`"+`, `+"`"+`"created_at": "2026-01-02T15:04:05Z"`+"`"+`); otherwise the write will fail with an unmarshalling error.

When batching is enabled the table name is resolved from the first message in each batch.
All messages in the same batch are written to that table.

The interpolated table name is sanitized for BigQuery: dots, hyphens, slashes and whitespace are replaced with underscores, non-ASCII-alphanumeric characters are stripped, leading digits are prefixed with `+"`_`"+`, and the result is truncated to 1024 characters.
A name that sanitizes to the empty string is rejected as a permanent error.

== Write modes

The `+"`write_mode`"+` field selects between two write paths:

- `+"`default_stream`"+` (default): the multiplexed default stream. Lowest latency, at-least-once semantics.
- `+"`pending_stream`"+`: a fresh pending stream is allocated per batch; rows are written with sequential offsets, the stream is finalized, then atomically committed. Provides exactly-once semantics within a single committed batch.

== Auto-create

When `+"`auto_create_table`"+` is true, the output creates missing tables on the fly using the configured `+"`schema`"+`, `+"`time_partitioning`"+`, and `+"`clustering`"+`. `+"`AlreadyExists`"+` errors from concurrent creators are treated as success. When the table name is interpolated, every auto-created table receives the same configuration.

== Exactly-once caveat

The exactly-once guarantee of `+"`pending_stream`"+` is "exactly-once within a stream". If a BatchCommitWriteStreams RPC succeeds but its response is lost to a network failure, benthos retries the batch through a new pending stream and the data lands twice. This is a fundamental limitation of the BigQuery Storage Write API exactly-once contract and applies to every implementation.

== CDC migration

When migrating from the load-jobs based `+"`gcp_bigquery`"+` output to CDC mode, see the xref:outputs/bigquery_cdc_migration.adoc[CDC migration guide].
`).
		Fields(
			service.NewStringField(bqwaFieldProject).
				Description("The GCP project ID."+
					" If empty, the project is auto-detected from the environment.").
				Default(""),
			service.NewStringField(bqwaFieldDataset).
				Description("The BigQuery dataset ID."),
			service.NewInterpolatedStringField(bqwaFieldTable).
				Description("The BigQuery table ID."+
					" Supports interpolation functions."+
					" When batching, resolved from the first message in each batch."),
			service.NewStringEnumField(bqwaFieldMessageFormat, "json", "protobuf").
				Description("The format of input messages."+
					" Use 'json' to have the component convert JSON to proto automatically."+
					" Use 'protobuf' to supply raw proto-encoded bytes.").
				Default("json"),
			service.NewStringEnumField(bqwaFieldWriteMode, "default_stream", "pending_stream", "upsert", "upsert_delete").
				Description("How the output writes to BigQuery."+
					" `default_stream` uses the multiplexed default stream (at-least-once, lowest latency)."+
					" `pending_stream` allocates a per-batch pending stream that commits atomically,"+
					" providing exactly-once semantics within a single committed batch."+
					" `upsert` writes UPSERT-only rows to a BigQuery CDC-enabled table; the target table must have a PRIMARY KEY."+
					" `upsert_delete` allows both UPSERT and DELETE rows. Both CDC modes use the default stream as required by BigQuery.").
				Default("default_stream").
				Advanced(),
			service.NewInterpolatedStringField(bqwaFieldChangeType).
				Description("Bloblang expression resolving to the `_CHANGE_TYPE` pseudo-column value for each row."+
					" Must resolve to `UPSERT` or `DELETE` (case-insensitive)."+
					" Required when `write_mode` is `upsert` or `upsert_delete`."+
					" Example: `${! metadata(\"operation\") }`.").
				Optional(),
			service.NewInterpolatedStringField(bqwaFieldChangeSequenceNumber).
				Description("Optional Bloblang expression resolving to the `_CHANGE_SEQUENCE_NUMBER` pseudo-column value."+
					" Format: 1 to 4 sections of 1 to 16 hexadecimal characters each, separated by `/`."+
					" Example: `${! metadata(\"scn\") }` or `${! \"0/0/0/0\" }`."+
					" When unset, BigQuery resolves ordering by arrival time.").
				Optional(),
			service.NewStringListField(bqwaFieldPrimaryKeys).
				Description("Optional list of primary-key column names."+
					" Required when `auto_create_table` is true and `write_mode` is `upsert` or `upsert_delete`."+
					" A pre-existing table must already declare its PRIMARY KEY — this field cannot add one;"+
					" when both are set they must match exactly (same columns, same order)."+
					" Up to 16 columns; composite keys are supported in the same order they are listed.").
				Optional(),
			service.NewBoolField(bqwaFieldAutoCreateTable).
				Description("If true and the target table does not exist, the output creates it using the configured `schema`, `time_partitioning`, and `clustering`."+
					" AlreadyExists errors from concurrent creators are treated as success."+
					" When the table name is interpolated, every auto-created table receives the same schema and partition/clustering settings.").
				Default(false).
				Advanced(),
			service.NewObjectListField(bqwaFieldSchema,
				service.NewStringField(bqwaSchemaFieldName).
					Description("Column name."),
				service.NewStringField(bqwaSchemaFieldType).
					Description("BigQuery column type (STRING, BYTES, INTEGER/INT64, FLOAT/FLOAT64, NUMERIC, BIGNUMERIC, BOOLEAN/BOOL, TIMESTAMP, DATE, TIME, DATETIME, GEOGRAPHY, JSON, RECORD)."),
				service.NewStringField(bqwaSchemaFieldMode).
					Description("Column mode: NULLABLE (default), REQUIRED, or REPEATED.").
					Default("NULLABLE"),
				service.NewAnyListField(bqwaSchemaFieldFields).
					Description("For RECORD columns, the list of nested fields. Same shape as the top-level schema list.").
					Optional(),
			).
				Description("Column definitions used by `auto_create_table`. Required when `auto_create_table` is true.").
				Default([]any{}).
				Advanced(),
			service.NewObjectField(bqwaFieldTimePartitioning,
				// `type` has no default — absence is the sentinel for "block
				// not configured". Benthos still fills in defaults for child
				// fields of an Optional ObjectField, so the parent Contains()
				// check is unreliable here.
				service.NewStringEnumField(bqwatpFieldType, "DAY", "HOUR", "MONTH", "YEAR").
					Description("Partitioning granularity.").
					Optional(),
				service.NewStringField(bqwatpFieldField).
					Description("Column to partition on. Must be of type DATE, TIMESTAMP, or DATETIME."+
						" If empty, the table uses ingestion-time partitioning (`_PARTITIONTIME`).").
					Default(""),
				service.NewDurationField(bqwatpFieldExpiration).
					Description("Optional partition expiration. Zero means no expiration.").
					Default("0s"),
				service.NewBoolField(bqwatpFieldRequireFilter).
					Description("If true, queries against the table must filter on the partition column.").
					Default(false),
			).
				Description("Optional time-partitioning settings applied during `auto_create_table`."+
					" Setting `type` is the trigger — when omitted, the block is treated as absent.").
				Advanced().
				Optional(),
			service.NewStringListField(bqwaFieldClustering).
				Description("Optional clustering columns (up to 4) applied during `auto_create_table`."+
					" All names must appear in `schema`.").
				Default([]any{}).
				Advanced(),
			service.NewOutputMaxInFlightField().Default(4),
			service.NewBatchPolicyField(bqwaFieldBatching),
			service.NewStringField(bqwaFieldCredentialsJSON).
				Description("An optional JSON string containing GCP credentials."+
					" If empty, credentials are loaded from the environment.").
				Secret().
				Default(""),
			service.NewStringField(bqwaFieldTargetPrincipal).
				Description("Service account email to impersonate."+
					" When set, the output obtains tokens acting as this service account."+
					" Requires the caller to have roles/iam.serviceAccountTokenCreator on the target.").
				Advanced().
				Default(""),
			service.NewStringListField(bqwaFieldDelegates).
				Description("Optional delegation chain for chained service account impersonation."+
					" Each service account must be granted roles/iam.serviceAccountTokenCreator on the next in the chain.").
				Advanced().
				Default([]any{}),
			service.NewDurationField(bqwaFieldStreamIdleTimeout).
				Description("How long a cached stream can remain unused before being closed."+
					" Relevant when the table field uses interpolation to route to many tables.").
				Advanced().
				Default("5m"),
			service.NewDurationField(bqwaFieldStreamSweepInterval).
				Description("How often to check for idle streams to close.").
				Advanced().
				Default("1m"),
			service.NewIntField(bqwaFieldMaxCachedStreams).
				Description("Soft cap on the number of cached streams."+
					" When the cache exceeds this size, the least-recently-used stream is evicted."+
					" Set to 0 for unlimited (rely on idle-timeout sweeping only)."+
					" Relevant when the table field uses interpolation to route to many tables.").
				Advanced().
				Default(1024),
			service.NewDurationField(bqwaFieldSchemaResolveTimeout).
				Description("How long a single BigQuery table-metadata fetch can run before being aborted."+
					" Coalesced concurrent resolves share one fetch, so this bounds the time a wedged backend"+
					" can stall every batch routing to the same table. On the auto_create_table path"+
					" the budget covers Metadata→Create→Metadata, so it needs to absorb transient backend"+
					" slowness on top of the metadata fetch itself.").
				Advanced().
				Default("15s"),
			service.NewDurationField(bqwaFieldSchemaEvolutionTimeout).
				Description("Total time budget for a single schema evolution attempt (Metadata + Update"+
					" across all CAS retries on HTTP 412). Bounds how long the WriteBatch retry loop can be"+
					" starved by a wedged backend.").
				Advanced().
				Default("30s"),
			service.NewObjectField(bqwaFieldEndpoint,
				service.NewStringField(bqwaepFieldHTTP).
					Description("Override the BigQuery HTTP endpoint."+
						" Useful for local emulators.").
					Default(""),
				service.NewStringField(bqwaepFieldGRPC).
					Description("Override the BigQuery Storage gRPC endpoint."+
						" Useful for local emulators.").
					Default(""),
			).
				Description("Optional endpoint overrides for the BigQuery and Storage Write API clients.").
				Advanced(),
		)
}

// bqSchemaField mirrors the YAML representation of a single column in the
// schema config used by auto_create_table. It is converted to a
// bigquery.FieldSchema in table_creator.go.
type bqSchemaField struct {
	Name   string
	Type   string // canonical BigQuery type (aliases normalised)
	Mode   string // NULLABLE / REQUIRED / REPEATED
	Fields []bqSchemaField
}

// bqTimePartitioningConfig mirrors the YAML time_partitioning block. Type=""
// means the user did not configure partitioning at all (block absent).
type bqTimePartitioningConfig struct {
	Type          string
	Field         string
	Expiration    time.Duration
	RequireFilter bool
}

type bqWriteAPIConfig struct {
	ProjectID              string
	DatasetID              string
	MessageFormat          string
	WriteMode              string
	ChangeType             *service.InterpolatedString
	ChangeSequenceNumber   *service.InterpolatedString
	PrimaryKeys            []string
	AutoCreateTable        bool
	Schema                 []bqSchemaField
	TimePartitioning       bqTimePartitioningConfig
	Clustering             []string
	CredentialsJSON        string
	TargetPrincipal        string
	Delegates              []string
	StreamIdleTimeout      time.Duration
	StreamSweepInterval    time.Duration
	MaxCachedStreams       int
	SchemaResolveTimeout   time.Duration
	SchemaEvolutionTimeout time.Duration
	EndpointHTTP           string
	EndpointGRPC           string
}

// validBQFieldTypes is the set of column types accepted by auto-create. Aliases
// (INT64↔INTEGER, FLOAT64↔FLOAT, BOOL↔BOOLEAN) are normalised to BigQuery's
// canonical names so the parsed config is stable.
var validBQFieldTypes = map[string]string{
	"STRING":     "STRING",
	"BYTES":      "BYTES",
	"INTEGER":    "INTEGER",
	"INT64":      "INTEGER",
	"FLOAT":      "FLOAT",
	"FLOAT64":    "FLOAT",
	"NUMERIC":    "NUMERIC",
	"BIGNUMERIC": "BIGNUMERIC",
	"BOOLEAN":    "BOOLEAN",
	"BOOL":       "BOOLEAN",
	"TIMESTAMP":  "TIMESTAMP",
	"DATE":       "DATE",
	"TIME":       "TIME",
	"DATETIME":   "DATETIME",
	"GEOGRAPHY":  "GEOGRAPHY",
	"JSON":       "JSON",
	"RECORD":     "RECORD",
}

var validBQFieldModes = map[string]struct{}{
	"NULLABLE": {},
	"REQUIRED": {},
	"REPEATED": {},
}

// validateSchemaReferences checks that time_partitioning.field and clustering
// columns reference columns declared in the schema, and that the partition
// field has a partition-eligible type. The checks only fire when columns are
// actually configured (clustering empty / partition field empty is a no-op).
func validateSchemaReferences(conf bqWriteAPIConfig) error {
	if conf.TimePartitioning.Field == "" && len(conf.Clustering) == 0 {
		return nil
	}
	cols := make(map[string]string, len(conf.Schema))
	for _, f := range conf.Schema {
		cols[f.Name] = f.Type
	}
	if conf.TimePartitioning.Field != "" {
		t, ok := cols[conf.TimePartitioning.Field]
		if !ok {
			return fmt.Errorf("%s.%s %q is not in %s",
				bqwaFieldTimePartitioning, bqwatpFieldField, conf.TimePartitioning.Field, bqwaFieldSchema)
		}
		switch t {
		case "DATE", "TIMESTAMP", "DATETIME":
		default:
			return fmt.Errorf("%s.%s %q has type %s; must be DATE/TIMESTAMP/DATETIME",
				bqwaFieldTimePartitioning, bqwatpFieldField, conf.TimePartitioning.Field, t)
		}
	}
	for _, c := range conf.Clustering {
		if _, ok := cols[c]; !ok {
			return fmt.Errorf("%s column %q is not in %s", bqwaFieldClustering, c, bqwaFieldSchema)
		}
	}
	return nil
}

// parseSchemaFields walks a list of ParsedConfig children (top-level schema list
// or a RECORD's nested fields) into validated bqSchemaField values.
func parseSchemaFields(confs []*service.ParsedConfig) ([]bqSchemaField, error) {
	out := make([]bqSchemaField, 0, len(confs))
	for i, c := range confs {
		name, err := c.FieldString(bqwaSchemaFieldName)
		if err != nil {
			return nil, fmt.Errorf("schema[%d]: %w", i, err)
		}
		if name == "" {
			return nil, fmt.Errorf("schema[%d]: %s is required", i, bqwaSchemaFieldName)
		}
		rawType, err := c.FieldString(bqwaSchemaFieldType)
		if err != nil {
			return nil, fmt.Errorf("schema[%d] %q: %w", i, name, err)
		}
		canonical, ok := validBQFieldTypes[strings.ToUpper(rawType)]
		if !ok {
			return nil, fmt.Errorf("schema[%d] %q: unsupported type %q", i, name, rawType)
		}
		// FieldString returns an error for missing keys when the child config
		// came from a NewAnyListField (no defaults applied for nested levels).
		mode := "NULLABLE"
		if c.Contains(bqwaSchemaFieldMode) {
			rawMode, err := c.FieldString(bqwaSchemaFieldMode)
			if err != nil {
				return nil, fmt.Errorf("schema[%d] %q: %w", i, name, err)
			}
			if rawMode != "" {
				mode = strings.ToUpper(rawMode)
			}
		}
		if _, ok := validBQFieldModes[mode]; !ok {
			return nil, fmt.Errorf("schema[%d] %q: invalid mode %q", i, name, mode)
		}
		fld := bqSchemaField{Name: name, Type: canonical, Mode: mode}
		if canonical == "RECORD" {
			childConfs, _ := c.FieldAnyList(bqwaSchemaFieldFields)
			if len(childConfs) == 0 {
				return nil, fmt.Errorf("schema[%d] %q: RECORD requires at least one nested field", i, name)
			}
			fld.Fields, err = parseSchemaFields(childConfs)
			if err != nil {
				return nil, fmt.Errorf("schema[%d] %q: %w", i, name, err)
			}
		}
		out = append(out, fld)
	}
	return out, nil
}

func bqWriteAPIConfigFromParsed(pConf *service.ParsedConfig) (conf bqWriteAPIConfig, err error) {
	if conf.ProjectID, err = pConf.FieldString(bqwaFieldProject); err != nil {
		return
	}
	if conf.ProjectID == "" {
		conf.ProjectID = bigquery.DetectProjectID
	}
	if conf.DatasetID, err = pConf.FieldString(bqwaFieldDataset); err != nil {
		return
	}
	if conf.MessageFormat, err = pConf.FieldString(bqwaFieldMessageFormat); err != nil {
		return
	}
	if conf.WriteMode, err = pConf.FieldString(bqwaFieldWriteMode); err != nil {
		return
	}
	if pConf.Contains(bqwaFieldChangeType) {
		if conf.ChangeType, err = pConf.FieldInterpolatedString(bqwaFieldChangeType); err != nil {
			return
		}
	}
	if pConf.Contains(bqwaFieldChangeSequenceNumber) {
		if conf.ChangeSequenceNumber, err = pConf.FieldInterpolatedString(bqwaFieldChangeSequenceNumber); err != nil {
			return
		}
	}
	if conf.PrimaryKeys, err = pConf.FieldStringList(bqwaFieldPrimaryKeys); err != nil {
		return
	}
	isCDC := conf.WriteMode == "upsert" || conf.WriteMode == "upsert_delete"
	if isCDC && conf.ChangeType == nil {
		err = fmt.Errorf("%s is required when %s is %q", bqwaFieldChangeType, bqwaFieldWriteMode, conf.WriteMode)
		return
	}
	if isCDC && conf.MessageFormat != "json" {
		// CDC mode injects pseudo-columns via JSON byte rewriting. Raw protobuf
		// payloads would require deserialising via dynamicpb, setting the
		// fields, and re-serialising — extra cost the user can avoid by
		// converting upstream.
		err = fmt.Errorf("%s is not supported when %s is %q (use message_format: json)",
			bqwaFieldMessageFormat, bqwaFieldWriteMode, conf.WriteMode)
		return
	}
	if !isCDC && conf.ChangeType != nil {
		err = fmt.Errorf("%s is only valid when %s is upsert or upsert_delete", bqwaFieldChangeType, bqwaFieldWriteMode)
		return
	}
	if !isCDC && conf.ChangeSequenceNumber != nil {
		err = fmt.Errorf("%s is only valid when %s is upsert or upsert_delete", bqwaFieldChangeSequenceNumber, bqwaFieldWriteMode)
		return
	}
	if len(conf.PrimaryKeys) > 16 {
		err = fmt.Errorf("%s accepts at most 16 columns, got %d", bqwaFieldPrimaryKeys, len(conf.PrimaryKeys))
		return
	}
	if conf.AutoCreateTable, err = pConf.FieldBool(bqwaFieldAutoCreateTable); err != nil {
		return
	}
	schemaConfs, err := pConf.FieldObjectList(bqwaFieldSchema)
	if err != nil {
		return
	}
	if conf.Schema, err = parseSchemaFields(schemaConfs); err != nil {
		return
	}
	if conf.AutoCreateTable && len(conf.Schema) == 0 {
		err = fmt.Errorf("%s requires %s to be non-empty", bqwaFieldAutoCreateTable, bqwaFieldSchema)
		return
	}
	if isCDC && conf.AutoCreateTable && len(conf.PrimaryKeys) == 0 {
		err = fmt.Errorf("%s is required when %s is true and %s is %q",
			bqwaFieldPrimaryKeys, bqwaFieldAutoCreateTable, bqwaFieldWriteMode, conf.WriteMode)
		return
	}
	if len(conf.PrimaryKeys) > 0 && len(conf.Schema) > 0 {
		cols := make(map[string]string, len(conf.Schema))
		for _, f := range conf.Schema {
			cols[f.Name] = f.Mode
		}
		for _, pk := range conf.PrimaryKeys {
			mode, ok := cols[pk]
			if !ok {
				err = fmt.Errorf("%s column %q is not in %s", bqwaFieldPrimaryKeys, pk, bqwaFieldSchema)
				return
			}
			if mode != "REQUIRED" {
				err = fmt.Errorf("%s column %q must have mode REQUIRED in %s (got %q)",
					bqwaFieldPrimaryKeys, pk, bqwaFieldSchema, mode)
				return
			}
		}
	}
	// time_partitioning.type has no default, so a missing type — either via an
	// absent block or a block without `type:` — leaves Type empty and the
	// other subfields are ignored. Type set is the explicit opt-in.
	tp := pConf.Namespace(bqwaFieldTimePartitioning)
	if typ, terr := tp.FieldString(bqwatpFieldType); terr == nil && typ != "" {
		conf.TimePartitioning.Type = typ
		if conf.TimePartitioning.Field, err = tp.FieldString(bqwatpFieldField); err != nil {
			return
		}
		if conf.TimePartitioning.Expiration, err = tp.FieldDuration(bqwatpFieldExpiration); err != nil {
			return
		}
		if conf.TimePartitioning.RequireFilter, err = tp.FieldBool(bqwatpFieldRequireFilter); err != nil {
			return
		}
	}
	if conf.Clustering, err = pConf.FieldStringList(bqwaFieldClustering); err != nil {
		return
	}
	if len(conf.Clustering) > 4 {
		err = fmt.Errorf("%s accepts at most 4 columns, got %d", bqwaFieldClustering, len(conf.Clustering))
		return
	}
	if err = validateSchemaReferences(conf); err != nil {
		return
	}
	if conf.CredentialsJSON, err = pConf.FieldString(bqwaFieldCredentialsJSON); err != nil {
		return
	}
	if conf.TargetPrincipal, err = pConf.FieldString(bqwaFieldTargetPrincipal); err != nil {
		return
	}
	if conf.Delegates, err = pConf.FieldStringList(bqwaFieldDelegates); err != nil {
		return
	}
	if len(conf.Delegates) > 0 && conf.TargetPrincipal == "" {
		err = fmt.Errorf("%s requires %s to be set", bqwaFieldDelegates, bqwaFieldTargetPrincipal)
		return
	}
	if conf.StreamIdleTimeout, err = pConf.FieldDuration(bqwaFieldStreamIdleTimeout); err != nil {
		return
	}
	if conf.StreamIdleTimeout <= 0 {
		err = fmt.Errorf("%s must be greater than zero", bqwaFieldStreamIdleTimeout)
		return
	}
	if conf.StreamSweepInterval, err = pConf.FieldDuration(bqwaFieldStreamSweepInterval); err != nil {
		return
	}
	if conf.StreamSweepInterval <= 0 {
		err = fmt.Errorf("%s must be greater than zero", bqwaFieldStreamSweepInterval)
		return
	}
	if conf.MaxCachedStreams, err = pConf.FieldInt(bqwaFieldMaxCachedStreams); err != nil {
		return
	}
	if conf.MaxCachedStreams < 0 {
		err = fmt.Errorf("%s must be >= 0 (0 = unlimited)", bqwaFieldMaxCachedStreams)
		return
	}
	if conf.SchemaResolveTimeout, err = pConf.FieldDuration(bqwaFieldSchemaResolveTimeout); err != nil {
		return
	}
	if conf.SchemaResolveTimeout <= 0 {
		err = fmt.Errorf("%s must be greater than zero", bqwaFieldSchemaResolveTimeout)
		return
	}
	if conf.SchemaEvolutionTimeout, err = pConf.FieldDuration(bqwaFieldSchemaEvolutionTimeout); err != nil {
		return
	}
	if conf.SchemaEvolutionTimeout <= 0 {
		err = fmt.Errorf("%s must be greater than zero", bqwaFieldSchemaEvolutionTimeout)
		return
	}
	epConf := pConf.Namespace(bqwaFieldEndpoint)
	if conf.EndpointHTTP, err = epConf.FieldString(bqwaepFieldHTTP); err != nil {
		return
	}
	if conf.EndpointGRPC, err = epConf.FieldString(bqwaepFieldGRPC); err != nil {
		return
	}
	return
}

type bqwaMetrics struct {
	rowsSent   *service.MetricCounter
	rowsFailed *service.MetricCounter
	// batchesSent counts batches whose append RPC succeeded, including batches
	// with per-row failures — row-level detail lives in rowsSent/rowsFailed.
	batchesSent             *service.MetricCounter
	batchLatency            *service.MetricTimer
	retries                 *service.MetricCounter
	schemaEvolutions        *service.MetricCounter
	schemaEvolutionFailures *service.MetricCounter
	// cachedStreams reflects the current size of the default-stream cache so
	// operators can tell whether max_cached_streams is sized correctly.
	cachedStreams *service.MetricGauge
	// streamsEvicted counts every cache eviction (idle-sweep + LRU + on-error).
	// A spike here without a corresponding writer-error spike usually indicates
	// max_cached_streams is too low for the table fan-out.
	streamsEvicted *service.MetricCounter
}

func newBQWAMetrics(m *service.Metrics) *bqwaMetrics {
	return &bqwaMetrics{
		rowsSent:                m.NewCounter("bigquery_write_api_rows_sent_total"),
		rowsFailed:              m.NewCounter("bigquery_write_api_rows_failed_total"),
		batchesSent:             m.NewCounter("bigquery_write_api_batches_sent_total"),
		batchLatency:            m.NewTimer("bigquery_write_api_batch_latency_ns"),
		retries:                 m.NewCounter("bigquery_write_api_retries_total"),
		schemaEvolutions:        m.NewCounter("bigquery_write_api_schema_evolutions_total"),
		schemaEvolutionFailures: m.NewCounter("bigquery_write_api_schema_evolutions_failures_total"),
		cachedStreams:           m.NewGauge("bigquery_write_api_streams_cached"),
		streamsEvicted:          m.NewCounter("bigquery_write_api_streams_evicted_total"),
	}
}

type bqErrorKind int

const (
	bqErrorTransient bqErrorKind = iota
	bqErrorPermanent
	bqErrorSchemaMismatch
)

// bqError wraps an error from the BigQuery REST or Storage Write APIs together
// with its retryability classification. Callers query intent via IsRetryable /
// IsPermanent / IsSchemaMismatch rather than re-inspecting the underlying
// gRPC status or googleapi.Error.
//
// SCHEMA_MISMATCH_EXTRA_FIELDS is kept distinct from generic permanent errors
// because it can be resolved by adding the missing columns and retrying; every
// other permanent classification routes straight to the DLQ.
type bqError struct {
	kind bqErrorKind
	err  error
}

func (e bqError) Error() string { return e.err.Error() }
func (e bqError) Unwrap() error { return e.err }

func (e bqError) IsRetryable() bool      { return e.kind == bqErrorTransient }
func (e bqError) IsPermanent() bool      { return e.kind == bqErrorPermanent }
func (e bqError) IsSchemaMismatch() bool { return e.kind == bqErrorSchemaMismatch }

// errPermanent is a sentinel that callers can wrap into errors that have no
// underlying gRPC/googleapi classification (e.g. CDC config validation) so
// classifyBQError surfaces them as permanent and the WriteBatch loop routes
// the batch to DLQ instead of retrying forever.
var errPermanent = errors.New("permanent error")

// classifyBQError inspects err and returns a bqError carrying its retry
// classification. gRPC permanent codes (InvalidArgument, NotFound,
// PermissionDenied, AlreadyExists, FailedPrecondition, Unimplemented,
// Unauthenticated) and REST 4xx codes (except 408 timeout and 429 throttling)
// are treated as permanent; SCHEMA_MISMATCH_EXTRA_FIELDS surfaces separately;
// errors wrapped with errPermanent are also treated as permanent; everything
// else falls back to transient so the benthos retry loop gets a chance.
func classifyBQError(err error) bqError {
	if errors.Is(err, errPermanent) {
		return bqError{kind: bqErrorPermanent, err: err}
	}
	if st, ok := grpcstatus.FromError(err); ok {
		for _, detail := range st.Details() {
			if storageErr, ok := detail.(*storagepb.StorageError); ok {
				if storageErr.GetCode() == storagepb.StorageError_SCHEMA_MISMATCH_EXTRA_FIELDS {
					return bqError{kind: bqErrorSchemaMismatch, err: err}
				}
			}
		}
		switch st.Code() {
		case codes.InvalidArgument,
			codes.NotFound,
			codes.PermissionDenied,
			codes.AlreadyExists,
			codes.FailedPrecondition,
			codes.Unimplemented,
			codes.Unauthenticated:
			return bqError{kind: bqErrorPermanent, err: err}
		}
		return bqError{kind: bqErrorTransient, err: err}
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) &&
		apiErr.Code >= 400 && apiErr.Code < 500 &&
		apiErr.Code != http.StatusRequestTimeout &&
		apiErr.Code != http.StatusTooManyRequests {
		return bqError{kind: bqErrorPermanent, err: err}
	}

	return bqError{kind: bqErrorTransient, err: err}
}

type streamWithDescriptor struct {
	stream     *managedwriter.ManagedStream
	descriptor protoreflect.MessageDescriptor
	// baseDescriptor is the user table's descriptor without the CDC pseudo-column
	// wrap. Equal to descriptor for non-CDC streams. Used by handleWriteError so
	// schema evolution diffs against the actual table schema rather than trying
	// to add _CHANGE_TYPE / _CHANGE_SEQUENCE_NUMBER as real columns.
	baseDescriptor  protoreflect.MessageDescriptor
	descriptorProto *descriptorpb.DescriptorProto // needed by pendingStreamWriter (write_mode=pending_stream)
	lastUsed        atomic.Int64                  // UnixNano timestamp, safe for concurrent access
}

type bigQueryWriteAPIOutput struct {
	conf        bqWriteAPIConfig
	tableInterp *service.InterpolatedString
	log         *service.Logger
	metrics     *bqwaMetrics
	resolver    *schemaResolver
	evolver     *schemaEvolver

	connMu            sync.RWMutex
	client            *bigquery.Client
	storageClient     *managedwriter.Client
	resolvedProjectID string
	// pending is non-nil while connected when write_mode=pending_stream.
	// It wraps storageClient and runs the per-batch Create/Append/Finalize/Commit
	// lifecycle. Nil-checked rather than gated on conf, so a pending-mode write
	// against a disconnected output cleanly returns ErrNotConnected.
	pending *pendingStreamWriter
	// cdc is non-nil when write_mode is upsert or upsert_delete. Holds the
	// Bloblang fields and the wrapped descriptor; injected per-row into
	// writeBatchCDC.
	cdc *cdcInjector

	// Lock ordering: connMu must always be acquired before streamsMu to
	// prevent deadlocks. Close() acquires connMu then streamsMu;
	// getOrCreateStream/createStream acquire them independently but never
	// hold streamsMu while calling createStream.
	streamsMu sync.RWMutex
	streams   map[string]*streamWithDescriptor
	stopSweep chan struct{}
	sweepWg   sync.WaitGroup

	// closeWg tracks background ManagedStream.Close goroutines spawned by
	// evictStream and the sweep loop, so Close can wait for them to finish
	// before tearing down the underlying clients.
	closeWg sync.WaitGroup
}

var _ service.BatchOutput = (*bigQueryWriteAPIOutput)(nil)

func bigQueryWriteAPIOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*bigQueryWriteAPIOutput, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}
	cfg, err := bqWriteAPIConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}
	tableInterp, err := conf.FieldInterpolatedString(bqwaFieldTable)
	if err != nil {
		return nil, err
	}

	creator, err := newTableCreator(cfg, mgr.Logger())
	if err != nil {
		return nil, err
	}

	var cdc *cdcInjector
	if cfg.WriteMode == "upsert" || cfg.WriteMode == "upsert_delete" {
		cdc = &cdcInjector{
			changeType:  cfg.ChangeType,
			changeSeq:   cfg.ChangeSequenceNumber,
			allowDelete: cfg.WriteMode == "upsert_delete",
		}
	}

	return &bigQueryWriteAPIOutput{
		conf:        cfg,
		tableInterp: tableInterp,
		log:         mgr.Logger(),
		metrics:     newBQWAMetrics(mgr.Metrics()),
		streams:     make(map[string]*streamWithDescriptor),
		resolver: &schemaResolver{
			log:            mgr.Logger(),
			resolveTimeout: cfg.SchemaResolveTimeout,
			creator:        creator,
		},
		evolver: &schemaEvolver{log: mgr.Logger(), evolveTimeout: cfg.SchemaEvolutionTimeout},
		cdc:     cdc,
	}, nil
}

func (o *bigQueryWriteAPIOutput) Connect(ctx context.Context) error {
	o.connMu.Lock()
	defer o.connMu.Unlock()

	if o.client != nil {
		return nil
	}

	bqOpts, err := o.buildAuthOpts(ctx, o.conf.EndpointHTTP, false)
	if err != nil {
		return err
	}

	storageOpts, err := o.buildAuthOpts(ctx, o.conf.EndpointGRPC, true)
	if err != nil {
		return err
	}

	bqClient, err := bigquery.NewClient(ctx, o.conf.ProjectID, bqOpts...)
	if err != nil {
		return fmt.Errorf("creating bigquery client: %w", err)
	}

	// Resolve the real project ID if auto-detection was used, so that
	// tableCacheKey produces valid resource paths for the Storage Write API.
	resolvedProject := bqClient.Project()
	if resolvedProject == "" {
		_ = bqClient.Close()
		return errors.New("could not determine GCP project ID; set the 'project' field explicitly")
	}

	storageClient, err := managedwriter.NewClient(ctx, resolvedProject, storageOpts...)
	if err != nil {
		_ = bqClient.Close()
		return fmt.Errorf("creating storage write client: %w", err)
	}

	o.resolvedProjectID = resolvedProject
	o.client = bqClient
	o.storageClient = storageClient
	o.pending = &pendingStreamWriter{storage: storageClient}
	o.stopSweep = make(chan struct{})
	o.sweepWg.Add(1)
	go o.sweepIdleStreams(o.stopSweep)
	return nil
}

func (o *bigQueryWriteAPIOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}

	// Snapshot client, resolvedProjectID, and pending together under one
	// RLock so a concurrent Connect-after-Close (which rewrites all three)
	// cannot tear them apart. In pending-stream mode we also call Begin()
	// while holding the lock so the inflight counter is incremented before
	// Close can acquire its write lock; this closes the race where Close
	// would observe inflight=0 between the WriteBatch lock release and the
	// pending.Write entry.
	o.connMu.RLock()
	client := o.client
	projectID := o.resolvedProjectID
	pending := o.pending
	var pendingDone func()
	if o.conf.WriteMode == "pending_stream" && pending != nil {
		pendingDone = pending.Begin()
	}
	o.connMu.RUnlock()
	if pendingDone != nil {
		defer pendingDone()
	}

	if client == nil {
		return service.ErrNotConnected
	}

	start := time.Now()

	rawTableID, err := batch.TryInterpolatedString(0, o.tableInterp)
	if err != nil {
		return fmt.Errorf("interpolating table name: %w", err)
	}
	tableID := sanitizeTableName(rawTableID)
	if tableID == "" {
		// Permanent: the interpolation result has no usable characters.
		return permanentBatchError(batch, fmt.Errorf("interpolated table name %q is empty after sanitization", rawTableID))
	}

	swd, cacheKey, err := o.getOrCreateStream(ctx, client, projectID, tableID)
	if err != nil {
		if classifyBQError(err).IsPermanent() {
			return permanentBatchError(batch, fmt.Errorf("getting stream for table %q: %w", tableID, err))
		}
		return fmt.Errorf("getting stream for table %q: %w", tableID, err)
	}

	// CDC write modes inject _CHANGE_TYPE (and optionally _CHANGE_SEQUENCE_NUMBER)
	// per row. Bad rows go to DLQ via BatchError.Failed(i, err) so good rows in
	// the same batch still get written.
	if o.cdc != nil {
		return o.writeBatchCDC(ctx, client, swd, cacheKey, batch, tableID, start)
	}

	rows := make([][]byte, 0, len(batch))
	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("reading message %d: %w", i, err)
		}

		var protoBytes []byte
		switch o.conf.MessageFormat {
		case "json":
			protoBytes, err = jsonToProtoBytes(msgBytes, swd.descriptor)
			if err != nil {
				return fmt.Errorf("converting message %d from JSON to proto: %w", i, err)
			}
		case "protobuf":
			protoBytes = msgBytes
		default:
			// Safety net: the config spec's enum validation rejects any value
			// other than "json" or "protobuf" before WriteBatch is called.
			return fmt.Errorf("unsupported message format: %s", o.conf.MessageFormat)
		}
		rows = append(rows, protoBytes)
	}

	// Pending-stream mode bypasses the cached default-stream and runs a fresh
	// Create/Append/Finalize/Commit lifecycle per batch. Schema-mismatch and
	// permanent errors flow through handleWriteError just like the default
	// path so evolution + DLQ semantics stay consistent.
	if o.conf.WriteMode == "pending_stream" {
		if pending == nil {
			return service.ErrNotConnected
		}
		parent := o.tableCacheKey(projectID, tableID)
		if err := pending.Write(ctx, parent, swd.descriptorProto, rows); err != nil {
			// Evict the cached stream so a fresh descriptor is fetched on retry
			// — matches the default-stream branch below. The cached swd is only
			// consulted for its descriptor in pending mode, but a schema-mismatch
			// error means that descriptor is stale.
			o.evictStream(cacheKey)
			return o.handleWriteError(ctx, client, err, batch, tableID, swd.descriptor, "pending stream write")
		}
		o.metrics.batchLatency.Timing(time.Since(start).Nanoseconds())
		o.metrics.batchesSent.Incr(1)
		o.metrics.rowsSent.Incr(int64(len(batch)))
		return nil
	}

	result, err := swd.stream.AppendRows(ctx, rows)
	if err != nil {
		o.evictStream(cacheKey)
		return o.handleWriteError(ctx, client, err, batch, tableID, swd.descriptor, "appending rows")
	}

	resp, err := result.FullResponse(ctx)
	if err != nil {
		o.evictStream(cacheKey)
		return o.handleWriteError(ctx, client, err, batch, tableID, swd.descriptor, "waiting for append result")
	}

	o.metrics.batchLatency.Timing(time.Since(start).Nanoseconds())

	if resp.GetUpdatedSchema() != nil {
		o.log.Infof("BigQuery reported schema update for table %q, evicting cached stream", tableID)
		o.evictStream(cacheKey)
		o.resolver.Evict(tableID)
	}

	if rowErrs := resp.GetRowErrors(); len(rowErrs) > 0 {
		o.metrics.rowsFailed.Incr(int64(len(rowErrs)))
		// Clamp: BQ shouldn't return more row errors than rows in the request,
		// but guard the metric counter against a negative delta if it ever did.
		o.metrics.rowsSent.Incr(int64(max(0, len(batch)-len(rowErrs))))
		// The append RPC itself succeeded, so the batch counts as sent — the
		// CDC path does the same; row-level detail lives in rowsSent/rowsFailed.
		o.metrics.batchesSent.Incr(1)
		batchErr := service.NewBatchError(batch, errors.New("row errors from BigQuery"))
		for _, re := range rowErrs {
			idx := int(re.GetIndex())
			if idx >= 0 && idx < len(batch) {
				batchErr = batchErr.Failed(idx, fmt.Errorf("row %d: code %d: %s", idx, re.GetCode(), re.GetMessage()))
			}
		}
		return batchErr
	}

	o.metrics.batchesSent.Incr(1)
	o.metrics.rowsSent.Incr(int64(len(batch)))
	return nil
}

// permanentBatchError builds a service.BatchError that fails every message in
// the batch with the same root cause. Use this for errors that benthos must
// not retry.
func permanentBatchError(batch service.MessageBatch, err error) error {
	batchErr := service.NewBatchError(batch, err)
	for i := range batch {
		batchErr = batchErr.Failed(i, err)
	}
	return batchErr
}

// appendRowFailure lazily constructs and appends to a BatchError so the caller
// can collect per-row failures without committing to a sentinel error when
// there are no failures. headline becomes the BatchError's underlying error
// when the first failure is appended; it's ignored on subsequent calls.
func appendRowFailure(be *service.BatchError, batch service.MessageBatch, headline string, idx int, err error) *service.BatchError {
	if be == nil {
		be = service.NewBatchError(batch, errors.New(headline))
	}
	return be.Failed(idx, err)
}

// mapRowErrorsToBatch attaches BigQuery per-row errors to batchErr. Each
// RowError's index refers to its position within the rows that were actually
// sent (good rows only, after validation drops), so it is translated back to
// the original batch index via rowIndex; both the attached failure and its
// message reference that original index so they agree with the failed message.
// Indices outside rowIndex are ignored.
func mapRowErrorsToBatch(batchErr *service.BatchError, batch service.MessageBatch, rowIndex []int, rowErrs []*storagepb.RowError) *service.BatchError {
	for _, re := range rowErrs {
		idx := int(re.GetIndex())
		if idx >= 0 && idx < len(rowIndex) {
			origIdx := rowIndex[idx]
			batchErr = appendRowFailure(batchErr, batch, "row errors from BigQuery", origIdx,
				fmt.Errorf("row %d: code %d: %s", origIdx, re.GetCode(), re.GetMessage()))
		}
	}
	return batchErr
}

// writeBatchCDC executes the CDC write path: per row, resolve the Bloblang
// fields, validate the values, inject _CHANGE_TYPE (and optionally
// _CHANGE_SEQUENCE_NUMBER) into the JSON payload, marshal to proto via the
// wrapped descriptor, and AppendRows the result. Per-row validation failures
// are collected into a BatchError so good rows in the same batch are still
// written.
func (o *bigQueryWriteAPIOutput) writeBatchCDC(
	ctx context.Context,
	client *bigquery.Client,
	swd *streamWithDescriptor,
	cacheKey string,
	batch service.MessageBatch,
	tableID string,
	start time.Time,
) error {
	if o.conf.MessageFormat != "json" {
		// Defensive — the parser already rejects message_format != json in CDC
		// modes. If this fires it means the parser missed a case.
		return permanentBatchError(batch, fmt.Errorf("CDC modes require message_format: json (got %q)", o.conf.MessageFormat))
	}

	var (
		batchErr           *service.BatchError
		rows               = make([][]byte, 0, len(batch))
		rowIndex           = make([]int, 0, len(batch))
		validationFailures = 0
	)

	const cdcValidationHeadline = "CDC row validation errors"
	for i, msg := range batch {
		ct, err := batch.TryInterpolatedString(i, o.cdc.changeType)
		if err != nil {
			batchErr = appendRowFailure(batchErr, batch, cdcValidationHeadline, i, fmt.Errorf("interpolating change_type: %w", err))
			validationFailures++
			continue
		}
		var seq string
		if o.cdc.changeSeq != nil {
			seq, err = batch.TryInterpolatedString(i, o.cdc.changeSeq)
			if err != nil {
				batchErr = appendRowFailure(batchErr, batch, cdcValidationHeadline, i, fmt.Errorf("interpolating change_sequence_number: %w", err))
				validationFailures++
				continue
			}
		}

		validatedCT, validatedSeq, err := o.cdc.validateAndResolveCDC(ct, seq)
		if err != nil {
			batchErr = appendRowFailure(batchErr, batch, cdcValidationHeadline, i, err)
			validationFailures++
			continue
		}

		msgBytes, err := msg.AsBytes()
		if err != nil {
			batchErr = appendRowFailure(batchErr, batch, cdcValidationHeadline, i, fmt.Errorf("reading message: %w", err))
			validationFailures++
			continue
		}

		injectedBytes, err := injectCDCJSON(msgBytes, validatedCT, validatedSeq)
		if err != nil {
			batchErr = appendRowFailure(batchErr, batch, cdcValidationHeadline, i, err)
			validationFailures++
			continue
		}
		protoBytes, err := jsonToProtoBytes(injectedBytes, swd.descriptor)
		if err != nil {
			batchErr = appendRowFailure(batchErr, batch, cdcValidationHeadline, i, fmt.Errorf("converting JSON to proto: %w", err))
			validationFailures++
			continue
		}
		rows = append(rows, protoBytes)
		rowIndex = append(rowIndex, i)
	}

	// validationFailures is counted into rowsFailed only on the paths that
	// surface batchErr (below, and after a successful append). On the
	// handleWriteError paths the whole batch is retried or DLQ'd, and counting
	// before AppendRows would re-count the same failures on every retry.
	if len(rows) == 0 {
		if batchErr != nil {
			o.metrics.rowsFailed.Incr(int64(validationFailures))
			return batchErr
		}
		return nil
	}

	result, err := swd.stream.AppendRows(ctx, rows)
	if err != nil {
		o.evictStream(cacheKey)
		// Pass baseDescriptor (not the CDC-wrapped one) so schema evolution
		// diffs against the real table schema; otherwise Evolve would see
		// _CHANGE_TYPE / _CHANGE_SEQUENCE_NUMBER as "missing" columns and
		// try to ALTER TABLE to add them.
		return o.handleWriteError(ctx, client, err, batch, tableID, swd.baseDescriptor, "appending CDC rows")
	}
	resp, err := result.FullResponse(ctx)
	if err != nil {
		o.evictStream(cacheKey)
		return o.handleWriteError(ctx, client, err, batch, tableID, swd.baseDescriptor, "waiting for CDC append result")
	}

	o.metrics.batchLatency.Timing(time.Since(start).Nanoseconds())
	if resp.GetUpdatedSchema() != nil {
		o.log.Infof("BigQuery reported schema update for table %q, evicting cached stream", tableID)
		o.evictStream(cacheKey)
		o.resolver.Evict(tableID)
	}

	if rowErrs := resp.GetRowErrors(); len(rowErrs) > 0 {
		batchErr = mapRowErrorsToBatch(batchErr, batch, rowIndex, rowErrs)
		o.metrics.rowsFailed.Incr(int64(len(rowErrs)))
	}
	if validationFailures > 0 {
		o.metrics.rowsFailed.Incr(int64(validationFailures))
	}
	o.metrics.batchesSent.Incr(1)
	o.metrics.rowsSent.Incr(int64(max(0, len(rows)-len(resp.GetRowErrors()))))
	if batchErr != nil {
		return batchErr
	}
	return nil
}

// handleWriteError classifies a gRPC error from an append or response and
// returns the appropriate error for benthos: a BatchError (permanent, no retry)
// or a plain error (transient, retry).
func (o *bigQueryWriteAPIOutput) handleWriteError(
	ctx context.Context, client *bigquery.Client, err error, batch service.MessageBatch,
	tableID string, descriptor protoreflect.MessageDescriptor, phase string,
) error {
	bqErr := classifyBQError(err)
	switch {
	case bqErr.IsSchemaMismatch():
		// Evolve returns (true, nil) for every outcome that warrants a retry —
		// columns we added, columns another writer added, or columns already
		// present when we read metadata. Any (false, ...) result carries a
		// non-nil error.
		if _, evolveErr := o.evolver.Evolve(ctx, client, o.conf.DatasetID, tableID, descriptor); evolveErr != nil {
			o.metrics.schemaEvolutionFailures.Incr(1)
			o.log.Warnf("Schema evolution failed for table %q: %v", tableID, evolveErr)
			return permanentBatchError(batch, fmt.Errorf("schema evolution failed for table %q: %w", tableID, evolveErr))
		}
		o.metrics.schemaEvolutions.Incr(1)
		o.resolver.Evict(tableID)
		o.metrics.retries.Incr(1)
		return fmt.Errorf("schema mismatch (evolution attempted): %w", err)
	case bqErr.IsPermanent():
		return permanentBatchError(batch, fmt.Errorf("permanent error %s: %w", phase, err))
	default:
		o.metrics.retries.Incr(1)
		return fmt.Errorf("%s: %w", phase, err)
	}
}

func (o *bigQueryWriteAPIOutput) Close(ctx context.Context) error {
	o.connMu.Lock()
	defer o.connMu.Unlock()

	if o.stopSweep != nil {
		close(o.stopSweep)
		o.stopSweep = nil
	}

	// Wait for the sweep goroutine to finish before closing streams/clients
	// so it does not access shared state after shutdown.
	o.sweepWg.Wait()

	// Wait for any in-flight async stream closes (from evictStream or
	// sweepIdleStreams) so they don't race with client shutdown.
	o.closeWg.Wait()

	// Wait for in-flight pending-stream Writes so they finish their
	// Finalize/BatchCommit calls before the underlying storage client is torn
	// down. Without this an in-flight pending Write would observe a closed
	// gRPC connection mid-commit and surface a permanent batch error.
	if o.pending != nil {
		o.pending.Wait()
	}

	o.streamsMu.Lock()
	streams := o.streams
	o.streams = make(map[string]*streamWithDescriptor)
	o.streamsMu.Unlock()

	// Close every stream and client unconditionally: the underlying gRPC
	// teardowns are non-blocking and skipping them leaks connections and
	// goroutines. ctx.Err() is appended afterwards so a caller-side deadline
	// is still surfaced, but it never gates the cleanup itself.
	var errs []error
	for _, swd := range streams {
		if err := swd.stream.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if o.storageClient != nil {
		if err := o.storageClient.Close(); err != nil {
			errs = append(errs, err)
		}
		o.storageClient = nil
	}
	o.pending = nil

	if o.client != nil {
		if err := o.client.Close(); err != nil {
			errs = append(errs, err)
		}
		o.client = nil
	}

	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}

	// Drop the resolver's schema cache so a subsequent Connect doesn't reuse
	// state tied to the now-closed client.
	o.resolver.cache.Clear()

	return errors.Join(errs...)
}

// buildAuthOpts returns client options for authentication based on the config.
// If an endpoint override is provided, authentication is skipped (emulator mode).
func (o *bigQueryWriteAPIOutput) buildAuthOpts(ctx context.Context, endpointOverride string, isGRPC bool) ([]option.ClientOption, error) {
	if endpointOverride != "" {
		o.log.Warnf("endpoint override %q is set; authentication is disabled — do not use this against production BigQuery", endpointOverride)
		if o.conf.TargetPrincipal != "" {
			o.log.Warnf("endpoint override is set; ignoring target_principal %q", o.conf.TargetPrincipal)
		}
		opts := []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithEndpoint(endpointOverride),
		}
		if isGRPC {
			opts = append(opts, option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
		}
		return opts, nil
	}

	var opts []option.ClientOption

	if o.conf.TargetPrincipal != "" {
		var baseOpts []option.ClientOption
		if o.conf.CredentialsJSON != "" {
			baseOpts = append(baseOpts, option.WithCredentialsJSON([]byte(o.conf.CredentialsJSON)))
		}

		// Detach from ctx: the token source outlives Connect (it refreshes
		// tokens on demand for the lifetime of the client). If we passed
		// Connect's ctx and that ctx was later cancelled, every refresh
		// would fail.
		ts, err := impersonate.CredentialsTokenSource(context.WithoutCancel(ctx), impersonate.CredentialsConfig{
			TargetPrincipal: o.conf.TargetPrincipal,
			Scopes:          []string{bigquery.Scope},
			Delegates:       o.conf.Delegates,
		}, baseOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating impersonated credentials for %q: %w", o.conf.TargetPrincipal, err)
		}
		opts = append(opts, option.WithTokenSource(ts))
		return opts, nil
	}

	if o.conf.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(o.conf.CredentialsJSON)))
	}

	return opts, nil
}

// tableCacheKey builds the BigQuery resource path for the given table. The
// projectID argument is captured by callers under connMu.RLock and passed
// through, so this function must not read o.resolvedProjectID directly.
func (o *bigQueryWriteAPIOutput) tableCacheKey(projectID, tableID string) string {
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, o.conf.DatasetID, tableID)
}

// closeStreamAsync schedules a stream close on a background goroutine tracked
// by closeWg, so Close() can wait for it to finish before tearing down the
// underlying clients.
func (o *bigQueryWriteAPIOutput) closeStreamAsync(s *managedwriter.ManagedStream) {
	if s == nil {
		return
	}
	o.closeWg.Go(func() { _ = s.Close() })
}

func (o *bigQueryWriteAPIOutput) getOrCreateStream(ctx context.Context, client *bigquery.Client, projectID, tableID string) (*streamWithDescriptor, string, error) {
	// parentPath is the BigQuery resource path used as the AppendRows
	// destination. cacheKey adds a write-mode suffix so a CDC pipeline and a
	// non-CDC pipeline pointed at the same table cannot share a stream — the
	// underlying ManagedStream is bound to its schema descriptor, and the CDC
	// path wraps the descriptor with pseudo-columns.
	parentPath := o.tableCacheKey(projectID, tableID)
	cacheKey := parentPath + "#mode=" + o.conf.WriteMode

	now := time.Now()

	// Fast path: check cache under read lock.
	o.streamsMu.RLock()
	if cached, exists := o.streams[cacheKey]; exists {
		cached.lastUsed.Store(now.UnixNano())
		o.streamsMu.RUnlock()
		return cached, cacheKey, nil
	}
	o.streamsMu.RUnlock()

	// Slow path: create stream without holding the lock (network I/O).
	swd, err := o.createStream(ctx, client, parentPath, tableID)
	if err != nil {
		return nil, cacheKey, err
	}

	// Store in cache, but another goroutine may have raced us.
	o.streamsMu.Lock()
	if cached, exists := o.streams[cacheKey]; exists {
		o.streamsMu.Unlock()
		o.closeStreamAsync(swd.stream)
		return cached, cacheKey, nil
	}
	swd.lastUsed.Store(now.UnixNano())
	o.streams[cacheKey] = swd
	// Enforce the max_cached_streams cap by evicting the least-recently-used
	// stream. We only run the scan when over the cap, and we collect the LRU
	// key under the lock then close it asynchronously after Unlock — the cold
	// path of the cache holds streamsMu.Lock for ~10μs at max=1024 (a tight
	// loop over an O(n) map), which is negligible compared to the
	// NewManagedStream RPC that already ran above.
	var evicted *streamWithDescriptor
	if o.conf.MaxCachedStreams > 0 && len(o.streams) > o.conf.MaxCachedStreams {
		var lruKey string
		var lruTS int64 = -1
		for k, s := range o.streams {
			if k == cacheKey {
				// Don't evict the entry we just inserted (its lastUsed is now).
				continue
			}
			ts := s.lastUsed.Load()
			if lruTS == -1 || ts < lruTS {
				lruKey = k
				lruTS = ts
			}
		}
		if lruKey != "" {
			evicted = o.streams[lruKey]
			delete(o.streams, lruKey)
		}
	}
	size := int64(len(o.streams))
	o.streamsMu.Unlock()
	o.metrics.cachedStreams.Set(size)
	if evicted != nil {
		o.metrics.streamsEvicted.Incr(1)
		o.closeStreamAsync(evicted.stream)
	}
	return swd, cacheKey, nil
}

// evictStream removes a stream from the cache and closes it on a tracked
// background goroutine. Concurrent WriteBatch goroutines that already hold a
// reference to the evicted stream will see errors from the closed stream and
// retry, which will create a fresh stream via getOrCreateStream.
func (o *bigQueryWriteAPIOutput) evictStream(cacheKey string) {
	o.streamsMu.Lock()
	swd, exists := o.streams[cacheKey]
	delete(o.streams, cacheKey)
	size := int64(len(o.streams))
	o.streamsMu.Unlock()

	o.metrics.cachedStreams.Set(size)
	if exists {
		o.metrics.streamsEvicted.Incr(1)
		o.closeStreamAsync(swd.stream)
	}
}

// sweepIdleStreams periodically evicts streams that haven't been used within
// the configured idle timeout. This prevents unbounded growth of the stream cache when
// the table field uses interpolation and routes to many distinct tables.
func (o *bigQueryWriteAPIOutput) sweepIdleStreams(stop <-chan struct{}) {
	defer o.sweepWg.Done()

	ticker := time.NewTicker(o.conf.StreamSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		now := time.Now()
		type evicted struct {
			key string
			swd *streamWithDescriptor
		}
		var toClose []evicted

		o.streamsMu.Lock()
		for key, swd := range o.streams {
			lastUsed := time.Unix(0, swd.lastUsed.Load())
			if now.Sub(lastUsed) > o.conf.StreamIdleTimeout {
				toClose = append(toClose, evicted{key, swd})
				delete(o.streams, key)
			}
		}
		size := int64(len(o.streams))
		o.streamsMu.Unlock()

		if len(toClose) > 0 {
			o.metrics.cachedStreams.Set(size)
			o.metrics.streamsEvicted.Incr(int64(len(toClose)))
		}
		for _, e := range toClose {
			o.log.Debugf("Closing idle BigQuery stream for %s", e.key)
			o.closeStreamAsync(e.swd.stream)
		}
	}
}

func (o *bigQueryWriteAPIOutput) createStream(ctx context.Context, client *bigquery.Client, parentPath, tableID string) (*streamWithDescriptor, error) {
	o.connMu.RLock()
	storageClient := o.storageClient
	o.connMu.RUnlock()

	if storageClient == nil {
		return nil, service.ErrNotConnected
	}

	rs, err := o.resolver.Resolve(ctx, client, o.conf.DatasetID, tableID)
	if err != nil {
		return nil, err
	}

	// In CDC mode, append _CHANGE_TYPE (and optionally _CHANGE_SEQUENCE_NUMBER)
	// to the user's descriptor and use the wrapped version for the managed
	// stream. The resolver's cached descriptor stays unchanged so non-CDC
	// pipelines sharing the resolver are not affected.
	descriptorProto := rs.descriptorProto
	descriptor := rs.messageDescriptor
	if o.cdc != nil {
		// CDC requires at least one PK source. If primary_keys is configured
		// AND the table already declares PKs, both must agree. These are
		// config-level errors with no underlying gRPC/googleapi classification,
		// so wrap them with errPermanent to short-circuit the retry loop and
		// route the batch to DLQ — the spec says these should fail loudly.
		if err := validateCDCPrimaryKeys(o.conf.PrimaryKeys, rs.primaryKeys, tableID); err != nil {
			return nil, fmt.Errorf("%w: %w", errPermanent, err)
		}
		wrapped, err := wrapDescriptorForCDC(rs.descriptorProto, o.cdc.changeSeq != nil)
		if err != nil {
			return nil, fmt.Errorf("%w: wrapping descriptor for CDC: %w", errPermanent, err)
		}
		wrappedMD, err := descriptorProtoToMessageDescriptor(wrapped)
		if err != nil {
			return nil, fmt.Errorf("%w: building wrapped message descriptor: %w", errPermanent, err)
		}
		descriptorProto = wrapped
		descriptor = wrappedMD
	}

	// Detach from the per-batch ctx: the cached stream outlives this WriteBatch
	// and is reused by every subsequent batch routing to the same table. If the
	// stream were bound to this ctx, cancellation of the first batch (per-message
	// deadline, source shutdown, ack timeout) would block all later AppendRows
	// against the cached stream until the idle sweeper evicted it.
	ms, err := storageClient.NewManagedStream(context.WithoutCancel(ctx),
		managedwriter.WithDestinationTable(parentPath),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(descriptorProto),
	)
	if err != nil {
		return nil, fmt.Errorf("creating managed stream for %q: %w", parentPath, err)
	}

	return &streamWithDescriptor{
		stream:          ms,
		descriptor:      descriptor,
		baseDescriptor:  rs.messageDescriptor,
		descriptorProto: descriptorProto,
	}, nil
}

// descriptorProtoToMessageDescriptor converts a *descriptorpb.DescriptorProto
// into a protoreflect.MessageDescriptor by wrapping it in a synthetic
// FileDescriptorProto and resolving it via protodesc.
func descriptorProtoToMessageDescriptor(dp *descriptorpb.DescriptorProto) (protoreflect.MessageDescriptor, error) {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        new("synthetic.proto"),
		Syntax:      new("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{dp},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		return nil, fmt.Errorf("creating file descriptor from normalized proto: %w", err)
	}
	if fd.Messages().Len() == 0 {
		return nil, errors.New("normalized descriptor produced no messages")
	}
	return fd.Messages().Get(0), nil
}

func jsonToProtoBytes(jsonData []byte, descriptor protoreflect.MessageDescriptor) ([]byte, error) {
	msg := dynamicpb.NewMessage(descriptor)
	if err := protojson.Unmarshal(jsonData, msg); err != nil {
		return nil, fmt.Errorf("unmarshalling JSON into proto message: %w", err)
	}
	return proto.Marshal(msg)
}
