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

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// maxNestedRecordDepth caps RECORD nesting so a self-referential proto
// descriptor cannot blow the stack. BigQuery's own limit is 15.
const maxNestedRecordDepth = 15

// protoKindToBQFieldType maps a protobuf field kind to a BigQuery field type.
// Returns an error for kinds that don't have a sensible BigQuery mapping
// rather than silently coercing them to STRING.
func protoKindToBQFieldType(kind protoreflect.Kind) (bigquery.FieldType, error) {
	switch kind {
	case protoreflect.StringKind:
		return bigquery.StringFieldType, nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Uint32Kind,
		protoreflect.Fixed64Kind, protoreflect.Fixed32Kind:
		return bigquery.IntegerFieldType, nil
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return bigquery.FloatFieldType, nil
	case protoreflect.BoolKind:
		return bigquery.BooleanFieldType, nil
	case protoreflect.BytesKind:
		return bigquery.BytesFieldType, nil
	case protoreflect.EnumKind:
		return bigquery.StringFieldType, nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return bigquery.RecordFieldType, nil
	default:
		return "", fmt.Errorf("no BigQuery type mapping for proto kind %s", kind)
	}
}

// fieldDescriptorToBQ converts one proto field descriptor to a BigQuery field
// schema. depth is the current RECORD nesting level; recursion past
// maxNestedRecordDepth fails fast rather than risking a stack overflow on
// self-referential descriptors.
func fieldDescriptorToBQ(fd protoreflect.FieldDescriptor, depth int) (*bigquery.FieldSchema, error) {
	if depth >= maxNestedRecordDepth {
		return nil, fmt.Errorf("proto field %q: nested RECORD depth exceeds %d (possible self-reference)", fd.Name(), maxNestedRecordDepth)
	}
	bqType, err := protoKindToBQFieldType(fd.Kind())
	if err != nil {
		return nil, fmt.Errorf("proto field %q: %w", fd.Name(), err)
	}
	fs := &bigquery.FieldSchema{
		Name:     string(fd.Name()),
		Type:     bqType,
		Required: false,
		Repeated: fd.Cardinality() == protoreflect.Repeated,
	}
	if fd.Kind() == protoreflect.MessageKind {
		nested, err := descriptorToBQSchemaAtDepth(fd.Message(), depth+1)
		if err != nil {
			return nil, err
		}
		fs.Schema = nested
	}
	return fs, nil
}

// descriptorToBQSchema converts a proto message descriptor to a BigQuery
// schema. All fields are created as NULLABLE; repeated fields become REPEATED.
// Returns an error if a field has no BigQuery mapping or if RECORD nesting
// would exceed maxNestedRecordDepth.
func descriptorToBQSchema(md protoreflect.MessageDescriptor) (bigquery.Schema, error) {
	return descriptorToBQSchemaAtDepth(md, 0)
}

func descriptorToBQSchemaAtDepth(md protoreflect.MessageDescriptor, depth int) (bigquery.Schema, error) {
	fields := md.Fields()
	schema := make(bigquery.Schema, 0, fields.Len())
	for i := range fields.Len() {
		fs, err := fieldDescriptorToBQ(fields.Get(i), depth)
		if err != nil {
			return nil, err
		}
		schema = append(schema, fs)
	}
	return schema, nil
}

// diffMissingColumns returns BQ field schemas for fields present in the proto
// descriptor but absent from the existing BQ schema.
func diffMissingColumns(md protoreflect.MessageDescriptor, existing bigquery.Schema) (bigquery.Schema, error) {
	existingNames := make(map[string]struct{}, len(existing))
	for _, f := range existing {
		existingNames[f.Name] = struct{}{}
	}

	var missing bigquery.Schema
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if _, ok := existingNames[string(fd.Name())]; ok {
			continue
		}
		fs, err := fieldDescriptorToBQ(fd, 0)
		if err != nil {
			return nil, err
		}
		missing = append(missing, fs)
	}
	return missing, nil
}

type schemaEvolver struct {
	log *service.Logger
}

// maxEvolveAttempts caps the CAS-on-412 retry loop so a pathologically busy
// table can't keep us spinning forever.
const maxEvolveAttempts = 5

// Evolve compares the message descriptor against the current BQ table schema
// and adds any missing columns under optimistic-locking via the table ETag.
// On HTTP 412 (a concurrent writer evolved the table first), it refetches and
// retries up to maxEvolveAttempts. Returns (true, nil) when the table now has
// every descriptor field (whether this call added them or another writer did),
// (false, nil) when the descriptor is already a subset of the table on the
// first attempt (so the SCHEMA_MISMATCH is not "extra fields"), or
// (false, err) on any other failure.
func (e *schemaEvolver) Evolve(ctx context.Context, client *bigquery.Client, datasetID, tableID string, md protoreflect.MessageDescriptor) (bool, error) {
	for attempt := range maxEvolveAttempts {
		meta, err := client.Dataset(datasetID).Table(tableID).Metadata(ctx)
		if err != nil {
			return false, fmt.Errorf("fetching table metadata for evolution: %w", err)
		}

		missing, err := diffMissingColumns(md, meta.Schema)
		if err != nil {
			return false, fmt.Errorf("computing missing columns: %w", err)
		}
		if len(missing) == 0 {
			// First attempt: the descriptor doesn't actually have any extra
			// columns, so the SCHEMA_MISMATCH wasn't due to additive evolution.
			// Later attempts: another writer added the columns we wanted; the
			// table is now correct, signal the caller to retry the write.
			return attempt > 0, nil
		}

		newSchema := make(bigquery.Schema, len(meta.Schema), len(meta.Schema)+len(missing))
		copy(newSchema, meta.Schema)
		newSchema = append(newSchema, missing...)

		// ETag enforces optimistic locking. On 412 we loop, refetch, and
		// recompute the diff so concurrent evolutions don't clobber each
		// other's added columns.
		_, err = client.Dataset(datasetID).Table(tableID).Update(ctx, bigquery.TableMetadataToUpdate{Schema: newSchema}, meta.ETag)
		if err == nil {
			for _, col := range missing {
				e.log.Infof("Added column %q (%s) to table %s.%s", col.Name, col.Type, datasetID, tableID)
			}
			return true, nil
		}

		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
			continue
		}
		return false, fmt.Errorf("updating table schema: %w", err)
	}
	return false, fmt.Errorf("schema evolution for %s.%s gave up after %d concurrent-update conflicts", datasetID, tableID, maxEvolveAttempts)
}
