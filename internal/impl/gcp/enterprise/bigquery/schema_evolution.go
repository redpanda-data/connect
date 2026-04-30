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

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// protoKindToBQFieldType maps a protobuf field kind to a BigQuery field type.
func protoKindToBQFieldType(kind protoreflect.Kind) bigquery.FieldType {
	switch kind {
	case protoreflect.StringKind:
		return bigquery.StringFieldType
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Uint32Kind,
		protoreflect.Fixed64Kind, protoreflect.Fixed32Kind:
		return bigquery.IntegerFieldType
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		return bigquery.FloatFieldType
	case protoreflect.BoolKind:
		return bigquery.BooleanFieldType
	case protoreflect.BytesKind:
		return bigquery.BytesFieldType
	case protoreflect.EnumKind:
		return bigquery.StringFieldType
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return bigquery.RecordFieldType
	default:
		return bigquery.StringFieldType
	}
}

// descriptorToBQSchema converts a proto message descriptor to a BigQuery
// schema. All fields are created as NULLABLE.
func descriptorToBQSchema(md protoreflect.MessageDescriptor) bigquery.Schema {
	fields := md.Fields()
	schema := make(bigquery.Schema, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		fs := &bigquery.FieldSchema{
			Name:     string(fd.Name()),
			Type:     protoKindToBQFieldType(fd.Kind()),
			Required: false,
		}
		if fd.Kind() == protoreflect.MessageKind {
			fs.Schema = descriptorToBQSchema(fd.Message())
		}
		schema = append(schema, fs)
	}
	return schema
}

// diffMissingColumns returns BQ field schemas for fields present in the proto
// descriptor but absent from the existing BQ schema.
func diffMissingColumns(md protoreflect.MessageDescriptor, existing bigquery.Schema) bigquery.Schema {
	existingNames := make(map[string]struct{}, len(existing))
	for _, f := range existing {
		existingNames[f.Name] = struct{}{}
	}

	var missing bigquery.Schema
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		if _, ok := existingNames[name]; ok {
			continue
		}
		fs := &bigquery.FieldSchema{
			Name:     name,
			Type:     protoKindToBQFieldType(fd.Kind()),
			Required: false,
		}
		if fd.Kind() == protoreflect.MessageKind {
			fs.Schema = descriptorToBQSchema(fd.Message())
		}
		missing = append(missing, fs)
	}
	return missing
}

type schemaEvolver struct {
	bqClient  *bigquery.Client
	datasetID string
	log       *service.Logger
}

// Evolve compares the message descriptor against the current BQ table schema
// and adds any missing columns. Returns (true, nil) when columns were added,
// (false, nil) when the schema already matches, or (false, err) on failure.
func (e *schemaEvolver) Evolve(ctx context.Context, tableID string, md protoreflect.MessageDescriptor) (bool, error) {
	meta, err := e.bqClient.Dataset(e.datasetID).Table(tableID).Metadata(ctx)
	if err != nil {
		return false, fmt.Errorf("fetching table metadata for evolution: %w", err)
	}

	missing := diffMissingColumns(md, meta.Schema)
	if len(missing) == 0 {
		return false, nil
	}

	newSchema := make(bigquery.Schema, len(meta.Schema), len(meta.Schema)+len(missing))
	copy(newSchema, meta.Schema)
	newSchema = append(newSchema, missing...)

	update := bigquery.TableMetadataToUpdate{Schema: newSchema}
	if _, err := e.bqClient.Dataset(e.datasetID).Table(tableID).Update(ctx, update, meta.ETag); err != nil {
		return false, fmt.Errorf("updating table schema: %w", err)
	}

	for _, col := range missing {
		e.log.Infof("Added column %q (%s) to table %s.%s", col.Name, col.Type, e.datasetID, tableID)
	}
	return true, nil
}

// CreateTable creates a new BigQuery table from a proto message descriptor.
// All columns are NULLABLE. Returns nil if the table already exists.
func (e *schemaEvolver) CreateTable(ctx context.Context, tableID string, md protoreflect.MessageDescriptor) error {
	schema := descriptorToBQSchema(md)
	if len(schema) == 0 {
		return fmt.Errorf("cannot create table %q: descriptor has no fields", tableID)
	}

	err := e.bqClient.Dataset(e.datasetID).Table(tableID).Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	})
	if err != nil {
		if hasStatusCode(err, 409) {
			return nil
		}
		if hasStatusCode(err, 403) {
			return fmt.Errorf("creating table %s.%s: permission denied (ensure the service account has bigquery.tables.create on dataset %s): %w",
				e.datasetID, tableID, e.datasetID, err)
		}
		if hasStatusCode(err, 404) {
			return fmt.Errorf("creating table %s.%s: dataset %s does not exist: %w",
				e.datasetID, tableID, e.datasetID, err)
		}
		return fmt.Errorf("creating table %s.%s: %w", e.datasetID, tableID, err)
	}
	e.log.Infof("Created table %s.%s with %d columns", e.datasetID, tableID, len(schema))
	return nil
}

// hasStatusCode checks if an error contains a Google API error with the given
// HTTP status code.
func hasStatusCode(err error, code int) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == code
}
