// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

func registerSchemas(ctx context.Context, client *sr.Client) (map[SignalType]int, error) {
	commonRef, err := registerCommonProto(ctx, client)
	if err != nil {
		return nil, err
	}

	schemaIDs := make(map[SignalType]int, 3)

	{
		subject := "redpanda.otel.v1.Span"
		schema := sr.Schema{
			Schema:     spanProtoSchema,
			Type:       sr.TypeProtobuf,
			References: []sr.SchemaReference{commonRef},
		}
		ss, err := client.CreateSchema(ctx, subject, schema)
		if err != nil {
			return nil, fmt.Errorf("register schema for subject %s: %w", subject, err)
		}
		schemaIDs[SignalTypeTrace] = ss.ID
	}

	{
		subject := "redpanda.otel.v1.LogRecord"
		schema := sr.Schema{
			Schema:     logRecordProtoSchema,
			Type:       sr.TypeProtobuf,
			References: []sr.SchemaReference{commonRef},
		}
		ss, err := client.CreateSchema(ctx, subject, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to register schema for subject %s: %w", subject, err)
		}
		schemaIDs[SignalTypeLog] = ss.ID
	}

	{
		subject := "redpanda.otel.v1.Metric"
		schema := sr.Schema{
			Schema:     metricProtoSchema,
			Type:       sr.TypeProtobuf,
			References: []sr.SchemaReference{commonRef},
		}
		ss, err := client.CreateSchema(ctx, subject, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to register schema for subject %s: %w", subject, err)
		}
		schemaIDs[SignalTypeMetric] = ss.ID
	}

	return schemaIDs, nil
}

func registerCommonProto(ctx context.Context, client *sr.Client) (sr.SchemaReference, error) {
	subject := "redpanda.otel.v1.common"
	schema := sr.Schema{
		Schema: commonProtoSchema,
		Type:   sr.TypeProtobuf,
	}
	ss, err := client.CreateSchema(ctx, subject, schema)
	if err != nil {
		return sr.SchemaReference{}, fmt.Errorf("register common schema for subject %s: %w", subject, err)
	}

	return sr.SchemaReference{
		Name:    "redpanda/otel/v1/common.proto",
		Subject: "redpanda.otel.v1.common",
		Version: ss.Version,
	}, nil
}
