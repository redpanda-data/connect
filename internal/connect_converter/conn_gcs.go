// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"strings"
)

func init() {
	registerConnector("io.confluent.connect.gcs.GcsSinkConnector", gcsSinkConnector{})
	// Aiven GCS sink connector class name.
	registerConnector("io.aiven.kafka.connect.gcs.GcsSinkConnector", gcsSinkConnector{})
}

type gcsSinkConnector struct{}

func (gcsSinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("gcs.bucket.name"); ok {
		kv(body, "bucket", scalar(v))
	} else {
		ctx.Warn("gcs.bucket.name", "missing required bucket name")
		stub := scalar("")
		stub.LineComment = "TODO: set the GCS bucket name"
		kv(body, "bucket", stub)
	}

	// Build an object path from the source topic. KC routes by topic; RPCN
	// uses interpolation on the kafka_topic metadata. The file extension is
	// derived from format.class; an optional topics.dir becomes a prefix.
	ctx.consume("topics")
	ext := objectFormatExtension(ctx) // consumes format.class / format.output.type
	path := topicObjectPath(ext)
	if dir, ok := ctx.String("topics.dir"); ok && dir != "" {
		path.Value = strings.TrimSuffix(dir, "/") + "/" + path.Value
	}
	if ext == ".avro" || ext == ".parquet" {
		path.LineComment = "TODO: add an encode step (e.g. avro/parquet) before this output"
	}
	// Aiven file.compression.type appends a suffix and a compress-processor TODO.
	applyObjectCompression(ctx, path, ext)
	kv(body, "path", path)

	// flush.size / file.max.records (count) + rotation interval (period) ->
	// common batch policy (single block).
	mapBatching(body, ctx, []string{"flush.size", "file.max.records"}, "", "rotate.interval.ms", "rotate.schedule.interval.ms")

	// Credentials: gcs.credentials.json contains inline JSON that maps
	// directly to credentials_json. gcs.credentials.path is a file path which
	// has no direct RPCN equivalent — leave as TODO.
	if v, ok := ctx.String("gcs.credentials.json"); ok {
		kv(body, "credentials_json", scalar(v))
	}
	if _, ok := ctx.Lookup("gcs.credentials.path"); ok {
		ctx.consume("gcs.credentials.path")
		// TODO: gcs.credentials.path is a file path; credentials_json expects
		// the JSON content inline — load the file manually if needed.
	}

	// Recognized KC plumbing with no RPCN equivalent — drop quietly.
	consumeIgnored(ctx,
		"gcs.part.size",
		"storage.class",
		"schema.generator.class",
		"schema.compatibility",
	)
	// DefaultPartitioner matches RPCN's topic-based path; TimeBasedPartitioner
	// is translated into a time-bucketed path prefix; any other partitioner
	// changes the layout in a way we cannot map, so leave it to surface as a
	// TODO for manual review.
	if p, ok := ctx.Lookup("partitioner.class"); ok {
		switch {
		case strings.Contains(p, "DefaultPartitioner"):
			ctx.consume("partitioner.class")
		case strings.Contains(p, "TimeBasedPartitioner"):
			applyTimeBasedPartitioner(ctx, path)
		}
	}

	return Component{Output: component("gcp_cloud_storage", body)}, nil
}
