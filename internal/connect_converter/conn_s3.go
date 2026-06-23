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
	registerConnector("io.confluent.connect.s3.S3SinkConnector", s3SinkConnector{})
	// Aiven S3 sink connector class names.
	registerConnector("io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector", s3SinkConnector{})
	registerConnector("io.aiven.kafka.connect.s3.S3SinkConnector", s3SinkConnector{})
}

type s3SinkConnector struct{}

func (s3SinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("s3.bucket.name"); ok {
		kv(body, "bucket", scalar(v))
	} else if v, ok := ctx.String("aws.s3.bucket.name"); ok {
		// Aiven S3 sink uses aws.s3.bucket.name instead of s3.bucket.name.
		kv(body, "bucket", scalar(v))
	} else {
		ctx.Warn("s3.bucket.name", "missing required bucket name")
		stub := scalar("")
		stub.LineComment = "TODO: set the S3 bucket name"
		kv(body, "bucket", stub)
	}

	if v, ok := ctx.String("s3.region"); ok {
		kv(body, "region", scalar(v))
	} else if v, ok := ctx.String("aws.s3.region"); ok {
		kv(body, "region", scalar(v))
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

	// Explicit static AWS credentials, if provided.
	if id, ok := ctx.String("aws.access.key.id"); ok {
		creds := mapping()
		kv(creds, "id", scalar(id))
		if secret, ok := ctx.String("aws.secret.access.key"); ok {
			kv(creds, "secret", scalar(secret))
		}
		kv(body, "credentials", creds)
	} else {
		ctx.consume("aws.secret.access.key")
	}

	// Recognized KC plumbing with no RPCN equivalent — drop quietly.
	consumeIgnored(ctx,
		"storage.class",
		"schema.generator.class",
		"schema.compatibility",
		"s3.part.size",
		"s3.compression.type",
		"parquet.codec",
		"file.name.template",
		"file.name.prefix",
		"file.name.timestamp.source",
		"file.name.timestamp.timezone",
		"format.output.fields",
		"format.output.fields.value.encoding",
	)
	// DefaultPartitioner matches RPCN's topic-based path; Time/Daily/Hourly
	// partitioners become a time-bucketed path prefix; FieldPartitioner becomes
	// a field-value prefix; any other partitioner is left as a TODO.
	if p, ok := ctx.Lookup("partitioner.class"); ok {
		switch {
		case strings.Contains(p, "DefaultPartitioner"):
			ctx.consume("partitioner.class")
		case strings.Contains(p, "HourlyPartitioner"):
			applyTimeBasedPartitioner(ctx, path, "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH")
		case strings.Contains(p, "DailyPartitioner"):
			applyTimeBasedPartitioner(ctx, path, "'year'=YYYY/'month'=MM/'day'=dd")
		case strings.Contains(p, "TimeBasedPartitioner"):
			applyTimeBasedPartitioner(ctx, path, "")
		case strings.Contains(p, "FieldPartitioner"):
			applyFieldPartitioner(ctx, path)
		}
	}

	return Component{Output: component("aws_s3", body)}, nil
}
