// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

func init() {
	registerConnector("io.confluent.connect.s3.S3SinkConnector", s3SinkConnector{})
}

type s3SinkConnector struct{}

func (s3SinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("s3.bucket.name"); ok {
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
	// uses interpolation on the kafka_topic metadata.
	ctx.consume("topics")
	kv(body, "path", topicObjectPath())

	return Component{Output: component("aws_s3", body)}, nil
}
