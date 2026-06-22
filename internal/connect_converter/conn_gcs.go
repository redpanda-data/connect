// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

func init() {
	registerConnector("io.confluent.connect.gcs.GcsSinkConnector", gcsSinkConnector{})
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

	ctx.consume("topics")
	path := scalar(`${! @kafka_topic }/${! timestamp_unix() }-${! uuid_v4() }.json`)
	path.LineComment = "TODO: review object path/partitioning"
	kv(body, "path", path)

	return Component{Output: component("gcp_cloud_storage", body)}, nil
}
