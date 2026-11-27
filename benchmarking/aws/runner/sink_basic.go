// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Destination topic shape for the redpanda sink: the same as the stack-none
// source topic (producer.go), so a point's writes fit retention.
const (
	dstPartitions     = sourcePartitions
	dstReplicas       = sourceReplicas
	dstRetentionBytes = sourceRetentionBytes
	dstRetentionMs    = sourceRetentionMs
)

// redpandaSink writes to bench_<session>_dst on the bench brokers and counts
// records by the topic's summed high watermarks.
type redpandaSink struct {
	brokers string
	// providedBrokers is set when the outs already carry
	// BENCH_REDPANDA_BROKERS, so Provision leaves it alone.
	providedBrokers bool
	rpk             RPKRunner
	// topic is the run's topic, from SinkDeps.SessionID; Count reads it.
	topic string
}

func newRedpandaSink(deps SinkDeps) (*redpandaSink, error) {
	brokers, err := sourceBrokers(deps.Outs)
	if err != nil {
		return nil, fmt.Errorf("redpanda sink: %w", err)
	}
	if deps.RPK == nil {
		return nil, errors.New("redpanda sink: no rpk runner")
	}
	_, provided := deps.Outs["bench_redpanda_brokers"]
	return &redpandaSink{brokers: brokers, providedBrokers: provided, rpk: deps.RPK, topic: dstTopic(deps.SessionID)}, nil
}

// dstTopic is the redpanda sink's per-run topic: bench_<session>_dst.
func dstTopic(session string) string {
	return "bench_" + scratchName(session) + "_dst"
}

func (r *redpandaSink) Provision(ctx context.Context, session string) (map[string]string, error) {
	topic := dstTopic(session)
	r.topic = topic
	out, err := r.rpk.RunRPK(ctx, r.brokers, "topic", "create", topic,
		"-p", strconv.Itoa(dstPartitions), "-r", strconv.Itoa(dstReplicas),
		"-c", "retention.bytes="+strconv.FormatInt(dstRetentionBytes, 10),
		"-c", "retention.ms="+strconv.Itoa(dstRetentionMs))
	if err != nil {
		return nil, fmt.Errorf("create topic %s: %w: %s", topic, err, strings.Join(out, "\n"))
	}
	ph := map[string]string{"BENCH_DST_TOPIC": topic}
	if !r.providedBrokers {
		ph["BENCH_REDPANDA_BROKERS"] = r.brokers
	}
	return ph, nil
}

// Count is the sum of the topic's high watermarks. The topic is never
// compacted, so the delta over a point is the records written in it.
func (r *redpandaSink) Count(ctx context.Context, _ string) (int64, error) {
	topic := r.topic
	out, err := r.rpk.RunRPK(ctx, r.brokers, "topic", "describe", "-p", topic)
	if err != nil {
		return 0, fmt.Errorf("describe topic %s: %w: %s", topic, err, strings.Join(out, "\n"))
	}
	parts, err := parseTopicPartitions(out)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, p := range parts {
		n += p.HighWatermark
	}
	return n, nil
}

func (r *redpandaSink) Teardown(ctx context.Context, session string) error {
	topic := dstTopic(session)
	out, err := r.rpk.RunRPK(ctx, r.brokers, "topic", "delete", topic)
	if err == nil {
		return nil
	}
	for _, l := range out {
		if strings.Contains(l, "UNKNOWN_TOPIC_OR_PARTITION") || strings.Contains(strings.ToLower(l), "not found") {
			return nil // already gone
		}
	}
	return fmt.Errorf("delete topic %s: %w: %s", topic, err, strings.Join(out, "\n"))
}

// s3Sink writes under sink/<session>/ in the session results bucket. Its
// object count times the batch size would only equal the record count with
// a fixed batching.count, so Count is unsupported. Teardown is a no-op: the
// results bucket is force_destroy and goes with the shared stack.
type s3Sink struct {
	bucket string
	region string
}

func newS3Sink(deps SinkDeps) (*s3Sink, error) {
	b := deps.Outs["results_bucket"]
	if b == "" {
		return nil, errors.New("s3 sink: terraform output results_bucket is missing or empty")
	}
	return &s3Sink{bucket: b, region: deps.Region}, nil
}

func (s *s3Sink) Provision(_ context.Context, session string) (map[string]string, error) {
	return map[string]string{
		"BENCH_S3_BUCKET":  s.bucket,
		"BENCH_S3_PREFIX":  "sink/" + session + "/",
		"BENCH_AWS_REGION": s.region,
	}, nil
}

func (*s3Sink) Count(context.Context, string) (int64, error) { return 0, ErrCountUnsupported }

func (*s3Sink) Teardown(context.Context, string) error { return nil }

// dropSink discards everything: no scratch, nothing to count.
type dropSink struct{}

func (dropSink) Provision(context.Context, string) (map[string]string, error) { return nil, nil }

func (dropSink) Count(context.Context, string) (int64, error) { return 0, ErrCountUnsupported }

func (dropSink) Teardown(context.Context, string) error { return nil }
