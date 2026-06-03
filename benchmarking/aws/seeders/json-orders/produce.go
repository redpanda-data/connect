// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// seed produces `rows` flat JSON records (~rowSize bytes each) into `topic`.
// Brokers come from REDPANDA_BROKERS (comma-separated host:port).
func seed(ctx context.Context, topic string, rows int64, rowSize, partitions int) error {
	brokers := os.Getenv("REDPANDA_BROKERS")
	if brokers == "" {
		return fmt.Errorf("REDPANDA_BROKERS env var is required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerBatchMaxBytes(16<<20),
		kgo.MaxBufferedRecords(200_000),
	)
	if err != nil {
		return fmt.Errorf("kgo client: %w", err)
	}
	defer cl.Close()

	// Explicitly create the topic before producing. Redpanda's broker-side
	// auto_create_topics_enabled is not sufficient — franz-go must request it,
	// and pre-creating lets us set the partition count for sink parallelism.
	//
	// max.message.bytes must clear the producer's ProducerBatchMaxBytes (16
	// MiB above): the broker validates each produce batch against the topic's
	// max.message.bytes, and the broker default (~1 MiB) is far below our
	// batch cap. Without this, batches that fill toward 16 MiB are rejected
	// with MESSAGE_TOO_LARGE — a rare spike at low volume (the 12M smoke
	// slipped by) but a hard failure when seeding 160M rows. 64 MiB gives the
	// 16 MiB batches 4x headroom.
	maxMsgBytes := "67108864" // 64 MiB
	topicConfigs := map[string]*string{"max.message.bytes": &maxMsgBytes}
	adm := kadm.NewClient(cl)
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		resp, err := adm.CreateTopics(ctx, int32(partitions), int16(3), topicConfigs, topic)
		if err == nil {
			if t, ok := resp[topic]; ok && t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
				lastErr = fmt.Errorf("create topic %q: %w", topic, t.Err)
			} else {
				lastErr = nil
				break
			}
		} else {
			lastErr = fmt.Errorf("create topic %q (attempt %d): %w", topic, attempt, err)
		}
		log.Printf("json-orders: waiting for brokers/topic (attempt %d): %v", attempt, lastErr)
		time.Sleep(5 * time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("topic not ready after retries: %w", lastErr)
	}
	log.Printf("json-orders: topic %q ready (%d partitions); producing %d records", topic, partitions, rows)

	const fixedOverhead = 120 // id/ts/region/amount/status + JSON punctuation
	padLen := rowSize - fixedOverhead
	if padLen < 0 {
		padLen = 0
	}
	pad := strings.Repeat("x", padLen)

	var produced, failed int64
	var firstErr atomic.Value // stores error
	for i := int64(0); i < rows; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rec := map[string]any{
			"id":      i,
			"ts":      time.Now().UTC().Format(time.RFC3339Nano),
			"region":  "us-east-2",
			"amount":  float64(i%100000) / 100.0,
			"status":  "NEW",
			"payload": pad,
		}
		val, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		cl.Produce(ctx, &kgo.Record{Value: val}, func(_ *kgo.Record, err error) {
			if err != nil {
				atomic.AddInt64(&failed, 1)
				firstErr.CompareAndSwap(nil, err)
			} else {
				atomic.AddInt64(&produced, 1)
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if f := atomic.LoadInt64(&failed); f > 0 {
		if e, ok := firstErr.Load().(error); ok && e != nil {
			return fmt.Errorf("%d/%d records failed to produce; first error: %w", f, rows, e)
		}
		return fmt.Errorf("%d/%d records failed to produce (no error captured)", f, rows)
	}
	fmt.Printf("json-orders: produced %d records to %s\n", atomic.LoadInt64(&produced), topic)
	return nil
}
