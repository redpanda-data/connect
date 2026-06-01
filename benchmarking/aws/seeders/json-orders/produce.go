// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// seed produces `rows` flat JSON records (~rowSize bytes each) into `topic`.
// Brokers come from REDPANDA_BROKERS (comma-separated host:port).
func seed(ctx context.Context, topic string, rows int64, rowSize int) error {
	brokers := os.Getenv("REDPANDA_BROKERS")
	if brokers == "" {
		return fmt.Errorf("REDPANDA_BROKERS env var is required")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(16<<20),
		kgo.MaxBufferedRecords(200_000),
	)
	if err != nil {
		return fmt.Errorf("kgo client: %w", err)
	}
	defer cl.Close()

	const fixedOverhead = 120 // id/ts/region/amount/status + JSON punctuation
	padLen := rowSize - fixedOverhead
	if padLen < 0 {
		padLen = 0
	}
	pad := strings.Repeat("x", padLen)

	var produced, failed int64
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
			} else {
				atomic.AddInt64(&produced, 1)
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if f := atomic.LoadInt64(&failed); f > 0 {
		return fmt.Errorf("%d records failed to produce", f)
	}
	fmt.Printf("json-orders: produced %d records to %s\n", atomic.LoadInt64(&produced), topic)
	return nil
}
