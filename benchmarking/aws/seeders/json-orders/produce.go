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
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fixedOverhead is the approximate byte cost of the id/ts/region/amount/
// status fields plus JSON punctuation, subtracted from a target row size to
// derive the payload padding length.
const fixedOverhead = 120

// charset and poolSize back the shared random payload pool sampled by both
// seed and workload. See payloadPool for the entropy rationale.
const (
	charset  = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	poolSize = 16 << 20 // 16 MiB
)

// regions and statuses vary the low-cardinality fields so the record shape
// resembles real order data rather than constants. Shared by seed and
// workload.
var (
	regions  = []string{"us-east-1", "us-east-2", "us-west-2", "eu-west-1", "ap-south-1"}
	statuses = []string{"NEW", "PAID", "SHIPPED", "CANCELLED", "REFUNDED"}
)

// payloadPool is a large precomputed block of random alphanumeric bytes that
// records sample fixed-size random windows from.
//
// Realistic, high-entropy payload. Repetitive padding (e.g. all 'x') used to
// compress ~150x in Parquet, which made committed-bytes/sec meaningless and
// gave the Kafka Connect comparison an absurd ~7 B/record. Sampling each
// record's payload from a large pool of random bytes instead (JSON-safe, no
// escaping surprises) keeps payloads distinct and barely compressible, so
// Parquet/S3 file sizes become representative and the MB/s axis symmetric
// across engines. A precomputed pool + random window keeps the hot path fast
// (no per-byte RNG per record).
//
// The pool's bytes are immutable once built, so sharing one *payloadPool
// across goroutines is safe; only the rng used to pick a sample window must
// stay per-goroutine (rand.Rand mutates its own state on every call).
type payloadPool struct {
	data []byte
}

func newPayloadPool(rng *rand.Rand) *payloadPool {
	data := make([]byte, poolSize)
	for i := range data {
		data[i] = charset[rng.Intn(len(charset))]
	}
	return &payloadPool{data: data}
}

// sample returns a padLen-byte random window of the pool, or "" if padLen is
// non-positive or the pool is too small to contain a window that size.
func (p *payloadPool) sample(rng *rand.Rand, padLen int) string {
	if padLen <= 0 || len(p.data) <= padLen {
		return ""
	}
	off := rng.Intn(len(p.data) - padLen)
	return string(p.data[off : off+padLen])
}

// rowPadLen derives the payload padding length from a target row size,
// clamped to zero.
func rowPadLen(rowSize int) int {
	padLen := rowSize - fixedOverhead
	if padLen < 0 {
		return 0
	}
	return padLen
}

// buildRecord assembles one flat order record. id is the record's own
// identifier (may recur under a bounded key space, see seed's keySpace);
// varySeed drives the low-cardinality field selection (region/status/amount)
// independently of id, so a recurring id still carries a distinct row image
// each time it appears. ts is always the current time, so consecutive
// records are never byte-identical even when id and varySeed repeat.
func buildRecord(pool *payloadPool, rng *rand.Rand, id, varySeed int64, padLen int) map[string]any {
	return map[string]any{
		"id":      id,
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"region":  regions[varySeed%int64(len(regions))],
		"amount":  float64(varySeed%100000) / 100.0,
		"status":  statuses[varySeed%int64(len(statuses))],
		"payload": pool.sample(rng, padLen),
	}
}

// seed produces `rows` flat JSON records (~rowSize bytes each) into `topic`.
// Brokers come from REDPANDA_BROKERS (comma-separated host:port).
//
// keySpace > 0 caps the id space so ids cycle (id = i % keySpace), giving
// keyed-upsert benches genuine key collisions; 0 keeps ids unique (the
// historical behaviour). Non-key fields (ts, payload, amount) still vary per
// record, so a recurring id carries a distinct row image each time — the
// shape an upsert actually sees.
//
// keyOrder controls how a bounded key space is walked. "sequential" is the
// plain i % keySpace: ids arrive in contiguous runs, so a batch's keys
// cluster into few data files (copy-on-write's best case). "scattered" walks
// the space by a stride coprime to keySpace — still a permutation, so every
// id is hit exactly once per cycle, but consecutive rows carry far-apart ids
// and a batch's keys spray across all files (the realistic CDC worst case).
func seed(ctx context.Context, topic string, rows int64, rowSize, partitions int, keySpace int64, keyOrder string) error {
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
	// 90 x 5s = 7.5 min: the brokers' cloud-init takes several minutes after
	// terraform apply, and a warm build cache can get the seed here ~90s
	// after apply — the old 30-attempt (2.5 min) window then expires before
	// the brokers listen (hit live 2026-08-24: "connection refused", run
	// aborted at seed).
	adm := kadm.NewClient(cl)
	var lastErr error
	for attempt := 1; attempt <= 90; attempt++ {
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

	padLen := rowPadLen(rowSize)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	pool := newPayloadPool(rng)

	// Scattered order walks the key space by a stride coprime to keySpace:
	// (pos * stride) % keySpace is then a permutation of [0, keySpace), so
	// every id still recurs exactly once per cycle — only the arrival order
	// changes. Start from a prime and bump until coprime so any keySpace
	// works; deterministic so every cycle revisits the identical key set.
	var stride int64
	if keySpace > 0 && keyOrder == "scattered" {
		stride = 1_000_003
		for gcd(stride, keySpace) != 1 {
			stride += 2
		}
	}

	var produced, failed int64
	var firstErr atomic.Value // stores error
	for i := int64(0); i < rows; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		id := i
		if keySpace > 0 {
			id = i % keySpace
			if stride > 0 {
				id = (id * stride) % keySpace
			}
		}
		rec := buildRecord(pool, rng, id, i, padLen)
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

// gcd is Euclid's algorithm, used to pick a stride coprime to the key space.
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// workload produces to `topic` at a steady `rate` writes/sec (total, across
// all workers) for `dur`, then returns. Unlike seed's fixed-count backlog, this
// keeps feeding the topic for the whole duration, so a sink benched against it
// is always draining a live feed rather than a static, pre-seeded dataset —
// see the live-stream scenarios in scenarios/s3.
//
// Modeled on cdc-rows-postgres' workload: 16 workers each drive their own
// 100ms ticker so the per-tick record count stays small (a single goroutine
// driving one large per-tick batch caps out on serialization + one network
// RTT per tick well below realistic target rates). Workers exit cleanly when
// ctx is cancelled or dur elapses; ctx.Canceled/DeadlineExceeded are treated
// as expected shutdown, not failures.
func workload(ctx context.Context, topic string, rate, rowSize int, dur time.Duration) error {
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

	padLen := rowPadLen(rowSize)
	pool := newPayloadPool(rand.New(rand.NewSource(time.Now().UnixNano())))

	const workers = 16
	perWorkerPer100ms := rate / workers / 10
	if perWorkerPer100ms < 1 {
		perWorkerPer100ms = 1
	}
	deadline := time.Now().Add(dur)

	var (
		produced, failed int64
		firstErr         atomic.Value // stores error
		recordSeq        int64        // shared across workers so ids/vary-seeds never collide
	)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerIdx := int64(w)
		go func() {
			defer wg.Done()
			// Each worker gets its own rng: rand.Rand mutates its own state on
			// every call, so sharing one across goroutines would race. The
			// pool's underlying bytes are read-only after construction and
			// safe to share.
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + workerIdx))
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case <-ticker.C:
					if time.Now().After(deadline) {
						errCh <- nil
						return
					}
					for n := 0; n < perWorkerPer100ms; n++ {
						seq := atomic.AddInt64(&recordSeq, 1) - 1
						rec := buildRecord(pool, rng, seq, seq, padLen)
						val, err := json.Marshal(rec)
						if err != nil {
							errCh <- err
							return
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
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if f := atomic.LoadInt64(&failed); f > 0 {
		p := atomic.LoadInt64(&produced)
		if e, ok := firstErr.Load().(error); ok && e != nil {
			return fmt.Errorf("%d produced, %d failed to produce; first error: %w", p, f, e)
		}
		return fmt.Errorf("%d produced, %d failed to produce (no error captured)", p, f)
	}
	fmt.Printf("json-orders: produced %d records to %s (workload)\n", atomic.LoadInt64(&produced), topic)
	return nil
}
