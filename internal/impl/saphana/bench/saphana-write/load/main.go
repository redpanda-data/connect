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
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/SAP/go-hdb/driver"
	"github.com/twmb/franz-go/pkg/kgo"
)

type orderMsg struct {
	MsgID     int     `json:"msg_id"`
	UserID    int     `json:"user_id"`
	ProductID int     `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
	Status    string  `json:"status"`
	Notes     string  `json:"notes"`
}

func main() {
	setupOnly := flag.Bool("setup", false, "create BENCH_WRITES table in HANA and exit")
	count := flag.Int("count", 100000, "number of messages to produce to Kafka")
	batchSize := flag.Int("batch", 1000, "Kafka produce batch size")
	brokers := flag.String("brokers", "localhost:9092", "Kafka bootstrap brokers (comma-separated)")
	topic := flag.String("topic", "rpcns-hana-bench-write", "Kafka topic to produce to")
	flag.Parse()

	if *setupOnly {
		dsn := requireEnv("HANA_DSN")
		schema := requireEnv("HANA_SCHEMA")
		setupTable(dsn, schema)
		return
	}

	produceToKafka(*topic, *brokers, *count, *batchSize)
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s environment variable is required\n", key)
		os.Exit(1)
	}
	return v
}

func tableRef(schema string) string {
	return fmt.Sprintf(`"%s"."BENCH_WRITES"`, schema)
}

func setupTable(dsn, schema string) {
	ctx := context.Background()
	db, err := sql.Open("hdb", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping: %v\n", err)
		os.Exit(1)
	}

	ref := tableRef(schema)
	_, _ = db.ExecContext(ctx, `DROP TABLE `+ref)
	createSQL := `CREATE COLUMN TABLE ` + ref + ` (` +
		`MSG_ID INTEGER PRIMARY KEY,` +
		`USER_ID INTEGER,` +
		`PRODUCT_ID INTEGER,` +
		`QUANTITY INTEGER,` +
		`PRICE DOUBLE,` +
		`STATUS NVARCHAR(20),` +
		`NOTES NVARCHAR(200)` +
		`)`
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		fmt.Fprintf(os.Stderr, "create table: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Table %s created\n", ref)
}

func produceToKafka(topic, brokers string, count, batchSize int) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka client: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	statuses := []string{"pending", "confirmed", "shipped"}

	var produced atomic.Int64
	start := time.Now()

	stopProg := make(chan struct{})
	go func() {
		prev := int64(0)
		prevT := start
		for {
			select {
			case <-stopProg:
				return
			case <-time.After(500 * time.Millisecond):
				now := time.Now()
				cur := produced.Load()
				rate := float64(cur-prev) / now.Sub(prevT).Seconds()
				fmt.Printf("\r%d / %d  (%.0f msg/s)", cur, count, rate)
				prev = cur
				prevT = now
			}
		}
	}()

	records := make([]*kgo.Record, 0, batchSize)
	for i := 0; i < count; i++ {
		msg := orderMsg{
			MsgID:     i + 1,
			UserID:    rng.Intn(100000) + 1,
			ProductID: rng.Intn(10000) + 1,
			Quantity:  rng.Intn(10) + 1,
			Price:     1.0 + rng.Float64()*999.0,
			Status:    statuses[rng.Intn(3)],
			Notes:     fmt.Sprintf("note_%d note_%d", i+1, i+1),
		}
		b, _ := json.Marshal(msg)
		records = append(records, &kgo.Record{Value: b})

		if len(records) >= batchSize {
			if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
				fmt.Fprintf(os.Stderr, "\nproduce: %v\n", err)
				os.Exit(1)
			}
			produced.Add(int64(len(records)))
			records = records[:0]
		}
	}
	if len(records) > 0 {
		if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
			fmt.Fprintf(os.Stderr, "\nproduce: %v\n", err)
			os.Exit(1)
		}
		produced.Add(int64(len(records)))
	}

	close(stopProg)
	elapsed := time.Since(start)
	total := int(produced.Load())
	fmt.Printf("\r%d / %d  (done)                    \n", total, count)
	fmt.Printf("Produced %d messages in %s (%.0f msg/s)\n", total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
}
