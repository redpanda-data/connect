/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package testing_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	streamtesting "github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming/testing"
)

// generateTestBatch creates a batch of messages with realistic JSON data
func generateTestBatch(size int) service.MessageBatch {
	batch := make(service.MessageBatch, size)
	for i := range size {
		data := fmt.Sprintf(`{
			"A": "row_%d",
			"B": %t,
			"C": {"id": %d, "data": "value_%d"},
			"D": [%d, %d, %d],
			"E": {"nested": "object_%d", "count": %d},
			"F": %f,
			"G": %d
		}`, i, i%2 == 0, i, i, i, i+1, i+2, i, i, float64(i)*3.14, i*42)
		batch[i] = service.NewMessage([]byte(data))
		_, _ = batch[i].AsStructured()
	}
	return batch
}

// BenchmarkParquetConstruction benchmarks parallel parquet file construction with various configurations
func BenchmarkParquetConstruction(b *testing.B) {
	env := streamtesting.Setup(&testing.T{})
	ctx := context.Background()

	privateKey := streamtesting.GenerateTestPrivateKey(&testing.T{})

	// Create service client
	serviceClient, err := streaming.NewSnowflakeServiceClient(ctx, streaming.ClientOptions{
		Account:        "test_account",
		URL:            env.Server.URL(),
		User:           "test_user",
		Role:           "TESTROLE",
		PrivateKey:     privateKey,
		Logger:         streamtesting.GetLogger(&testing.T{}),
		ConnectVersion: "1.0.0",
	})
	require.NoError(b, err)
	defer serviceClient.Close()

	// Test configurations: batch size, chunk size, parallelism
	benchmarks := []struct {
		name        string
		batchSize   int
		chunkSize   int
		parallelism int
	}{
		{"1K_rows_1_worker", 1000, 50000, 1},
		{"1K_rows_2_workers", 1000, 500, 2},
		{"1K_rows_4_workers", 1000, 250, 4},
		{"10K_rows_1_worker", 10000, 50000, 1},
		{"10K_rows_2_workers", 10000, 5000, 2},
		{"10K_rows_4_workers", 10000, 2500, 4},
		{"10K_rows_8_workers", 10000, 1250, 8},
		{"50K_rows_1_worker", 50000, 50000, 1},
		{"50K_rows_2_workers", 50000, 25000, 2},
		{"50K_rows_4_workers", 50000, 12500, 4},
		{"50K_rows_8_workers", 50000, 6250, 8},
		{"100K_rows_1_worker", 100000, 50000, 1},
		{"100K_rows_4_workers", 100000, 25000, 4},
		{"100K_rows_8_workers", 100000, 12500, 8},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Open a channel with specific build options
			channelOpts := streaming.ChannelOptions{
				Name:         "benchmark_channel_" + bm.name,
				DatabaseName: "TEST_DB",
				SchemaName:   "PUBLIC",
				TableName:    "TEST_TABLE",
				BuildOptions: streaming.BuildOptions{
					Parallelism: bm.parallelism,
					ChunkSize:   bm.chunkSize,
				},
			}

			channel, err := serviceClient.OpenChannel(ctx, channelOpts)
			require.NoError(b, err)

			// Generate test data once
			batch := generateTestBatch(bm.batchSize)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				stats, err := channel.InsertRows(ctx, batch, nil)
				if err != nil {
					b.Fatalf("InsertRows failed: %v", err)
				}

				// Report detailed timing
				if i == 0 {
					b.ReportMetric(float64(stats.ConvertTime.Microseconds()), "convert_µs")
					b.ReportMetric(float64(stats.SerializeTime.Microseconds()), "serialize_µs")
					b.ReportMetric(float64(stats.BuildTime.Microseconds()), "build_µs")
					b.ReportMetric(float64(stats.UploadTime.Microseconds()), "upload_µs")
					b.ReportMetric(float64(stats.CompressedOutputSize), "output_bytes")
					b.ReportMetric(float64(bm.batchSize)/float64(stats.BuildTime.Milliseconds())*1000, "rows/sec")
				}
			}
		})
	}
}

// BenchmarkParquetConstructionChunkSizes benchmarks different chunk sizes with fixed parallelism
func BenchmarkParquetConstructionChunkSizes(b *testing.B) {
	env := streamtesting.Setup(&testing.T{})
	ctx := context.Background()

	privateKey := streamtesting.GenerateTestPrivateKey(&testing.T{})

	serviceClient, err := streaming.NewSnowflakeServiceClient(ctx, streaming.ClientOptions{
		Account:        "test_account",
		URL:            env.Server.URL(),
		User:           "test_user",
		Role:           "TESTROLE",
		PrivateKey:     privateKey,
		Logger:         streamtesting.GetLogger(&testing.T{}),
		ConnectVersion: "1.0.0",
	})
	require.NoError(b, err)
	defer serviceClient.Close()

	const batchSize = 50000
	const parallelism = 4

	chunkSizes := []int{1000, 2500, 5000, 10000, 12500, 25000, 50000}

	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("chunk_%d", chunkSize), func(b *testing.B) {
			channelOpts := streaming.ChannelOptions{
				Name:         fmt.Sprintf("benchmark_chunk_%d", chunkSize),
				DatabaseName: "TEST_DB",
				SchemaName:   "PUBLIC",
				TableName:    "TEST_TABLE",
				BuildOptions: streaming.BuildOptions{
					Parallelism: parallelism,
					ChunkSize:   chunkSize,
				},
			}

			channel, err := serviceClient.OpenChannel(ctx, channelOpts)
			require.NoError(b, err)

			batch := generateTestBatch(batchSize)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				stats, err := channel.InsertRows(ctx, batch, nil)
				if err != nil {
					b.Fatalf("InsertRows failed: %v", err)
				}

				if i == 0 {
					b.ReportMetric(float64(stats.BuildTime.Microseconds()), "build_µs")
					b.ReportMetric(float64(batchSize)/float64(stats.BuildTime.Milliseconds())*1000, "rows/sec")
				}
			}
		})
	}
}

// BenchmarkParquetConstructionParallelism benchmarks different parallelism levels with fixed chunk size
func BenchmarkParquetConstructionParallelism(b *testing.B) {
	env := streamtesting.Setup(&testing.T{})
	ctx := context.Background()

	privateKey := streamtesting.GenerateTestPrivateKey(&testing.T{})

	serviceClient, err := streaming.NewSnowflakeServiceClient(ctx, streaming.ClientOptions{
		Account:        "test_account",
		URL:            env.Server.URL(),
		User:           "test_user",
		Role:           "TESTROLE",
		PrivateKey:     privateKey,
		Logger:         streamtesting.GetLogger(&testing.T{}),
		ConnectVersion: "1.0.0",
	})
	require.NoError(b, err)
	defer serviceClient.Close()

	const batchSize = 50000
	const chunkSize = 10000

	parallelismLevels := []int{1, 2, 4, 8, 16}

	for _, parallelism := range parallelismLevels {
		b.Run(fmt.Sprintf("parallel_%d", parallelism), func(b *testing.B) {
			channelOpts := streaming.ChannelOptions{
				Name:         fmt.Sprintf("benchmark_parallel_%d", parallelism),
				DatabaseName: "TEST_DB",
				SchemaName:   "PUBLIC",
				TableName:    "TEST_TABLE",
				BuildOptions: streaming.BuildOptions{
					Parallelism: parallelism,
					ChunkSize:   chunkSize,
				},
			}

			channel, err := serviceClient.OpenChannel(ctx, channelOpts)
			require.NoError(b, err)

			batch := generateTestBatch(batchSize)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				stats, err := channel.InsertRows(ctx, batch, nil)
				if err != nil {
					b.Fatalf("InsertRows failed: %v", err)
				}

				if i == 0 {
					b.ReportMetric(float64(stats.BuildTime.Microseconds()), "build_µs")
					b.ReportMetric(float64(batchSize)/float64(stats.BuildTime.Milliseconds())*1000, "rows/sec")
				}
			}
		})
	}
}
