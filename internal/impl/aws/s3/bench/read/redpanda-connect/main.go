// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// output_salesforce.go implements a Redpanda Connect output component that writes
// messages to Salesforce using either the sObject Collections REST API (realtime mode)
// or the Bulk API 2.0 (bulk mode).
//
// Messages are routed to the correct SObject configuration based on the "topic" field
// set by the per-topic processor. Each topic_mapping entry defines the SObject, operation,
// external ID field, and write mode for a given topic.

// Package main provides a benchmark data generation tool for AWS S3.
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	bucketName       = "bench-objects"
	progressInterval = 5000
)

func newS3Client(endpoint, region string) *s3.Client {
	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx"),
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <setup|seed|drop-bucket> [flags]\n", os.Args[0])
		os.Exit(1)
	}
	switch os.Args[1] {
	case "setup":
		runSetup(os.Args[2:])
	case "seed":
		runSeed(os.Args[2:])
	case "drop-bucket":
		runDropBucket(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(1)
	}
}

// setup -----------------------------------------------------------------------

func runSetup(args []string) {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:4566", "S3 endpoint URL")
	region := fs.String("region", "us-east-1", "AWS region")
	_ = fs.Parse(args)

	client := newS3Client(*endpoint, *region)
	ctx := context.Background()
	if err := createBucketIfNotExists(ctx, client); err != nil {
		fmt.Fprintf(os.Stderr, "setup: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Bucket ready.")
}

func createBucketIfNotExists(ctx context.Context, client *s3.Client) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err == nil {
		fmt.Printf("Bucket %s already exists.\n", bucketName)
		return nil
	}

	fmt.Printf("Creating bucket %s...\n", bucketName)
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}); err != nil {
		return fmt.Errorf("create %s: %w", bucketName, err)
	}

	waiter := s3.NewBucketExistsWaiter(client)
	if err := waiter.Wait(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}, time.Minute); err != nil {
		return fmt.Errorf("wait %s: %w", bucketName, err)
	}
	fmt.Printf("Bucket %s created.\n", bucketName)
	return nil
}

// seed ------------------------------------------------------------------------

func runSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:4566", "S3 endpoint URL")
	region := fs.String("region", "us-east-1", "AWS region")
	total := fs.Int("total", 50000, "Number of objects to upload")
	size := fs.Int("size", 1024, "Object size in bytes")
	workers := fs.Int("workers", 64, "Number of concurrent workers")
	_ = fs.Parse(args)

	client := newS3Client(*endpoint, *region)
	ctx := context.Background()
	if err := seedBucket(ctx, client, *total, *size, *workers); err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
}

func seedBucket(ctx context.Context, client *s3.Client, total, objSize, numWorkers int) error {
	fmt.Printf("Uploading %d objects (~%d bytes each) to %s...\n", total, objSize, bucketName)
	start := time.Now()

	jobs := make(chan int, numWorkers*2)

	var uploaded atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				if err := uploadObject(ctx, client, n, objSize); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				done := uploaded.Add(1)
				if done%progressInterval == 0 {
					elapsed := time.Since(start).Seconds()
					fmt.Printf("Progress: %d/%d objects (%.0f obj/sec)\n", done, total, float64(done)/elapsed)
				}
			}
		}()
	}

	for i := range total {
		select {
		case jobs <- i:
		case err := <-errCh:
			close(jobs)
			wg.Wait()
			return err
		}
	}
	close(jobs)
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	elapsed := time.Since(start).Seconds()
	done := uploaded.Load()
	totalMB := float64(int64(objSize)*done) / 1e6
	fmt.Printf("Completed: %d objects uploaded to %s in %.1fs (%.0f obj/sec, %.1f MB/s)\n",
		done, bucketName, elapsed, float64(done)/elapsed, totalMB/elapsed)
	return nil
}

func uploadObject(ctx context.Context, client *s3.Client, n, size int) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(fmt.Sprintf("bench/obj-%08d.json", n)),
		Body:        bytes.NewReader(makeObjectBody(n, size)),
		ContentType: aws.String("application/json"),
	})
	return err
}

func makeObjectBody(n, size int) []byte {
	prefix := fmt.Sprintf(`{"id":"obj-%d","created_at":"%s","data":"`, n, time.Now().UTC().Format(time.RFC3339))
	suffix := `"}`
	pad := size - len(prefix) - len(suffix)
	if pad < 0 {
		pad = 0
	}
	return []byte(prefix + strings.Repeat("x", pad) + suffix)
}

// drop-bucket -----------------------------------------------------------------

func runDropBucket(args []string) {
	fs := flag.NewFlagSet("drop-bucket", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:4566", "S3 endpoint URL")
	region := fs.String("region", "us-east-1", "AWS region")
	_ = fs.Parse(args)

	client := newS3Client(*endpoint, *region)
	ctx := context.Background()
	deleted, err := emptyBucket(ctx, client)
	if err != nil {
		fmt.Fprintf(os.Stderr, "drop-bucket: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Deleted %d objects from %s.\n", deleted, bucketName)
}

func emptyBucket(ctx context.Context, client *s3.Client) (int, error) {
	var total int
	var token *string
	for {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			ContinuationToken: token,
		})
		if err != nil {
			return total, fmt.Errorf("list: %w", err)
		}
		if len(out.Contents) == 0 {
			break
		}
		ids := make([]types.ObjectIdentifier, len(out.Contents))
		for i, obj := range out.Contents {
			ids[i] = types.ObjectIdentifier{Key: obj.Key}
		}
		if _, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{Objects: ids},
		}); err != nil {
			return total, fmt.Errorf("delete: %w", err)
		}
		total += len(ids)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	return total, nil
}
