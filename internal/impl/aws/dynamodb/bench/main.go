// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package main provides a benchmark data generation tool for DynamoDB CDC.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	tableUsers       = "bench-users"
	tableProducts    = "bench-products"
	tableOrders      = "bench-orders"
	tableCheckpoints = "bench-checkpoints"
	batchSize        = 25
	progressInterval = 10000
)

var orderStatuses = []string{"pending", "processing", "shipped", "delivered", "cancelled"}

func newDynamoClient(endpoint, region string) *dynamodb.Client {
	cfg := aws.Config{
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx"),
		BaseEndpoint: aws.String(endpoint),
	}
	return dynamodb.NewFromConfig(cfg)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <setup|seed|drop-checkpoint> [flags]\n", os.Args[0])
		os.Exit(1)
	}
	switch os.Args[1] {
	case "setup":
		runSetup(os.Args[2:])
	case "seed":
		runSeed(os.Args[2:])
	case "drop-checkpoint":
		runDropCheckpoint(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(1)
	}
}

// setup -----------------------------------------------------------------------

func runSetup(args []string) {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:8000", "DynamoDB endpoint URL")
	region := fs.String("region", "us-east-1", "AWS region")
	_ = fs.Parse(args)

	client := newDynamoClient(*endpoint, *region)
	ctx := context.Background()
	for _, name := range []string{tableUsers, tableProducts, tableOrders} {
		if err := createTableIfNotExists(ctx, client, name); err != nil {
			fmt.Fprintf(os.Stderr, "setup: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println("All tables ready.")
}

func createTableIfNotExists(ctx context.Context, client *dynamodb.Client, tableName string) error {
	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
	if err == nil {
		fmt.Printf("Table %s already exists.\n", tableName)
		return nil
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("describe %s: %w", tableName, err)
	}

	fmt.Printf("Creating table %s...\n", tableName)
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		return fmt.Errorf("create %s: %w", tableName, err)
	}

	waiter := dynamodb.NewTableExistsWaiter(client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, time.Minute); err != nil {
		return fmt.Errorf("wait %s: %w", tableName, err)
	}
	fmt.Printf("Table %s created with streams enabled.\n", tableName)
	return nil
}

// seed ------------------------------------------------------------------------

func runSeed(args []string) {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:8000", "DynamoDB endpoint URL")
	region := fs.String("region", "us-east-1", "AWS region")
	table := fs.String("table", "", "Table to seed (bench-users, bench-products, bench-orders)")
	total := fs.Int("total", 150000, "Number of items to insert")
	workers := fs.Int("workers", 16, "Number of concurrent workers")
	_ = fs.Parse(args)

	if *table == "" {
		fmt.Fprintln(os.Stderr, "seed: --table is required")
		os.Exit(1)
	}

	var itemFn func(n int) map[string]types.AttributeValue
	switch *table {
	case tableUsers:
		itemFn = makeUserItem
	case tableProducts:
		itemFn = makeProductItem
	case tableOrders:
		itemFn = makeOrderItem
	default:
		fmt.Fprintf(os.Stderr, "seed: unknown table %q\n", *table)
		os.Exit(1)
	}

	client := newDynamoClient(*endpoint, *region)
	ctx := context.Background()
	if err := seedTable(ctx, client, *table, *total, *workers, itemFn); err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
}

func seedTable(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	total, numWorkers int,
	itemFn func(n int) map[string]types.AttributeValue,
) error {
	fmt.Printf("Inserting %d items into %s...\n", total, tableName)
	start := time.Now()

	type job struct{ from, to int }
	jobs := make(chan job, numWorkers*2)

	var written atomic.Int64
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := writeBatch(ctx, client, tableName, j.from, j.to, itemFn); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				n := written.Add(int64(j.to - j.from))
				prev := n - int64(j.to-j.from)
				for ms := (prev/progressInterval + 1) * progressInterval; ms <= n; ms += progressInterval {
					elapsed := time.Since(start).Seconds()
					fmt.Printf("Progress: %d/%d items (%.0f items/sec)\n", n, total, float64(n)/elapsed)
				}
			}
		}()
	}

	for from := 0; from < total; from += batchSize {
		to := min(from+batchSize, total)
		select {
		case jobs <- job{from, to}:
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
	fmt.Printf("Completed: %d items inserted into %s in %.1fs (%.0f items/sec)\n",
		total, tableName, elapsed, float64(total)/elapsed)
	return nil
}

func writeBatch(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	from, to int,
	itemFn func(n int) map[string]types.AttributeValue,
) error {
	reqs := make([]types.WriteRequest, 0, to-from)
	for n := from; n < to; n++ {
		reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: itemFn(n)}})
	}
	for len(reqs) > 0 {
		out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{tableName: reqs},
		})
		if err != nil {
			return fmt.Errorf("batch write: %w", err)
		}
		reqs = out.UnprocessedItems[tableName]
	}
	return nil
}

// item factories --------------------------------------------------------------

func sAttr(v string) types.AttributeValue  { return &types.AttributeValueMemberS{Value: v} }
func nAttr(v string) types.AttributeValue  { return &types.AttributeValueMemberN{Value: v} }
func bAttr(v bool) types.AttributeValue    { return &types.AttributeValueMemberBOOL{Value: v} }

func makeUserItem(n int) map[string]types.AttributeValue {
	dob := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, n%10000).Format("2006-01-02")
	about := ""
	for range 50 {
		about += fmt.Sprintf("This is about user %d. ", n)
	}
	return map[string]types.AttributeValue{
		"id":            sAttr(fmt.Sprintf("user-%d", n)),
		"name":          sAttr(fmt.Sprintf("user-%d", n)),
		"surname":       sAttr(fmt.Sprintf("surname-%d", n)),
		"about":         sAttr(about),
		"email":         sAttr(fmt.Sprintf("user%d@example.com", n)),
		"date_of_birth": sAttr(dob),
		"created_at":    sAttr(time.Now().UTC().Format(time.RFC3339)),
		"is_active":     bAttr(n%2 == 0),
		"login_count":   nAttr(fmt.Sprintf("%d", n%100)),
		"balance":       nAttr(fmt.Sprintf("%.2f", float64(n%1000)+float64(n%100)/100.0)),
	}
}

func makeProductItem(n int) map[string]types.AttributeValue {
	dateAdded := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, n%1825).Format("2006-01-02")
	desc := ""
	for range 50 {
		desc += fmt.Sprintf("Product description for item %d. ", n)
	}
	return map[string]types.AttributeValue{
		"id":           sAttr(fmt.Sprintf("product-%d", n)),
		"name":         sAttr(fmt.Sprintf("Product %d", n)),
		"info":         sAttr(fmt.Sprintf("SKU-%08d", n)),
		"description":  sAttr(desc),
		"email":        sAttr(fmt.Sprintf("vendor%d@example.com", n)),
		"date_added":   sAttr(dateAdded),
		"created_at":   sAttr(time.Now().UTC().Format(time.RFC3339)),
		"is_active":    bAttr(n%3 != 0),
		"basket_count": nAttr(fmt.Sprintf("%d", n%500)),
		"price":        nAttr(fmt.Sprintf("%.2f", float64(n%10000)/100.0)),
	}
}

func makeOrderItem(n int) map[string]types.AttributeValue {
	orderDate := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, n%730).Format("2006-01-02")
	notes := ""
	for range 50 {
		notes += fmt.Sprintf("Order notes for order %d. ", n)
	}
	qty := n%10 + 1
	return map[string]types.AttributeValue{
		"id":         sAttr(fmt.Sprintf("order-%d", n)),
		"user_id":    sAttr(fmt.Sprintf("user-%d", n%10000)),
		"product_id": sAttr(fmt.Sprintf("product-%d", n%5000)),
		"notes":      sAttr(notes),
		"status":     sAttr(orderStatuses[n%5]),
		"order_date": sAttr(orderDate),
		"created_at": sAttr(time.Now().UTC().Format(time.RFC3339)),
		"quantity":   nAttr(fmt.Sprintf("%d", qty)),
		"total":      nAttr(fmt.Sprintf("%.2f", float64(n%10000)/100.0*float64(qty))),
	}
}

// drop-checkpoint -------------------------------------------------------------

func runDropCheckpoint(args []string) {
	fs := flag.NewFlagSet("drop-checkpoint", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://localhost:8000", "DynamoDB endpoint URL")
	region := fs.String("region", "us-east-1", "AWS region")
	_ = fs.Parse(args)

	client := newDynamoClient(*endpoint, *region)
	_, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
		TableName: aws.String(tableCheckpoints),
	})
	if err != nil {
		var notFound *types.ResourceNotFoundException
		if errors.As(err, &notFound) {
			fmt.Printf("Table %s does not exist, nothing to drop.\n", tableCheckpoints)
			return
		}
		fmt.Fprintf(os.Stderr, "drop-checkpoint: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Dropped table %s.\n", tableCheckpoints)
}
