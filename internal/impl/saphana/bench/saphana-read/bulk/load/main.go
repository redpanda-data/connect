package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/SAP/go-hdb/driver"
)

func main() {
	count := flag.Int("count", 1000000, "number of rows to insert")
	batch := flag.Int("batch", 5000, "rows per transaction")
	flag.Parse()

	dsn := os.Getenv("HANA_DSN")
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "HANA_DSN environment variable is required\n")
		os.Exit(1)
	}
	schema := os.Getenv("HANA_SCHEMA")
	if schema == "" {
		fmt.Fprintf(os.Stderr, "HANA_SCHEMA environment variable is required\n")
		os.Exit(1)
	}
	table := fmt.Sprintf(`"%s"."BENCH_ORDERS"`, schema)

	ctx := context.Background()

	db, err := sql.Open("hdb", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
		os.Exit(1)
	}

	_, _ = db.ExecContext(ctx, `DROP TABLE `+table)

	createSQL := `CREATE COLUMN TABLE ` + table + ` (` +
		`ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,` +
		`USER_ID INTEGER,` +
		`PRODUCT_ID INTEGER,` +
		`QUANTITY INTEGER,` +
		`PRICE DECIMAL(10,2),` +
		`STATUS NVARCHAR(20),` +
		`NOTES NVARCHAR(5000),` +
		`CREATED_AT TIMESTAMP` +
		`)`
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	statuses := []string{"pending", "confirmed", "shipped"}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	inserted := 0
	start := time.Now()
	lastPrint := start

	for inserted < *count {
		batchSize := *batch
		if remaining := *count - inserted; remaining < batchSize {
			batchSize = remaining
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to begin transaction: %v\n", err)
			os.Exit(1)
		}

		stmt, err := tx.PrepareContext(ctx, `INSERT INTO `+table+` (USER_ID, PRODUCT_ID, QUANTITY, PRICE, STATUS, NOTES, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			_ = tx.Rollback()
			fmt.Fprintf(os.Stderr, "failed to prepare statement: %v\n", err)
			os.Exit(1)
		}

		for i := 0; i < batchSize; i++ {
			notes := strings.Repeat(fmt.Sprintf("note_%d ", inserted+i+1), 10)
			if _, err := stmt.ExecContext(ctx,
				rng.Intn(100000)+1,
				rng.Intn(10000)+1,
				rng.Intn(10)+1,
				1.0+rng.Float64()*999.0,
				statuses[rng.Intn(3)],
				notes,
				time.Now().AddDate(0, 0, -rng.Intn(365)),
			); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				fmt.Fprintf(os.Stderr, "failed to insert row: %v\n", err)
				os.Exit(1)
			}
		}

		if err := stmt.Close(); err != nil {
			_ = tx.Rollback()
			fmt.Fprintf(os.Stderr, "failed to close statement: %v\n", err)
			os.Exit(1)
		}
		if err := tx.Commit(); err != nil {
			fmt.Fprintf(os.Stderr, "failed to commit transaction: %v\n", err)
			os.Exit(1)
		}

		inserted += batchSize

		now := time.Now()
		if now.Sub(lastPrint) >= 250*time.Millisecond || inserted >= *count {
			elapsed := now.Sub(start).Seconds()
			rate := float64(inserted) / elapsed
			fmt.Printf("\r%d / %d  (%.0f rows/s)", inserted, *count, rate)
			lastPrint = now
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("\nInserted %d rows in %s (%.0f rows/s)\n", inserted, elapsed.Round(time.Millisecond), float64(inserted)/elapsed.Seconds())
}
