package mssqlserver

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/stretchr/testify/require"
)

// func TestSetup2(t *testing.T) {
// 	port := "1433"
// 	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")

// 	db, err := sql.Open("mssql", connectionString)
// 	require.NoError(t, err)
// 	defer db.Close()

// 	// Insert 1M into users
// 	_, err = db.Exec(`
// 		WITH Numbers AS (
// 			SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
// 			FROM sys.all_objects a
// 			CROSS JOIN sys.all_objects b
// 		)
// 		INSERT INTO users (id, name)
// 		SELECT n, CONCAT('user-', n) FROM Numbers;
// 	`)
// 	require.NoError(t, err)

// 	// Insert 1M into products
// 	_, err = db.Exec(`
// 		WITH Numbers AS (
// 			SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
// 			FROM sys.all_objects a
// 			CROSS JOIN sys.all_objects b
// 		)
// 		INSERT INTO products (id, name)
// 		SELECT n, CONCAT('product-', n) FROM Numbers;
// 	`)
// 	require.NoError(t, err)
// }

func BenchmarkStreamingCDCChanges2(b *testing.B) {
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")
	db, err := sql.Open("mssql", connectionString)
	require.NoError(b, err)
	defer db.Close()

	var (
		fromLSN   []byte
		toLSN     []byte
		lastLSN   []byte
		instances = []string{"dbo_users", "dbo_products"}
	)

	b.ReportAllocs()

	// Reset timer to exclude setup time
	for b.Loop() {
		err := db.QueryRow(`SELECT sys.fn_cdc_get_max_lsn()`).Scan(&toLSN)
		require.NoError(b, err)

		ctx := context.Background()
		h := &rowIteratorMinHeap{}
		heap.Init(h)

		iters := make([]*changeTableRowIter, 0, len(instances))
		for _, inst := range instances {
			it, err := newChangeTableRowIter(ctx, db, inst, fromLSN, toLSN)
			require.NoError(b, err)

			if it != nil && it.current != nil {
				iters = append(iters, it)
				heap.Push(h, &heapItem{iter: it})
			} else if it != nil {
				it.Close()
			}
		}

		maxChanges := 1000 // per benchmark iteration
		count := 0
		for h.Len() > 0 && count < maxChanges {
			item := heap.Pop(h).(*heapItem)
			cur := item.iter.current

			// simulate handling, but don’t print (printing dominates cost)
			_ = cur.StartLSN
			lastLSN = cur.StartLSN

			if err := item.iter.next(); err != nil {
				item.iter.Close()
			} else {
				heap.Push(h, item)
			}
			count++
		}

		if lastLSN != nil {
			fromLSN = lastLSN
		}
	}
}

func TestStreamingCDCChanges(t *testing.T) {
	t.Skip()
	port := "1433"
	connectionString := fmt.Sprintf("sqlserver://sa:YourStrong!Passw0rd@localhost:%s?database=%s&encrypt=disable", port, "mydb")
	db, err := sql.Open("mssql", connectionString)
	require.NoError(t, err)

	var (
		fromLSN   []byte // e.g., load last checkpoint; nil means "start from beginning in tables"
		toLSN     []byte // often set to fn_cdc_get_max_lsn(); nil means "no upper bound"
		lastLSN   []byte
		instances = []string{"dbo_users", "dbo_products"}
	)

	for {
		// Fetch a upper bound so the run is repeatable
		err := db.QueryRowContext(t.Context(), `SELECT sys.fn_cdc_get_max_lsn()`).Scan(&toLSN)
		require.NoError(t, err)

		ctx := context.Background()

		// Create an iterator per table, table LSNs can be ordrred by we need to create a global
		// ordering by merging them using a (min) heap.
		h := &rowIteratorMinHeap{}
		heap.Init(h)

		iters := make([]*changeTableRowIter, 0, len(instances))
		for _, inst := range instances {
			it, err := newChangeTableRowIter(ctx, db, inst, fromLSN, toLSN)
			require.NoError(t, err)

			if it != nil && it.current != nil {
				iters = append(iters, it)
				heap.Push(h, &heapItem{iter: it})
			} else if it != nil {
				it.Close()
			}
		}

		// handle
		handle := func(c *change) error {
			fmt.Printf("LSN=%x, CommandID=%d, SeqVal=%x, op=%d table=%s cols=%d\n", c.StartLSN, c.CommandID, c.SeqVal, c.Operation, c.Table, len(c.Columns))

			// start is really just current
			lastLSN = c.StartLSN
			return nil
		}

		for h.Len() > 0 {
			item := heap.Pop(h).(*heapItem)
			cur := item.iter.current

			// Process the smallest-LSN change
			if err := handle(cur); err != nil {
				// Ensure resources get closed on early exit
				for _, it := range iters {
					it.Close()
				}
				require.NoError(t, err)
			}

			// Advance the iterator and push back if it has more
			if err := item.iter.next(); err != nil {
				// exhausted
				item.iter.Close()
			} else {
				// put back advanced back on the heap to sort it again
				heap.Push(h, item)
			}
		}

		if lastLSN != nil {
			fromLSN = lastLSN
		}

		time.Sleep(5 * time.Second)
	}
}
