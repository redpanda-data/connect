package pure

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

func TestSyncResponseWriter(t *testing.T) {
	wctx := context.Background()

	impl := transaction.NewResultStore()
	w := SyncResponseWriter{}
	if err := w.Connect(wctx); err != nil {
		t.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), transaction.ResultStoreKey, impl)

	msg := message.QuickBatch(nil)
	p := message.NewPart([]byte("foo"))
	p = message.WithContext(ctx, p)
	msg = append(msg, p, message.NewPart([]byte("bar")))

	if err := w.WriteBatch(wctx, msg); err != nil {
		t.Fatal(err)
	}

	impl.Get()
	results := impl.Get()
	if len(results) != 1 {
		t.Fatalf("Wrong count of result batches: %v", len(results))
	}
	if results[0].Len() != 2 {
		t.Fatalf("Wrong count of messages: %v", results[0].Len())
	}
	if exp, act := "foo", string(results[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(results[0].Get(1).AsBytes()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if store := message.GetContext(results[0].Get(0)).Value(transaction.ResultStoreKey); store != nil {
		t.Error("Unexpected nested result store")
	}

	require.NoError(t, w.Close(ctx))
}
