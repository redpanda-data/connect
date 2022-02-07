package output

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
)

func TestSyncResponseWriter(t *testing.T) {
	wctx := context.Background()

	impl := roundtrip.NewResultStore()
	w := SyncResponseWriter{}
	if err := w.ConnectWithContext(wctx); err != nil {
		t.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), roundtrip.ResultStoreKey, impl)

	msg := message.QuickBatch(nil)
	p := message.NewPart([]byte("foo"))
	p = message.WithContext(ctx, p)
	msg.Append(p)
	msg.Append(message.NewPart([]byte("bar")))

	if err := w.WriteWithContext(wctx, msg); err != nil {
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
	if exp, act := "foo", string(results[0].Get(0).Get()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if exp, act := "bar", string(results[0].Get(1).Get()); exp != act {
		t.Errorf("Wrong message contents: %v != %v", act, exp)
	}
	if store := message.GetContext(results[0].Get(0)).Value(roundtrip.ResultStoreKey); store != nil {
		t.Error("Unexpected nested result store")
	}

	w.CloseAsync()
	if err := w.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
