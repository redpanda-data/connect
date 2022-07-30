package transaction

import (
	"context"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestResultStore(t *testing.T) {
	impl := &resultStoreImpl{}
	ctx := context.WithValue(context.Background(), ResultStoreKey, impl)
	msg := message.Batch{
		message.WithContext(ctx, message.NewPart([]byte("foo"))),
		message.NewPart([]byte("bar")),
	}

	impl.Add(msg)
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
	if store := message.GetContext(results[0].Get(0)).Value(ResultStoreKey); store != nil {
		t.Error("Unexpected nested result store")
	}

	impl.Clear()
	if exp, act := len(impl.Get()), 0; exp != act {
		t.Errorf("Unexpected count of stored messages: %v != %v", act, exp)
	}
}
