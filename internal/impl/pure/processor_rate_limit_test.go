package pure_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestRateLimitBasic(t *testing.T) {
	var hits int32
	rlFn := func(context.Context) (time.Duration, error) {
		atomic.AddInt32(&hits, 1)
		return 0, nil
	}

	mgr := mock.NewManager()
	mgr.RateLimits["foo"] = rlFn

	conf := processor.NewConfig()
	conf.Type = "rate_limit"
	conf.RateLimit.Resource = "foo"
	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessBatch(context.Background(), input)
	if res != nil {
		t.Fatal(res)
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	if exp, act := int32(3), atomic.LoadInt32(&hits); exp != act {
		t.Errorf("Wrong count of rate limit hits: %v != %v", act, exp)
	}
}

func TestRateLimitErroredOut(t *testing.T) {
	rlFn := func(context.Context) (time.Duration, error) {
		return 0, errors.New("omg foo")
	}

	mgr := mock.NewManager()
	mgr.RateLimits["foo"] = rlFn

	conf := processor.NewConfig()
	conf.Type = "rate_limit"
	conf.RateLimit.Resource = "foo"
	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	closedChan := make(chan struct{})
	go func() {
		output, res := proc.ProcessBatch(context.Background(), input)
		if res != nil {
			t.Error(res)
		}

		if len(output) != 1 {
			t.Errorf("Wrong count of result messages: %v", len(output))
		}

		if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result messages: %s != %s", act, exp)
		}
		close(closedChan)
	}()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()
	assert.NoError(t, proc.Close(ctx))

	select {
	case <-closedChan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
}

func TestRateLimitBlocked(t *testing.T) {
	rlFn := func(context.Context) (time.Duration, error) {
		return time.Second * 10, nil
	}

	mgr := mock.NewManager()
	mgr.RateLimits["foo"] = rlFn

	conf := processor.NewConfig()
	conf.Type = "rate_limit"
	conf.RateLimit.Resource = "foo"
	proc, err := mgr.NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	input := message.QuickBatch([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	closedChan := make(chan struct{})
	go func() {
		output, res := proc.ProcessBatch(context.Background(), input)
		if res != nil {
			t.Error(res)
		}

		if len(output) != 1 {
			t.Errorf("Wrong count of result messages: %v", len(output))
		}

		if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result messages: %s != %s", act, exp)
		}
		close(closedChan)
	}()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()
	assert.NoError(t, proc.Close(ctx))

	select {
	case <-closedChan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
}
