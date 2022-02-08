package processor

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestDedupe(t *testing.T) {
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(rndText1)
	doc2 := []byte(rndText1) // duplicate
	doc3 := []byte(rndText2)

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.QuickBatch([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}
}

func TestDedupeInterpolation(t *testing.T) {
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(fmt.Sprintf(`{"id":%q,"content":"foo"}`, rndText1))
	doc2 := []byte(fmt.Sprintf(`{"id":%q,"content":"bar"}`, rndText1)) // duplicate
	doc3 := []byte(fmt.Sprintf(`{"id":%q,"content":"foo"}`, rndText2))
	doc4 := []byte(`{"content":"foo"}`)

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Key = "${! json(\"id\") }${! json(\"never.exists\") }"
	conf.Dedupe.DropOnCacheErr = false
	proc, err1 := NewDedupe(conf, mgr, log.Noop(), metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.QuickBatch([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 3 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc4})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 4 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 4 told not to propagate even if it was expected to propagate")
	}
}

func TestDedupeXXHash(t *testing.T) {
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(rndText1)
	doc2 := []byte(rndText1) // duplicate
	doc3 := []byte(rndText2)

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.HashType = "xxhash"
	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.QuickBatch([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.QuickBatch([][]byte{doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}
}

func TestDedupePartSelection(t *testing.T) {
	hdr := []byte(`some header`)
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(rndText1)
	doc2 := []byte(rndText1) // duplicate
	doc3 := []byte(rndText2)

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Parts = []int{1} // only take the 2nd part
	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.QuickBatch([][]byte{hdr, doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.QuickBatch([][]byte{hdr, doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.QuickBatch([][]byte{hdr, doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}
}

func TestDedupeBadCache(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"

	testLog := log.Noop()

	mgr := mock.NewManager()
	if _, err := NewDedupe(conf, mgr, testLog, metrics.Noop()); err == nil {
		t.Error("Expected error from missing cache")
	}
}

func TestDedupeCacheErrors(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	delete(mgr.Caches, "foocache")

	msgs, res := proc.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	if exp := response.NewAck(); !reflect.DeepEqual(exp, res) || len(msgs) > 0 {
		t.Errorf("Expected message drop on error: %v - %v", res, len(msgs))
	}

	conf.Dedupe.DropOnCacheErr = false
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err = NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	delete(mgr.Caches, "foocache")

	msgs, res = proc.ProcessMessage(message.QuickBatch([][]byte{[]byte("foo"), []byte("bar")}))
	if res != nil || len(msgs) != 1 {
		t.Errorf("Expected message propagate on error: %v - %v", res, len(msgs))
	}
}

func TestDedupeBadHash(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.HashType = "notexist"

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	if _, err := NewDedupe(conf, mgr, testLog, metrics.Noop()); err == nil {
		t.Error("Expected error from bad hash")
	}
}

func TestDedupeBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Parts = []int{5}

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Fatal(err1)
	}

	msgIn := message.QuickBatch([][]byte{})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) > 0 {
		t.Error("OOB message told to propagate")
	}

	if exp, act := response.NewAck(), res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong response returned: %v != %v", act, exp)
	}
}

func TestDedupeNegBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Parts = []int{-5}

	testLog := log.Noop()

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Fatal(err1)
	}

	msgIn := message.QuickBatch([][]byte{})
	msgs, res := proc.ProcessMessage(msgIn)
	if len(msgs) > 0 {
		t.Error("OOB message told to propagate")
	}

	if exp, act := response.NewAck(), res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong response returned: %v != %v", act, exp)
	}
}

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
