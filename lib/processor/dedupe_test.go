package processor

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type fakeMgr struct {
	caches     map[string]types.Cache
	ratelimits map[string]types.RateLimit
}

func (f *fakeMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}
func (f *fakeMgr) GetCache(name string) (types.Cache, error) {
	if c, exists := f.caches[name]; exists {
		return c, nil
	}
	return nil, types.ErrCacheNotFound
}
func (f *fakeMgr) GetRateLimit(name string) (types.RateLimit, error) {
	if r, exists := f.ratelimits[name]; exists {
		return r, nil
	}
	return nil, types.ErrRateLimitNotFound
}
func (f *fakeMgr) GetPlugin(name string) (interface{}, error) {
	return nil, types.ErrPluginNotFound
}
func (f *fakeMgr) GetPipe(name string) (<-chan types.Transaction, error) {
	return nil, types.ErrPipeNotFound
}
func (f *fakeMgr) SetPipe(name string, prod <-chan types.Transaction)   {}
func (f *fakeMgr) UnsetPipe(name string, prod <-chan types.Transaction) {}

func TestDedupe(t *testing.T) {
	rndText1 := randStringRunes(20)
	rndText2 := randStringRunes(15)
	doc1 := []byte(rndText1)
	doc2 := []byte(rndText1) // duplicate
	doc3 := []byte(rndText2)

	testLog := log.Noop()

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, testLog, metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.New([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.New([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.New([][]byte{doc3})
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

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Key = "${! json(\"id\") }${! json(\"never.exists\") }"
	conf.Dedupe.DropOnCacheErr = false
	proc, err1 := NewDedupe(conf, mgr, log.Noop(), metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.New([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.New([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 3 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.New([][]byte{doc3})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 3 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.New([][]byte{doc4})
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

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, testLog, metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.HashType = "xxhash"
	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.New([][]byte{doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.New([][]byte{doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.New([][]byte{doc3})
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

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, testLog, metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Parts = []int{1} // only take the 2nd part
	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Error(err1)
		return
	}

	msgIn := message.New([][]byte{hdr, doc1})
	msgOut, err := proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate. Cache error:", err.Error())
	}
	if msgOut == nil {
		t.Error("Message 1 told not to propagate even if it was expected to propagate")
	}

	msgIn = message.New([][]byte{hdr, doc2})
	msgOut, err = proc.ProcessMessage(msgIn)
	if err != nil && err.Error() != nil {
		t.Error("Message 1 told to propagate even if it was expected not to propagate. Cache error:", err.Error())
	}
	if msgOut != nil {
		t.Error("Message 2 told to propagate even if it was expected not to propagate")
	}

	msgIn = message.New([][]byte{hdr, doc3})
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

	mgr := &fakeMgr{
		caches: map[string]types.Cache{},
	}
	if _, err := NewDedupe(conf, mgr, testLog, metrics.Noop()); err == nil {
		t.Error("Expected error from missing cache")
	}
}

type errCache struct{}

func (e errCache) Get(key string) ([]byte, error) {
	return nil, errors.New("test err")
}
func (e errCache) Set(key string, value []byte) error {
	return errors.New("test err")
}
func (e errCache) SetWithTTL(key string, value []byte, ttl *time.Duration) error {
	return errors.New("test err")
}
func (e errCache) SetMulti(items map[string][]byte) error {
	return errors.New("test err")
}
func (e errCache) SetMultiWithTTL(items map[string][]byte, ttl *time.Duration) error {
	return errors.New("test err")
}
func (e errCache) Add(key string, value []byte) error {
	return errors.New("test err")
}
func (e errCache) AddWithTTL(key string, value []byte, ttl *time.Duration) error {
	return errors.New("test err")
}
func (e errCache) Delete(key string) error {
	return errors.New("test err")
}
func (e errCache) CloseAsync() {
}
func (e errCache) WaitForClose(timeout time.Duration) error {
	return nil
}

func TestDedupeCacheErrors(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"

	testLog := log.Noop()

	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": errCache{},
		},
	}

	proc, err := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res := proc.ProcessMessage(message.New([][]byte{[]byte("foo"), []byte("bar")}))
	if exp := response.NewAck(); !reflect.DeepEqual(exp, res) || len(msgs) > 0 {
		t.Errorf("Expected message drop on error: %v - %v", res, len(msgs))
	}

	conf.Dedupe.DropOnCacheErr = false

	proc, err = NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msgs, res = proc.ProcessMessage(message.New([][]byte{[]byte("foo"), []byte("bar")}))
	if res != nil || len(msgs) != 1 {
		t.Errorf("Expected message propagate on error: %v - %v", res, len(msgs))
	}
}

func TestDedupeBadHash(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.HashType = "notexist"

	testLog := log.Noop()

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, testLog, metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}
	if _, err := NewDedupe(conf, mgr, testLog, metrics.Noop()); err == nil {
		t.Error("Expected error from bad hash")
	}
}

func TestDedupeBoundsCheck(t *testing.T) {
	conf := NewConfig()
	conf.Dedupe.Cache = "foocache"
	conf.Dedupe.Parts = []int{5}

	testLog := log.Noop()

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, testLog, metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Fatal(err1)
	}

	msgIn := message.New([][]byte{})
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

	memCache, cacheErr := cache.NewMemory(cache.NewConfig(), nil, testLog, metrics.Noop())
	if cacheErr != nil {
		t.Fatal(cacheErr)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	proc, err1 := NewDedupe(conf, mgr, testLog, metrics.Noop())
	if err1 != nil {
		t.Fatal(err1)
	}

	msgIn := message.New([][]byte{})
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
