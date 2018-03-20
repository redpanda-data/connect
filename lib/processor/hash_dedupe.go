// Copyright (c) 2018 Lorenzo Alberton
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/OneOfOne/xxhash"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["hash_dedupe"] = TypeSpec{
		constructor: NewHashDedupe,
		description: `
Dedupes a message by hashing selected parts of the message and checking the hash
against a memcache cluster, dropping messages whose hash is already present in the cache.`,
	}
}

//------------------------------------------------------------------------------

type MemcacheConfig struct {
	Servers []string `json:"servers" yaml:"servers"`
	Prefix  string   `json:"prefix" yaml:"prefix"`
	TTL     string   `json:"ttl" yaml:"ttl"`
}

// HashDedupeConfig contains any configuration for the HashDedupe processor.
type HashDedupeConfig struct {
	Cache MemcacheConfig `json:"memcache" yaml:"memcache"`
	Parts []int          `json:"parts" yaml:"parts"` // message parts to hash
}

// NewHashDedupeConfig returns a HashDedupeConfig with default values.
func NewHashDedupeConfig() HashDedupeConfig {
	return HashDedupeConfig{
		Cache: MemcacheConfig{
			Servers: []string{"localhost:11211"},
			Prefix:  "",
			TTL:     "",
		},
		Parts:     []int{0}, // only consider the 1st part
	}
}

//------------------------------------------------------------------------------

// HashDedupe is a processor that hashes each message and checks if the has is already
// present in the cache
type HashDedupe struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mc  *memcache.Client
	ttl time.Duration
}

// NewHashDedupe returns a HashDedupe processor.
func NewHashDedupe(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var ttl time.Duration
	var err error
	if len(conf.HashDedupe.Cache.TTL) > 0 {
		ttl, err = time.ParseDuration(conf.HashDedupe.Cache.TTL)
	}

	return &HashDedupe{
		conf:  conf,
		log:   log.NewModule(".processor.hash_dedupe"),
		stats: stats,

		mc:  memcache.New(conf.HashDedupe.Cache.Servers...),
		ttl: ttl,
	}, err
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (s *HashDedupe) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.stats.Incr("processor.hash_dedupe.count", 1)

	hash := xxhash.New64()

	lParts := msg.Len()
	for _, index := range s.conf.HashDedupe.Parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}

		// Check boundary of part index.
		if index < 0 || index >= lParts {
			s.stats.Incr("processor.hash_dedupe.dropped_part_out_of_bounds", 1)
			s.stats.Incr("processor.hash_dedupe.dropped", 1)
			s.log.Errorf("Cannot sample message part %v for parts count: %v\n", index, lParts)
			return nil, types.NewSimpleResponse(nil)
		}

		// Attempt to add part to hash.
		if _, err := hash.Write(msg.Get(index)); nil != err {
			s.stats.Incr("processor.hash_dedupe.hashing_error", 1)
			s.log.Errorf("Cannot hash message part for duplicate check: %v\n", err)
			return nil, types.NewSimpleResponse(nil)
		}
	}

	isDupe, err := s.isDuplicate(hash.Sum64())
	if nil != err {
		s.stats.Incr("processor.hash_dedupe.cache_error", 1)
		s.log.Errorf("Error talking to the cache while checking for duplicate: %v\n", err)
		return nil, types.NewSimpleResponse(err)
	}
	if !isDupe {
		s.stats.Incr("processor.hash_dedupe.sent", 1)
		msgs := [1]types.Message{msg}
		return msgs[:], nil
	}

	s.stats.Incr("processor.hash_dedupe.dropped", 1)
	return nil, types.NewSimpleResponse(nil)
}

//------------------------------------------------------------------------------

// isDuplicate checks if the ID has been seen before (true) or if it's the first time (false).
// This counts as a touch: the first time an ID is checked, it is added to the cache;
// the second time the same ID is checked, it is considered as a duplicate
// The function could return an error in case Memcache is not reachable
func (s *HashDedupe) isDuplicate(id uint64) (bool, error) {
	err := s.mc.Add(s.getItemFor(id))
	if memcache.ErrNotStored == err {
		return true, nil
	}
	return false, err
}

// getKeyFor prepends the prefix to the item key
func (s *HashDedupe) getKeyFor(id uint64) string {
	return fmt.Sprintf("%s%d", s.conf.HashDedupe.Cache.Prefix, id)
}

// getItemFor returns a memcache.Item object ready to be stored in memcache
func (s *HashDedupe) getItemFor(id uint64) *memcache.Item {
	return &memcache.Item{
		Key:        s.getKeyFor(id),
		Value:      []byte{'t'},
		Expiration: int32(s.ttl * time.Second),
	}
}

//------------------------------------------------------------------------------
