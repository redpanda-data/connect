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
	"bytes"
	"fmt"
	"strconv"

	"github.com/OneOfOne/xxhash"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["dedupe"] = TypeSpec{
		constructor: NewDedupe,
		description: `
Dedupes messages by caching selected (and optionally hashed) parts, dropping
messages that are already cached. The hash type can be chosen from: none or
xxhash (more will come soon).

It's possible to dedupe based on JSON field data from message parts by setting
the value of ` + "`json_paths`" + `, which is an array of JSON dot paths that
will be extracted from the message payload and concatenated. The result will
then be used to deduplicate. If the result is empty (i.e. none of the target
paths were found in the data) then this is considered an error, and the message
will be dropped or propagated based on the value of ` + "`drop_on_err`." + `

For example, if each message is a single part containing a JSON blob of the
following format:

` + "``` json" + `
{
	"id": "3274892374892374",
	"content": "hello world"
}
` + "```" + `

Then you could deduplicate using the raw contents of the 'id' field instead of
the whole body with the following config:

` + "``` json" + `
type: dedupe
dedupe:
  cache: foo_cache
  parts: [0]
  json_paths:
    - id
  hash: none
` + "```" + `

Caches should be configured as a resource, for more information check out the
[documentation here](../caches).`,
	}
}

//------------------------------------------------------------------------------

// DedupeConfig contains any configuration for the Dedupe processor.
type DedupeConfig struct {
	Cache          string   `json:"cache" yaml:"cache"`
	HashType       string   `json:"hash" yaml:"hash"`
	Parts          []int    `json:"parts" yaml:"parts"` // message parts to hash
	JSONPaths      []string `json:"json_paths" yaml:"json_paths"`
	DropOnCacheErr bool     `json:"drop_on_err" yaml:"drop_on_err"`
}

// NewDedupeConfig returns a DedupeConfig with default values.
func NewDedupeConfig() DedupeConfig {
	return DedupeConfig{
		Cache:          "",
		HashType:       "none",
		Parts:          []int{0}, // only consider the 1st part
		JSONPaths:      []string{},
		DropOnCacheErr: true,
	}
}

//------------------------------------------------------------------------------

type hasher interface {
	Write(str []byte) (int, error)
	Bytes() []byte
}

type hasherFunc func() hasher

//------------------------------------------------------------------------------

type xxhashHasher struct {
	h *xxhash.XXHash64
}

func (x *xxhashHasher) Write(str []byte) (int, error) {
	return x.h.Write(str)
}

func (x *xxhashHasher) Bytes() []byte {
	return []byte(strconv.FormatUint(x.h.Sum64(), 10))
}

//------------------------------------------------------------------------------

func strToHasher(str string) (hasherFunc, error) {
	switch str {
	case "none":
		return func() hasher {
			return bytes.NewBuffer(nil)
		}, nil
	case "xxhash":
		return func() hasher {
			return &xxhashHasher{
				h: xxhash.New64(),
			}
		}, nil
	}
	return nil, fmt.Errorf("hash type not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Dedupe is a processor that hashes each message and checks if the has is already
// present in the cache
type Dedupe struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	cache      types.Cache
	hasherFunc hasherFunc
	jPaths     []string

	mCount     metrics.StatCounter
	mErrJSON   metrics.StatCounter
	mDropped   metrics.StatCounter
	mErrHash   metrics.StatCounter
	mErrCache  metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewDedupe returns a Dedupe processor.
func NewDedupe(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	c, err := mgr.GetCache(conf.Dedupe.Cache)
	if err != nil {
		return nil, err
	}
	hFunc, err := strToHasher(conf.Dedupe.HashType)
	if err != nil {
		return nil, err
	}
	return &Dedupe{
		conf:  conf,
		log:   log.NewModule(".processor.dedupe"),
		stats: stats,

		cache:      c,
		hasherFunc: hFunc,
		jPaths:     conf.Dedupe.JSONPaths,

		mCount:     stats.GetCounter("processor.dedupe.count"),
		mErrJSON:   stats.GetCounter("processor.dedupe.error.json_parse"),
		mDropped:   stats.GetCounter("processor.dedupe.dropped"),
		mErrHash:   stats.GetCounter("processor.dedupe.error.hash"),
		mErrCache:  stats.GetCounter("processor.dedupe.error.cache"),
		mSent:      stats.GetCounter("processor.dedupe.sent"),
		mSentParts: stats.GetCounter("processor.dedupe.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (d *Dedupe) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)

	extractedHash := false
	hasher := d.hasherFunc()

	for _, index := range d.conf.Dedupe.Parts {
		if len(d.jPaths) > 0 {
			// Attempt to add JSON fields from part to hash.
			jPart, err := msg.GetJSON(index)
			if err != nil {
				d.mErrJSON.Incr(1)
				d.mDropped.Incr(1)
				d.log.Errorf("JSON Parse error: %v\n", err)
				continue
			}

			var gPart *gabs.Container
			if gPart, err = gabs.Consume(jPart); err != nil {
				d.mErrJSON.Incr(1)
				d.mDropped.Incr(1)
				d.log.Errorf("JSON Parse error: %v\n", err)
				continue
			}

			for _, jPath := range d.jPaths {
				gTarget := gPart.Path(jPath)
				if gTarget.Data() == nil {
					continue
				}

				var hashBytes []byte
				switch t := gTarget.Data().(type) {
				case string:
					hashBytes = []byte(t)
				default:
					hashBytes = gTarget.Bytes()
				}

				if _, err := hasher.Write(hashBytes); nil != err {
					d.mErrHash.Incr(1)
					d.mDropped.Incr(1)
					d.log.Errorf("Hash error: %v\n", err)
				} else {
					extractedHash = true
				}
			}
		} else {
			// Attempt to add whole part to hash.
			if partBytes := msg.Get(index); partBytes != nil {
				if _, err := hasher.Write(msg.Get(index)); nil != err {
					d.mErrHash.Incr(1)
					d.mDropped.Incr(1)
					d.log.Errorf("Hash error: %v\n", err)
				} else {
					extractedHash = true
				}
			}
		}
	}

	if !extractedHash {
		if d.conf.Dedupe.DropOnCacheErr {
			d.mDropped.Incr(1)
			return nil, types.NewSimpleResponse(nil)
		}
	} else if err := d.cache.Add(string(hasher.Bytes()), []byte{'t'}); err != nil {
		if err != types.ErrKeyAlreadyExists {
			d.mErrCache.Incr(1)
			d.log.Errorf("Cache error: %v\n", err)
			if d.conf.Dedupe.DropOnCacheErr {
				d.mDropped.Incr(1)
				return nil, types.NewSimpleResponse(nil)
			}
		} else {
			d.mDropped.Incr(1)
			return nil, types.NewSimpleResponse(nil)
		}
	}

	d.mSent.Incr(1)
	d.mSentParts.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
