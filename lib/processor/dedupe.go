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
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["dedupe"] = TypeSpec{
		constructor: NewDedupe,
		description: `
Dedupes messages by caching selected (and optionally hashed) message parts,
dropping messages that are already cached. The hash type can be chosen from:
none or xxhash (more will come soon).

Optionally, the ` + "`key`" + ` field can be populated in order to hash on a
function interpolated string rather than the full contents of message parts.
This allows you to deduplicate based on dynamic fields within a message, such as
its metadata, JSON fields, etc. A full list of interpolation functions can be
found [here](../config_interpolation.md#functions).

For example, the following config would deduplicate based on the concatenated
values of the metadata field ` + "`kafka_key`" + ` and the value of the JSON
path ` + "`id`" + ` within the message contents:

` + "``` yaml" + `
dedupe:
  cache: foocache
  key: ${!metadata:kafka_key}-${!json_field:id}
` + "```" + `

The ` + "`json_paths`" + ` field is deprecated.

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
	Key            string   `json:"key" yaml:"key"`
	JSONPaths      []string `json:"json_paths" yaml:"json_paths"`
	DropOnCacheErr bool     `json:"drop_on_err" yaml:"drop_on_err"`
}

// NewDedupeConfig returns a DedupeConfig with default values.
func NewDedupeConfig() DedupeConfig {
	return DedupeConfig{
		Cache:          "",
		HashType:       "none",
		Parts:          []int{0}, // only consider the 1st part
		Key:            "",
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

	keyBytes       []byte
	interpolateKey bool

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
	if len(conf.Dedupe.JSONPaths) > 0 {
		log.Warnln(
			"WARNING: The 'json_paths' field of the 'dedupe' processor is" +
				" deprecated, instead use the 'key' field with function" +
				" interpolation, e.g. '${!json_field:foo.bar}'",
		)
	}

	c, err := mgr.GetCache(conf.Dedupe.Cache)
	if err != nil {
		return nil, err
	}

	hFunc, err := strToHasher(conf.Dedupe.HashType)
	if err != nil {
		return nil, err
	}

	keyBytes := []byte(conf.Dedupe.Key)
	interpolateKey := text.ContainsFunctionVariables(keyBytes)

	return &Dedupe{
		conf:  conf,
		log:   log.NewModule(".processor.dedupe"),
		stats: stats,

		keyBytes:       keyBytes,
		interpolateKey: interpolateKey,

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

	key := d.keyBytes
	if len(key) > 0 && d.interpolateKey {
		key = text.ReplaceFunctionVariables(msg, key)
	}
	if len(key) > 0 {
		hasher.Write(key)
		extractedHash = true
	}

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
		} else if len(key) == 0 {
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
			return nil, response.NewAck()
		}
	} else if err := d.cache.Add(string(hasher.Bytes()), []byte{'t'}); err != nil {
		if err != types.ErrKeyAlreadyExists {
			d.mErrCache.Incr(1)
			d.log.Errorf("Cache error: %v\n", err)
			if d.conf.Dedupe.DropOnCacheErr {
				d.mDropped.Incr(1)
				return nil, response.NewAck()
			}
		} else {
			d.mDropped.Incr(1)
			return nil, response.NewAck()
		}
	}

	d.mSent.Incr(1)
	d.mSentParts.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
