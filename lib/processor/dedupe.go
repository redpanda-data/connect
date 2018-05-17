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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
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

It's possible to extract JSON field data from message parts by setting the value
of ` + "`json_path`" + `, which will become the value that is deduplicated
against. Please note that this extraction will apply to all message parts
specified.

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
  json_path: id
  hash: none
` + "```" + `

Caches should be configured as a resource, for more information check out the
[documentation here](../caches).`,
	}
}

//------------------------------------------------------------------------------

// DedupeConfig contains any configuration for the Dedupe processor.
type DedupeConfig struct {
	Cache          string `json:"cache" yaml:"cache"`
	HashType       string `json:"hash" yaml:"hash"`
	Parts          []int  `json:"parts" yaml:"parts"` // message parts to hash
	JSONPath       string `json:"json_path" yaml:"json_path"`
	DropOnCacheErr bool   `json:"drop_on_err" yaml:"drop_on_err"`
}

// NewDedupeConfig returns a DedupeConfig with default values.
func NewDedupeConfig() DedupeConfig {
	return DedupeConfig{
		Cache:          "",
		HashType:       "none",
		Parts:          []int{0}, // only consider the 1st part
		JSONPath:       "",
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
	jPath      string
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
		jPath:      conf.Dedupe.JSONPath,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (d *Dedupe) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.stats.Incr("processor.dedupe.count", 1)
	hasher := d.hasherFunc()

	lParts := msg.Len()
	for _, index := range d.conf.Dedupe.Parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}

		// Check boundary of part index.
		if index < 0 || index >= lParts {
			d.stats.Incr("processor.dedupe.dropped_part_out_of_bounds", 1)
			d.stats.Incr("processor.dedupe.dropped", 1)
			return nil, types.NewSimpleResponse(nil)
		}

		if len(d.jPath) > 0 {
			// Attempt to add JSON field from part to hash.
			jPart, err := msg.GetJSON(index)
			if err != nil {
				d.stats.Incr("processor.dedupe.error.json_parse", 1)
				d.stats.Incr("processor.dedupe.dropped", 1)
				d.log.Debugf("JSON Parse error: %v\n", err)
				return nil, types.NewSimpleResponse(nil)
			}
			var gPart *gabs.Container
			if gPart, err = gabs.Consume(jPart); err != nil {
				d.stats.Incr("processor.dedupe.error.json_parse", 1)
				d.stats.Incr("processor.dedupe.dropped", 1)
				d.log.Debugf("JSON Parse error: %v\n", err)
				return nil, types.NewSimpleResponse(nil)
			}

			var hashBytes []byte

			gTarget := gPart.Path(d.jPath)
			switch t := gTarget.Data().(type) {
			case string:
				hashBytes = []byte(t)
			default:
				hashBytes = gTarget.Bytes()
			}

			if _, err := hasher.Write(hashBytes); nil != err {
				d.stats.Incr("processor.dedupe.error.hash", 1)
				d.stats.Incr("processor.dedupe.dropped", 1)
				d.log.Debugf("Hash error: %v\n", err)
				return nil, types.NewSimpleResponse(nil)
			}
		} else {
			// Attempt to add whole part to hash.
			if _, err := hasher.Write(msg.Get(index)); nil != err {
				d.stats.Incr("processor.dedupe.error.hash", 1)
				d.stats.Incr("processor.dedupe.dropped", 1)
				d.log.Debugf("Hash error: %v\n", err)
				return nil, types.NewSimpleResponse(nil)
			}
		}
	}

	if err := d.cache.Add(string(hasher.Bytes()), []byte{'t'}); err != nil {
		if err != types.ErrKeyAlreadyExists {
			d.stats.Incr("processor.dedupe.error.cache", 1)
			d.log.Errorf("Cache error: %v\n", err)
			if d.conf.Dedupe.DropOnCacheErr {
				d.stats.Incr("processor.dedupe.dropped", 1)
				return nil, types.NewSimpleResponse(nil)
			}
		} else {
			d.stats.Incr("processor.dedupe.dropped", 1)
			return nil, types.NewSimpleResponse(nil)
		}
	}

	d.stats.Incr("processor.dedupe.sent", 1)
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
