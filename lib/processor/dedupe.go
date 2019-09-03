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
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/OneOfOne/xxhash"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDedupe] = TypeSpec{
		constructor: NewDedupe,
		description: `
Dedupes message batches by caching selected (and optionally hashed) messages,
dropping batches that are already cached. The hash type can be chosen from:
none or xxhash.

This processor acts across an entire batch, in order to deduplicate individual
messages within a batch use this processor with the
` + "[`for_each`](#for_each)" + ` processor.

Optionally, the ` + "`key`" + ` field can be populated in order to hash on a
function interpolated string rather than the full contents of messages. This
allows you to deduplicate based on dynamic fields within a message, such as its
metadata, JSON fields, etc. A full list of interpolation functions can be found
[here](../config_interpolation.md#functions).

For example, the following config would deduplicate based on the concatenated
values of the metadata field ` + "`kafka_key`" + ` and the value of the JSON
path ` + "`id`" + ` within the message contents:

` + "``` yaml" + `
dedupe:
  cache: foocache
  key: ${!metadata:kafka_key}-${!json_field:id}
` + "```" + `

Caches should be configured as a resource, for more information check out the
[documentation here](../caches/README.md).

When using this processor with an output target that might fail you should
always wrap the output within a ` + "[`retry`](../outputs/README.md#retry)" + `
block. This ensures that during outages your messages aren't reprocessed after
failures, which would result in messages being dropped.

### Delivery Guarantees

Performing deduplication on a stream using a distributed cache voids any
at-least-once guarantees that it previously had. This is because the cache will
preserve message signatures even if the message fails to leave the Benthos
pipeline, which would cause message loss in the event of an outage at the output
sink followed by a restart of the Benthos instance.

If you intend to preserve at-least-once delivery guarantees you can avoid this
problem by using a memory based cache. This is a compromise that can achieve
effective deduplication but parallel deployments of the pipeline as well as
service restarts increase the chances of duplicates passing undetected.`,
	}
}

//------------------------------------------------------------------------------

// DedupeConfig contains configuration fields for the Dedupe processor.
type DedupeConfig struct {
	Cache          string `json:"cache" yaml:"cache"`
	HashType       string `json:"hash" yaml:"hash"`
	Parts          []int  `json:"parts" yaml:"parts"` // message parts to hash
	Key            string `json:"key" yaml:"key"`
	DropOnCacheErr bool   `json:"drop_on_err" yaml:"drop_on_err"`
}

// NewDedupeConfig returns a DedupeConfig with default values.
func NewDedupeConfig() DedupeConfig {
	return DedupeConfig{
		Cache:          "",
		HashType:       "none",
		Parts:          []int{0}, // only consider the 1st part
		Key:            "",
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

// Dedupe is a processor that deduplicates messages either by hashing the full
// contents of message parts or by hashing the value of an interpolated string.
type Dedupe struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	keyBytes       []byte
	interpolateKey bool

	cache      types.Cache
	hasherFunc hasherFunc

	mCount     metrics.StatCounter
	mErrHash   metrics.StatCounter
	mErrCache  metrics.StatCounter
	mErr       metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
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

	keyBytes := []byte(conf.Dedupe.Key)
	interpolateKey := text.ContainsFunctionVariables(keyBytes)

	return &Dedupe{
		conf:  conf,
		log:   log,
		stats: stats,

		keyBytes:       keyBytes,
		interpolateKey: interpolateKey,

		cache:      c,
		hasherFunc: hFunc,

		mCount:     stats.GetCounter("count"),
		mErrHash:   stats.GetCounter("error.hash"),
		mErrCache:  stats.GetCounter("error.cache"),
		mErr:       stats.GetCounter("error"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (d *Dedupe) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	d.mCount.Incr(1)

	extractedHash := false
	hasher := d.hasherFunc()

	spans := tracing.CreateChildSpans(TypeDedupe, msg)
	defer func() {
		for _, s := range spans {
			s.Finish()
		}
	}()

	key := d.keyBytes
	if len(key) > 0 && d.interpolateKey {
		key = text.ReplaceFunctionVariables(msg, key)
	}
	if len(key) > 0 {
		hasher.Write(key)
		extractedHash = true
	} else {
		for _, index := range d.conf.Dedupe.Parts {
			// Attempt to add whole part to hash.
			if partBytes := msg.Get(index).Get(); partBytes != nil {
				if _, err := hasher.Write(msg.Get(index).Get()); nil != err {
					d.mErrHash.Incr(1)
					d.mErr.Incr(1)
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
			d.mErr.Incr(1)
			d.log.Errorf("Cache error: %v\n", err)
			for _, s := range spans {
				s.LogFields(
					olog.String("event", "error"),
					olog.String("type", err.Error()),
				)
			}
			if d.conf.Dedupe.DropOnCacheErr {
				d.mDropped.Incr(1)
				return nil, response.NewAck()
			}
		} else {
			for _, s := range spans {
				s.LogFields(
					olog.String("event", "dropped"),
					olog.String("type", "deduplicated"),
				)
			}
			d.mDropped.Incr(1)
			return nil, response.NewAck()
		}
	}

	d.mBatchSent.Incr(1)
	d.mSent.Incr(int64(msg.Len()))
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (d *Dedupe) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (d *Dedupe) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
