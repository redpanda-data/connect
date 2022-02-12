package processor

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/OneOfOne/xxhash"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeDedupe] = TypeSpec{
		constructor: NewDedupe,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Deduplicates message batches by caching selected (and optionally hashed)
messages, dropping batches that are already cached.`,
		Description: `
This processor acts across an entire batch, in order to deduplicate individual
messages within a batch use this processor with the
` + "[`for_each`](/docs/components/processors/for_each)" + ` processor.

Optionally, the ` + "`key`" + ` field can be populated in order to hash on a
function interpolated string rather than the full contents of messages. This
allows you to deduplicate based on dynamic fields within a message, such as its
metadata, JSON fields, etc. A full list of interpolation functions can be found
[here](/docs/configuration/interpolation#bloblang-queries).

For example, the following config would deduplicate based on the concatenated
values of the metadata field ` + "`kafka_key`" + ` and the value of the JSON
path ` + "`id`" + ` within the message contents:

` + "```yaml" + `
pipeline:
  processors:
    - dedupe:
        cache: foocache
        key: ${! meta("kafka_key") }-${! json("id") }
` + "```" + `

Caches should be configured as a resource, for more information check out the
[documentation here](/docs/components/caches/about).

When using this processor with an output target that might fail you should
always wrap the output within a ` + "[`retry`](/docs/components/outputs/retry)" + `
block. This ensures that during outages your messages aren't reprocessed after
failures, which would result in messages being dropped.

## Delivery Guarantees

Performing deduplication on a stream using a distributed cache voids any
at-least-once guarantees that it previously had. This is because the cache will
preserve message signatures even if the message fails to leave the Benthos
pipeline, which would cause message loss in the event of an outage at the output
sink followed by a restart of the Benthos instance.

If you intend to preserve at-least-once delivery guarantees you can avoid this
problem by using a memory based cache. This is a compromise that can achieve
effective deduplication but parallel deployments of the pipeline as well as
service restarts increase the chances of duplicates passing undetected.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("cache", "The [`cache` resource](/docs/components/caches/about) to target with this processor."),
			docs.FieldCommon("hash", "The hash type to used.").HasOptions("none", "xxhash"),
			docs.FieldCommon("key", "An optional key to use for deduplication (instead of the entire message contents).").IsInterpolated(),
			docs.FieldCommon("drop_on_err", "Whether messages should be dropped when the cache returns an error."),
			docs.FieldAdvanced("parts", "An array of message indexes within the batch to deduplicate based on. If left empty all messages are included. This field is only applicable when batching messages [at the input level](/docs/configuration/batching).").Array(),
		},
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

	key *field.Expression

	mgr        interop.Manager
	cacheName  string
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
	conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	hFunc, err := strToHasher(conf.Dedupe.HashType)
	if err != nil {
		return nil, err
	}

	key, err := mgr.BloblEnvironment().NewField(conf.Dedupe.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	if !mgr.ProbeCache(conf.Dedupe.Cache) {
		return nil, fmt.Errorf("cache resource '%v' was not found", conf.Dedupe.Cache)
	}

	return &Dedupe{
		conf:  conf,
		log:   log,
		stats: stats,

		key: key,

		mgr:        mgr,
		cacheName:  conf.Dedupe.Cache,
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
func (d *Dedupe) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	d.mCount.Incr(1)

	extractedHash := false
	hasher := d.hasherFunc()

	spans := tracing.CreateChildSpans(TypeDedupe, msg)
	defer func() {
		for _, s := range spans {
			s.Finish()
		}
	}()

	key := d.key.Bytes(0, msg)
	if len(key) > 0 {
		hasher.Write(key)
		extractedHash = true
	} else {
		for _, index := range d.conf.Dedupe.Parts {
			// Attempt to add whole part to hash.
			if partBytes := msg.Get(index).Get(); partBytes != nil {
				if _, err := hasher.Write(msg.Get(index).Get()); err != nil {
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
			return nil, nil
		}
	} else {
		var err error
		if cerr := d.mgr.AccessCache(context.Background(), d.cacheName, func(cache cache.V1) {
			err = cache.Add(context.Background(), string(hasher.Bytes()), []byte{'t'}, nil)
		}); cerr != nil {
			err = cerr
		}
		if err != nil {
			if err == component.ErrKeyAlreadyExists {
				for _, s := range spans {
					s.LogKV(
						"event", "dropped",
						"type", "deduplicated",
					)
				}
				d.mDropped.Incr(1)
				return nil, nil
			}
			d.mErrCache.Incr(1)
			d.mErr.Incr(1)
			d.log.Errorf("Cache error: %v\n", err)
			for _, s := range spans {
				s.LogKV(
					"event", "error",
					"type", err.Error(),
				)
			}
			if d.conf.Dedupe.DropOnCacheErr {
				d.mDropped.Incr(1)
				return nil, nil
			}
		}
	}

	d.mBatchSent.Incr(1)
	d.mSent.Incr(int64(msg.Len()))
	msgs := [1]*message.Batch{msg}
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
