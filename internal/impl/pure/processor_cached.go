package pure

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/benthosdev/benthos/v4/public/service"
)

func newCachedProcessorConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.3.0").
		Categories("Utility").
		Summary("Cache the result of applying one or more processors to messages identified by a key. If the key already exists within the cache the contents of the message will be replaced with the cached result instead of applying the processors. This component is therefore useful in situations where an expensive set of processors need only be executed periodically.").
		Description("The format of the data when stored within the cache is a custom and versioned schema chosen to balance performance and storage space. It is therefore not possible to point this processor to a cache that is pre-populated with data that this processor has not created itself.").
		Field(service.NewStringField("cache").Description("The cache resource to read and write processor results from.")).
		Field(service.NewInterpolatedStringField("key").
			Description("A key to be resolved for each message, if the key already exists in the cache then the cached result is used, otherwise the processors are applied and the result is cached under this key. The key could be static and therefore apply generally to all messages or it could be an interpolated expression that is potentially unique for each message.").
			Example("my_foo_result").
			Example(`${! this.document.id }`).
			Example(`${! meta("kafka_key") }`).
			Example(`${! meta("kafka_topic") }`)).
		Field(service.NewDurationField("ttl").Description("An optional expiry period to set for each cache entry. Some caches only have a general TTL and will therefore ignore this setting.").Optional()).
		Field(service.NewProcessorListField("processors").Description("The list of processors whose result will be cached.")).
		Example(
			"Cached Enrichment",
			"In the following example we want to we enrich messages consumed from Kafka with data specific to the origin topic partition, we do this by placing an `http` processor within a `branch`, where the HTTP URL contains interpolation functions with the topic and partition in the path.\n\nHowever, it would be inefficient to make this HTTP request for every single message as the result is consistent for all data of a given topic partition. We can solve this by placing our enrichment call within a `cached` processor where the key contains the topic and partition, resulting in messages that originate from the same topic/partition combination using the cached result of the prior.",
			`
pipeline:
  processors:
    - branch:
        processors:
          - cached:
              key: '${! meta("kafka_topic") }-${! meta("kafka_partition") }'
              cache: foo_cache
              processors:
                - mapping: 'root = ""'
                - http:
                    url: http://example.com/enrichment/${! meta("kafka_topic") }/${! meta("kafka_partition") }
                    verb: GET
        result_map: 'root.enrichment = this'

cache_resources:
  - label: foo_cache
    memory:
      # Disable compaction so that cached items never expire
      compaction_interval: ""
`,
		).
		Example(
			"Periodic Global Enrichment",
			"In the following example we enrich all messages with the same data obtained from a static URL with an `http` processor within a `branch`. However, we expect the data from this URL to change roughly every 10 minutes, so we configure a `cached` processor with a static key (since this request is consistent for all messages) and a TTL of `10m`.",
			`
pipeline:
  processors:
    - branch:
        request_map: 'root = ""'
        processors:
          - cached:
              key: static_foo
              cache: foo_cache
              ttl: 10m
              processors:
                - http:
                    url: http://example.com/get/foo.json
                    verb: GET
        result_map: 'root.foo = this'

cache_resources:
  - label: foo_cache
    memory: {}
`,
		)
}

func init() {
	err := service.RegisterProcessor(
		"cached", newCachedProcessorConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newCachedProcessorFromParsedConf(mgr, conf)
		})
	if err != nil {
		panic(err)
	}
}

type cachedProcessor struct {
	manager *service.Resources

	cacheName  string
	key        *service.InterpolatedString
	ttl        *time.Duration
	processors []*service.OwnedProcessor
}

func newCachedProcessorFromParsedConf(manager *service.Resources, conf *service.ParsedConfig) (proc *cachedProcessor, err error) {
	proc = &cachedProcessor{
		manager: manager,
	}

	if proc.cacheName, err = conf.FieldString("cache"); err != nil {
		return nil, err
	}
	if !manager.HasCache(proc.cacheName) {
		return nil, fmt.Errorf("cache named %v not found", proc.cacheName)
	}

	if proc.key, err = conf.FieldInterpolatedString("key"); err != nil {
		return nil, err
	}

	if conf.Contains("ttl") {
		var ttl time.Duration
		if ttl, err = conf.FieldDuration("ttl"); err != nil {
			return nil, err
		}

		proc.ttl = &ttl
	}

	proc.processors, err = conf.FieldProcessorList("processors")
	return
}

func (proc *cachedProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	cacheKey := proc.key.String(msg)

	var cachedBytes []byte
	var err error
	if cerr := proc.manager.AccessCache(ctx, proc.cacheName, func(cache service.Cache) {
		cachedBytes, err = cache.Get(ctx, cacheKey)
	}); cerr != nil {
		return nil, cerr
	}

	// Return early if we have a cached result
	if err == nil {
		batch, err := cachedProcResultToBatch(msg, cachedBytes)
		if err != nil {
			err = fmt.Errorf("failed to parsed cached result, this indicates the data was not set by this processor: %w", err)
		}
		return batch, err
	}

	// Or if an error occurred that wasn't ErrKeyNotFound
	if !errors.Is(err, service.ErrKeyNotFound) {
		return nil, err
	}

	// Result is not cached, so execute processors and cache the result
	resultBatch, err := service.ExecuteProcessors(ctx, proc.processors, service.MessageBatch{msg})
	if err != nil {
		return nil, err
	}

	var collapsedBatch service.MessageBatch
	for _, b := range resultBatch {
		collapsedBatch = append(collapsedBatch, b...)
	}

	// Any errors in creating a serialised batch or caching are non-fatal and
	// should be logged but otherwise regarded as insignificant to the flowing
	// messages.
	result, err := cachedProcSerialiseBatch(collapsedBatch)
	if err != nil {
		proc.manager.Logger().Errorf("failed to serialise resulting batch for caching: %w", err)
		return collapsedBatch, nil
	}

	var setErr error
	cerr := proc.manager.AccessCache(ctx, proc.cacheName, func(cache service.Cache) {
		setErr = cache.Set(ctx, cacheKey, result, proc.ttl)
	})
	if cerr != nil {
		proc.manager.Logger().Errorf("failed to access cache for result: %w", err)
	}
	if setErr != nil {
		proc.manager.Logger().Errorf("failed to write result to cache: %w", err)
	}

	return collapsedBatch, nil
}

func (proc *cachedProcessor) Close(ctx context.Context) error {
	var group errgroup.Group
	for _, ownedProc := range proc.processors {
		op := ownedProc
		group.Go(func() error {
			return op.Close(ctx)
		})
	}

	return group.Wait()
}

//------------------------------------------------------------------------------

// We use versioning for the bytes we write to the cache, this allows us to
// update and modify our serialiser in future in a backwards compatible way.

func cachedProcExtractUint32(b []byte) (n uint32, remaining []byte, err error) {
	if len(b) < 4 {
		err = errors.New("message is too small to extract number")
		return
	}
	n = binary.BigEndian.Uint32(b[:4])
	remaining = b[4:]
	return
}

func cachedProcUint32ToBytes(n uint32) []byte {
	newBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(newBytes, n)
	return newBytes
}

func cachedProcSerialiseBatch(batch service.MessageBatch) ([]byte, error) {
	var buf bytes.Buffer

	// Insert schema version
	// TODO: Increment this on any schema change
	if _, err := buf.Write(cachedProcUint32ToBytes(1)); err != nil {
		return nil, err
	}

	// Insert number of batch messages
	if _, err := buf.Write(cachedProcUint32ToBytes(uint32(len(batch)))); err != nil {
		return nil, err
	}

	// For each message
	for i, msg := range batch {
		mBytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("unable to extract bytes from message %v: %w", i, err)
		}

		// Insert size of message
		if _, err := buf.Write(cachedProcUint32ToBytes(uint32(len(mBytes)))); err != nil {
			return nil, err
		}

		// Write data
		if _, err := buf.Write(mBytes); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func cachedProcV1DeserialiseBatch(msg *service.Message, data []byte) (resBatch service.MessageBatch, err error) {
	// Extract number of batch messages
	var nBatches uint32
	if nBatches, data, err = cachedProcExtractUint32(data); err != nil {
		return nil, fmt.Errorf("failed to extract batch size: %w", err)
	}

	for i := 0; i < int(nBatches); i++ {
		// Extract message length
		var msgSize uint32
		if msgSize, data, err = cachedProcExtractUint32(data); err != nil {
			return nil, fmt.Errorf("failed to extract message %v size: %w", i, err)
		}

		if len(data) < int(msgSize) {
			return nil, fmt.Errorf("failed to extract message %v size: input data ended unexpectedly", i)
		}

		// Copy message with bytes
		msgCopy := msg.Copy()
		msgCopy.SetBytes(data[:msgSize])
		resBatch = append(resBatch, msgCopy)

		data = data[msgSize:]
	}

	return
}

func cachedProcResultToBatch(msg *service.Message, cachedResult []byte) (service.MessageBatch, error) {
	verID, remaining, err := cachedProcExtractUint32(cachedResult)
	if err != nil {
		return nil, fmt.Errorf("failed to extract serialisation format version: %w", err)
	}
	if verID == 1 {
		return cachedProcV1DeserialiseBatch(msg, remaining)
	}
	return nil, fmt.Errorf("invalid format version: %v", verID)
}
