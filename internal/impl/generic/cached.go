package generic

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/public/service"
	"golang.org/x/sync/errgroup"
)

type cachedResult struct {
	Batches *[][]json.RawMessage `json:"batches"`
}

func cachedResultFromBatches(batches []service.MessageBatch) (*cachedResult, error) {
	outbs := make([][]json.RawMessage, 0, len(batches))

	for _, batch := range batches {
		outb := make([]json.RawMessage, 0, len(batch))

		for _, msg := range batch {
			outm, err := msg.AsBytes()
			if err != nil {
				return nil, err
			}

			outb = append(outb, json.RawMessage(outm))
		}

		outbs = append(outbs, outb)
	}

	return &cachedResult{Batches: &outbs}, nil
}

// cachedResultToBatches converts cached results to message batches. This
// function can return nil if the cached representation is no longer compatible
// with `cachedResult`.
func cachedResultToBatches(c *cachedResult) []service.MessageBatch {
	if c.Batches == nil {
		return nil
	}

	rawBatches := *c.Batches

	bs := make([]service.MessageBatch, 0, len(rawBatches))
	for _, raw := range rawBatches {
		batch := make(service.MessageBatch, 0, len(raw))

		for _, rawMsg := range raw {
			batch = append(batch, service.NewMessage(rawMsg))
		}

		bs = append(bs, batch)
	}

	return bs
}

func newCachedProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("3.64.0").
		Categories(string(processor.CategoryUtility)).
		Summary("Cache the output of one or more processors. This is especially useful when processing messages is expensive.").
		Field(service.NewStringField("cache").Description("The cache resource to read and write processor results from.")).
		Field(service.NewInterpolatedStringField("key").Description("The cache key to use for a given message.")).
		Field(service.NewDurationField("ttl").Description("The expiry period for each cache entry.").Optional()).
		Field(service.NewProcessorListField("processors").Description("The list of processors whose result will be cached."))
}

type cachedProcessorConfig struct {
	cacheName  string
	key        *service.InterpolatedString
	ttl        *time.Duration
	processors []*service.OwnedProcessor
}

func cachedProcessorConfigFromParsed(inConf *service.ParsedConfig) (conf cachedProcessorConfig, err error) {
	if conf.cacheName, err = inConf.FieldString("cache"); err != nil {
		return
	}

	if conf.key, err = inConf.FieldInterpolatedString("key"); err != nil {
		return
	}

	if inConf.Contains("ttl") {
		var ttl time.Duration
		if ttl, err = inConf.FieldDuration("ttl"); err != nil {
			return
		}

		conf.ttl = &ttl
	}

	if conf.processors, err = inConf.FieldProcessorList("processors"); err != nil {
		return
	}

	return
}

type cachedProcessor struct {
	manager *service.Resources
	logger  *service.Logger
	config  *cachedProcessorConfig
}

func newCachedProcessor(manager *service.Resources, config *service.ParsedConfig, logger *service.Logger) (*cachedProcessor, error) {
	conf, err := cachedProcessorConfigFromParsed(config)
	if err != nil {
		return nil, err
	}

	return &cachedProcessor{manager: manager, logger: logger, config: &conf}, nil
}

func (proc *cachedProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batches := make([]service.MessageBatch, 0, len(batch))

	for i, msg := range batch {
		cacheKey := batch.InterpolatedString(i, proc.config.key)

		result, err := proc.processMessage(ctx, msg, cacheKey)
		if err != nil {
			msg.SetError(err)
			continue
		}

		batches = append(batches, result...)
	}

	return batches, nil
}

func (proc *cachedProcessor) processMessage(ctx context.Context, message *service.Message, cacheKey string) ([]service.MessageBatch, error) {
	var result []byte
	var err error
	if cerr := proc.manager.AccessCache(ctx, proc.config.cacheName, func(cache service.Cache) {
		result, err = cache.Get(ctx, cacheKey)
	}); cerr != nil {
		return nil, cerr
	}

	notFound := errors.Is(err, service.ErrKeyNotFound)

	if err != nil && !notFound {
		return nil, err
	}

	var batches []service.MessageBatch

	if !notFound {
		restored := &cachedResult{}
		err := json.Unmarshal(result, restored)
		if err != nil {
			return nil, err
		}

		batches = cachedResultToBatches(restored)
	}

	if batches == nil {
		notFound = true
	}

	if notFound {
		bs, err := service.ExecuteProcessors(ctx, proc.config.processors, service.MessageBatch{message})
		if err != nil {
			return nil, err
		}

		batches = bs

		if err := proc.updateCache(ctx, cacheKey, batches); err != nil {
			return nil, err
		}
	}

	return batches, nil
}

func (proc *cachedProcessor) Close(ctx context.Context) error {
	var group errgroup.Group
	for _, ownedProc := range proc.config.processors {
		op := ownedProc
		group.Go(func() error {
			return op.Close(ctx)
		})
	}

	return group.Wait()
}

func (proc *cachedProcessor) updateCache(ctx context.Context, cacheKey string, batches []service.MessageBatch) error {
	result, err := cachedResultFromBatches(batches)
	if err != nil {
		return err
	}

	cacheValue, err := json.Marshal(result)
	if err != nil {
		return err
	}

	var setErr error
	if cerr := proc.manager.AccessCache(ctx, proc.config.cacheName, func(cache service.Cache) {
		setErr = cache.Set(ctx, cacheKey, cacheValue, proc.config.ttl)
	}); cerr != nil {
		return cerr
	}

	return setErr
}

func init() {
	err := service.RegisterBatchProcessor(
		"cached", newCachedProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newCachedProcessor(mgr, conf, mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}
