package pure

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	bloom "github.com/bits-and-blooms/bloom/v3"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	bloomCacheFieldCapLabel        = "cap"
	bloomCacheFieldCapDefaultValue = 10000

	bloomCacheFieldFalsePositiveRateLabel        = "fp"
	bloomCacheFieldFalsePositiveRateDefaultValue = 0.01

	bloomCacheFieldInitValuesLabel = "init_values"

	bloomCacheFieldStorageLabel               = "storage"
	commonFieldStoragePathLabel               = "path"
	commonFieldStorageSkipDumpLabel           = "skip_dump"
	commonFieldStorageSkipDumpDefaultValue    = false
	commonFieldStorageSkipRestoreLabel        = "skip_restore"
	commonFieldStorageSkipRestoreDefaultValue = false
)

func bloomCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores keys in a bloom in-memory filter, useful for deduplication. This cache is therefore reset every time the service restarts.`).
		Description(`This provides the bloom package which implements a fixed-size thread safe filter.

A Bloom filter is a concise/compressed representation of a set, where the main requirement is to make membership queries; i.e., whether an item is a member of a set. A Bloom filter will always correctly report the presence of an element in the set when the element is indeed present. A Bloom filter can use much less storage than the original set, but it allows for some 'false positives': it may sometimes report that an element is in the set whereas it is not.

When you construct, you need to know how many elements you have (the desired capacity), and what is the desired false positive rate you are willing to tolerate. A common false-positive rate is 1%. The lower the false-positive rate, the more memory you are going to require. Similarly, the higher the capacity, the more memory you will use.

It uses the package ` + "[`bloomfilter`](github.com/bits-and-blooms/bloom/v3)" + `

The field ` + bloomCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of keys:

` + "```yml" + `
cache_resources:
  - label: foocache
    bloom:
      cap: 1024
      init_values:
        - foo
        - bar
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(bloomCacheFieldCapLabel).
			Description("The cache maximum capacity (number of entries)").
			Default(bloomCacheFieldCapDefaultValue)).
		Field(service.NewFloatField(bloomCacheFieldFalsePositiveRateLabel).
			Description("false positive rate. 1% is 0.01").
			Default(bloomCacheFieldFalsePositiveRateDefaultValue)).
		Field(service.NewStringListField(bloomCacheFieldInitValuesLabel).
			Description("A table of keys that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default([]string{}).
			Example([]string{
				"Nickelback",
				"Spice Girls",
				"The Human League",
			})).
		Field(service.NewObjectField(bloomCacheFieldStorageLabel,
			service.NewStringField(commonFieldStoragePathLabel).
				Description(`Path to a dir or file where we can restore or write dumps of bloom filter.

This cache can try to dump the content of the cache in disk during the benthos shutdown.

Also, the cache can try to restore the state from an existing dump file. Errors will be ignored on this phase.

This field accepts two kinds of value:

If the path contains a single file with extension '.dat', it will be used for I/O operations.

If the path contains a directory, we will try to use the most recent dump file (if any). The directory must exits.

If necessary, we will create a file with format 'benthos-bloom-dump.<timestamp>.dat'
`).
				Example("/path/to/bloom-dumps-dir/").
				Example("/path/to/bloom-dumps-dir/benthos-bloom-dump.1691480368391.dat").
				Example("/path/to/bloom-dumps-dir/you-can-choose-any-other-name.dat").
				Advanced(),
			service.NewBoolField(commonFieldStorageSkipRestoreLabel).
				Description("If true, will not restore the filter state from disk").
				Default(commonFieldStorageSkipRestoreDefaultValue).
				Advanced(),
			service.NewBoolField(commonFieldStorageSkipDumpLabel).
				Description("If true, will not dump the filter state on disk").
				Default(commonFieldStorageSkipDumpDefaultValue).
				Advanced()).
			Description("If present, can be used to write and restore dumps of bloom filters").
			Advanced().
			Optional()).
		Footnotes(`This component implements all cache operations except *delete*, however it does not store any value, only the keys.

The main intent is to be used on deduplication.

When fetch a key from this case, if the key exists, we return a fixed string` + "`t`" + `.`)

	return spec
}

func init() {
	err := service.RegisterCache(
		"bloom", bloomCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := bloomMemCacheFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func bloomMemCacheFromConfig(conf *service.ParsedConfig, log *service.Logger) (*bloomCacheAdapter, error) {
	capacity, err := conf.FieldInt(bloomCacheFieldCapLabel)
	if err != nil {
		return nil, err
	}

	fp, err := conf.FieldFloat(bloomCacheFieldFalsePositiveRateLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringList(bloomCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	bloomLogger := log.With("cache", "bloom")

	var storage *storageDumpConf

	if conf.Contains(bloomCacheFieldStorageLabel) {
		subConf := conf.Namespace(bloomCacheFieldStorageLabel)
		storage, err = newStorageDumpConf(subConf, bloomLogger)
		if err != nil {
			return nil, err
		}
	}

	return bloomMemCache(capacity, fp, initValues, storage, bloomLogger)
}

//------------------------------------------------------------------------------

var (
	errInvalidBloomCacheCapacityValue          = fmt.Errorf("invalid bloom cache parameter capacity: must be bigger than 0")
	errInvalidBloomCacheFalsePositiveRateValue = fmt.Errorf("invalid bloom cache parameter fp: must be bigger than 0")
)

func bloomMemCache(capacity int,
	fp float64,
	initValues []string,
	storage *storageDumpConf,
	log *service.Logger,
) (ca *bloomCacheAdapter, err error) {
	if capacity <= 0 {
		return nil, errInvalidBloomCacheCapacityValue
	}

	if fp <= 0 {
		return nil, errInvalidBloomCacheFalsePositiveRateValue
	}

	inner := bloom.NewWithEstimates(uint(capacity), fp)

	for _, key := range initValues {
		inner.AddString(key)
	}

	ca = &bloomCacheAdapter{
		inner:   inner,
		storage: storage,
		log:     log,
	}

	if ierr := ca.restoreDumpFromDisk(); ierr != nil {
		log.With("error", err).Warnf("unable to restore dump from disk, skip")
	}

	for _, key := range initValues {
		_ = ca.inner.AddString(key)
	}

	return ca, nil
}

//------------------------------------------------------------------------------

var _ service.Cache = (*bloomCacheAdapter)(nil)

type bloomCacheAdapter struct {
	inner *bloom.BloomFilter

	storage *storageDumpConf

	log *service.Logger

	sync.RWMutex
}

func (ca *bloomCacheAdapter) prefix() string {
	return "benthos-bloom-dump"
}

func (ca *bloomCacheAdapter) suffix() string {
	return ".dat"
}

func (ca *bloomCacheAdapter) restoreDumpFromDisk() error {
	ca.Lock()
	defer ca.Unlock()

	err := ca.storage.searchForDumpFile(ca.prefix(), ca.suffix(), func(dumpFile *os.File) error {
		_, err := ca.inner.ReadFrom(dumpFile)
		if err != nil {
			return fmt.Errorf("unable to restore dump file %q: %w", dumpFile.Name(), err)
		}

		ca.log.Debugf("restore bloom dump from file %q with success", dumpFile.Name())

		return nil
	})
	if err != nil {
		return fmt.Errorf("error while search for dump file: %w", err)
	}

	return nil
}

func (ca *bloomCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	ca.RWMutex.RLock()

	ok := ca.inner.TestString(key)

	ca.RWMutex.RUnlock()

	if !ok {
		return nil, service.ErrKeyNotFound
	}

	return []byte{'t'}, nil
}

func (ca *bloomCacheAdapter) Set(_ context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ca.inner.AddString(key)

	ca.RWMutex.Unlock()

	return nil
}

func (ca *bloomCacheAdapter) Add(ctx context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ok := ca.inner.TestOrAddString(key)

	ca.RWMutex.Unlock()

	if ok {
		return service.ErrKeyAlreadyExists
	}

	return nil
}

var errUnableToDeleteKeyIntoBloomFilter = errors.New("unable to delete key into bloom filter: not supported")

func (ca *bloomCacheAdapter) Delete(_ context.Context, key string) error {
	return errUnableToDeleteKeyIntoBloomFilter
}

func (ca *bloomCacheAdapter) Close(_ context.Context) error {
	return ca.flushOnDisk(time.Now())
}

func (ca *bloomCacheAdapter) flushOnDisk(now time.Time) error {
	ca.Lock()
	defer ca.Unlock()

	err := ca.storage.writeDumpFile(ca.prefix(), ca.suffix(), now, func(f *os.File) error {
		_, err := ca.inner.WriteTo(f)
		if err != nil {
			return err
		}

		ca.log.Debugf("write dump file with success on %q", f.Name())

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

//------------------------------------------------------------------------------

type storageDumpConf struct {
	storagePath      string
	lastImportedFile string
	skipDump         bool
	skipRestore      bool

	log *service.Logger
}

func newStorageDumpConf(subConf *service.ParsedConfig, log *service.Logger) (*storageDumpConf, error) {
	storagePath, err := subConf.FieldString(commonFieldStoragePathLabel)
	if err != nil {
		return nil, err
	}

	skipDump, err := subConf.FieldBool(commonFieldStorageSkipDumpLabel)
	if err != nil {
		return nil, err
	}

	return &storageDumpConf{
		storagePath: path.Clean(storagePath),
		skipDump:    skipDump,
		log:         log,
	}, nil
}

func (c *storageDumpConf) shouldSkipRestore() bool {
	if c == nil {
		return true
	}

	return c.skipRestore
}

func (c *storageDumpConf) shouldSkipDump() bool {
	if c == nil {
		return true
	}

	return c.skipDump
}

func (c *storageDumpConf) searchForDumpFile(prefix, suffix string, callback func(*os.File) error) error {
	if c.shouldSkipRestore() {
		return nil
	}

	info, err := os.Stat(c.storagePath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("unable to extract information from path %q: %w", c.storagePath, err)
	}

	if info.IsDir() {
		return c.scanDir(prefix, suffix, callback)
	}

	dumpFile, err := os.Open(c.storagePath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	defer dumpFile.Close()

	err = callback(dumpFile)
	if err != nil {
		return err
	}

	c.lastImportedFile = dumpFile.Name()

	return nil
}

func (c *storageDumpConf) scanDir(prefix, suffix string, callback func(*os.File) error) error {
	entries, err := os.ReadDir(c.storagePath)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("unable to list entries from dir %q: %w", c.storagePath, err)
	}

	var (
		dumpFile *os.File
		modTime  time.Time
	)

	for _, entry := range entries {
		if entry.IsDir() || !entry.Type().IsRegular() {
			continue
		}

		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}

		if !strings.HasSuffix(entry.Name(), suffix) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			c.log.With("err", err, "entry", entry.Name()).
				Tracef("unable to read filesystem information, skip entry")

			continue
		}

		if fileSize := info.Size(); fileSize <= int64(2*binary.Size(uint64(0))) {
			c.log.With("file_size", fileSize, "entry", entry.Name()).
				Tracef("file size too low, skip entry")

			continue
		}

		if curModTime := info.ModTime(); curModTime.After(modTime) {
			fileName := path.Join(c.storagePath, info.Name())

			candidate, err := os.Open(fileName)
			if err != nil {
				c.log.With("err", err, "file", fileName).Tracef("unable to open file, skip entry")

				continue
			}

			if dumpFile != nil {
				dumpFile.Close()
			}

			dumpFile, modTime = candidate, curModTime
		}
	}

	if dumpFile != nil {
		defer dumpFile.Close()

		err = callback(dumpFile)
		if err != nil {
			return err
		}

		c.lastImportedFile = dumpFile.Name()

		return nil
	}

	return nil
}

func (c *storageDumpConf) writeDumpFile(prefix, suffix string, now time.Time, callback func(*os.File) error) error {
	if c.shouldSkipDump() {
		return nil
	}

	filePath := c.lastImportedFile

	if filePath == "" {
		filePath = c.storagePath

		info, err := os.Stat(filePath)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		if info != nil && info.IsDir() {
			fileName := fmt.Sprintf("%s.%d%s", prefix, now.UnixNano(), suffix)
			filePath = path.Join(filePath, fileName)
		}
	}

	dumpFile, err := os.Create(filePath)
	if err != nil {
		return err
	}

	defer dumpFile.Close()

	err = callback(dumpFile)
	if err != nil {
		return err
	}

	err = dumpFile.Sync()
	if err != nil {
		return err
	}

	c.log.Tracef("dump file on %q", filePath)

	c.lastImportedFile = filePath

	return nil
}
