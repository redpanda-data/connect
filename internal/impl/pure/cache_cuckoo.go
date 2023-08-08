package pure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	cuckoo "github.com/seiflotfy/cuckoofilter"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	cuckooCacheFieldCapLabel        = "cap"
	cuckooCacheFieldCapDefaultValue = cuckoo.DefaultCapacity

	cuckooCacheFieldInitValuesLabel = "init_values"

	cuckooCacheFieldStorageLabel = "storage"
)

func cuckooCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores keys in a cuckoo in-memory filter, useful for deduplication. This cache is therefore reset every time the service restarts.`).
		Description(`This provides the cuckoo package which implements a fixed-size thread safe Cuckoo filter.

Cuckoo filter is a Bloom filter replacement for approximated set-membership queries. While Bloom filters are well-known space-efficient data structures to serve queries like "if item x is in a set?", they do not support deletion. Their variances to enable deletion (like counting Bloom filters) usually require much more space.

Cuckoo ﬁlters provide the ﬂexibility to add and remove items dynamically. A cuckoo filter is based on cuckoo hashing (and therefore named as cuckoo filter). It is essentially a cuckoo hash table storing each key's fingerprint. Cuckoo hash tables can be highly compact, thus a cuckoo filter could use less space than conventional Bloom ﬁlters, for applications that require low false positive rates (< 3%).

For details about the algorithm and citations please use this article for now` +

			"[`Cuckoo Filter: Better Than Bloom`](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)" +

			` by Bin Fan, Dave Andersen and Michael Kaminsky

It uses the package ` + "[`cuckoofilter`](github.com/seiflotfy/cuckoofilter)" + `

The field ` + cuckooCacheFieldInitValuesLabel + ` can be used to pre-populate the memory cache with any number of keys:

` + "```yml" + `
cache_resources:
  - label: foocache
    cuckoo:
      cap: 1024
      init_values:
        - foo
        - bar
` + "```" + `

These values can be overridden during execution.`).
		Field(service.NewIntField(cuckooCacheFieldCapLabel).
			Description("The cache maximum capacity (number of entries)").
			Default(cuckooCacheFieldCapDefaultValue)).
		Field(service.NewStringListField(cuckooCacheFieldInitValuesLabel).
			Description("A table of keys that should be present in the cache on initialization. This can be used to create static lookup tables.").
			Default([]string{}).
			Example([]string{
				"Nickelback",
				"Spice Girls",
				"The Human League",
			})).
		Field(service.NewObjectField(cuckooCacheFieldStorageLabel,
			service.NewStringField(commonFieldStoragePathLabel).
				Description(`Path to a dir or file where we can restore or write dumps of cuckoo filter.

This cache can try to dump the content of the cache in disk during the benthos shutdown.

Also, the cache can try to restore the state from an existing dump file. Errors will be ignored on this phase.

This field accepts two kinds of value:

If the path contains a single file with extension '.dat', it will be used for I/O operations.

If the path constains a directory, we will try to use the most recent dump file (if any).

If necessary, we will create a file with format 'benthos-cuckoo-dump.<timestamp>.dat'
`).
				Examples("/path/to/cuckoo-dumps-dir", "/path/to/cuckoo-dumps-dir/dump.dat").
				Advanced(),
			service.NewBoolField(commonFieldStorageReadOnlyLabel).
				Description("If true, will try to read the dump but will not flush it on disk on exit").
				Default(commonFieldStorageReadOnlyDefaultValue).
				Advanced()).
			Description("If present, can be used to write and restore dumps of cuckoo filters").
			Advanced().
			Optional()).
		Footnotes(`This component implements all cache operations, however it does not store any value, only the keys.

The main intent is to be used on deduplication.

When fetch a key from this case, if the key exists, we return a fixed string` + "`t`" + `.`)

	return spec
}

func init() {
	err := service.RegisterCache(
		"cuckoo", cuckooCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := cuckooMemCacheFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func cuckooMemCacheFromConfig(conf *service.ParsedConfig, log *service.Logger) (*cuckooCacheAdapter, error) {
	capacity, err := conf.FieldInt(cuckooCacheFieldCapLabel)
	if err != nil {
		return nil, err
	}

	initValues, err := conf.FieldStringList(cuckooCacheFieldInitValuesLabel)
	if err != nil {
		return nil, err
	}

	cuckooLogger := log.With("cache", "cuckoo")

	var storage *storageDumpConf

	if conf.Contains(cuckooCacheFieldStorageLabel) {
		subConf := conf.Namespace(cuckooCacheFieldStorageLabel)
		storage, err = newStorageDumpConf(subConf, cuckooLogger)
		if err != nil {
			return nil, err
		}
	}

	return cuckooMemCache(capacity, initValues, cuckooLogger, storage)
}

//------------------------------------------------------------------------------

var (
	errInvalidCuckooCacheCapacityValue = fmt.Errorf("invalid cuckoo cache parameter capacity: must be bigger than 0")
)

func cuckooMemCache(capacity int,
	initValues []string,
	log *service.Logger,
	storage *storageDumpConf,
) (ca *cuckooCacheAdapter, err error) {
	if capacity <= 0 {
		return nil, errInvalidCuckooCacheCapacityValue
	}

	inner := cuckoo.NewFilter(uint(capacity))

	ca = &cuckooCacheAdapter{
		inner:   inner,
		log:     log,
		storage: storage,
	}

	if ierr := ca.importDumpFromDisk(); ierr != nil {
		log.With("import error", err).Warnf("unable to import dump from disk")
	}

	for _, key := range initValues {
		inner.Insert([]byte(key))
	}

	return ca, nil
}

//------------------------------------------------------------------------------

var _ cuckooCache = (*cuckoo.Filter)(nil)

type cuckooCache interface {
	Lookup([]byte) bool
	Insert([]byte) bool
	InsertUnique([]byte) bool
	Delete([]byte) bool

	Encode() []byte
}

//------------------------------------------------------------------------------

var _ service.Cache = (*cuckooCacheAdapter)(nil)

type cuckooCacheAdapter struct {
	inner cuckooCache

	storage *storageDumpConf

	log *service.Logger

	sync.RWMutex
}

func (ca *cuckooCacheAdapter) prefix() string {
	return "benthos-cuckoo-dump"
}

func (ca *cuckooCacheAdapter) suffix() string {
	return ".dat"
}

func (ca *cuckooCacheAdapter) importDumpFromDisk() error {
	ca.Lock()
	defer ca.Unlock()

	err := ca.storage.searchForDumpFile(ca.prefix(), func(dumpFile *os.File) error {
		data, err := io.ReadAll(dumpFile)
		if err != nil {
			return err
		}

		filter, err := cuckoo.Decode(data)
		if err != nil {
			return err
		}

		ca.inner = filter

		ca.log.With("byted_readed", len(data)).Infof("import cuckoo dump from file %q with success", dumpFile.Name())

		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to search for dump file: %w", err)
	}

	return nil
}

func (ca *cuckooCacheAdapter) Get(_ context.Context, key string) ([]byte, error) {
	ca.RWMutex.RLock()

	ok := ca.inner.Lookup([]byte(key))

	ca.RWMutex.RUnlock()

	if !ok {
		return nil, service.ErrKeyNotFound
	}

	return []byte{'t'}, nil
}

var (
	errUnableToInsertKeyIntoCuckooFilter = errors.New("unable to insert key into cuckoo filter")
	errUnableToDeleteKeyIntoCuckooFilter = errors.New("unable to delete key into cuckoo filter")
)

func (ca *cuckooCacheAdapter) Set(_ context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ok := ca.inner.Insert([]byte(key))

	ca.RWMutex.Unlock()

	if !ok {
		return errUnableToInsertKeyIntoCuckooFilter
	}

	return nil
}

func (ca *cuckooCacheAdapter) Add(ctx context.Context, key string, _ []byte, _ *time.Duration) error {
	ca.RWMutex.Lock()

	ok := ca.inner.InsertUnique([]byte(key))

	ca.RWMutex.Unlock()

	if !ok {
		return service.ErrKeyAlreadyExists
	}

	return nil
}

func (ca *cuckooCacheAdapter) Delete(_ context.Context, key string) error {
	ca.RWMutex.Lock()

	ok := ca.inner.Delete([]byte(key))

	ca.RWMutex.Unlock()

	if !ok {
		return errUnableToDeleteKeyIntoCuckooFilter
	}

	return nil
}

func (ca *cuckooCacheAdapter) Close(_ context.Context) error {
	return ca.flushOnDisk(time.Now())
}

func (ca *cuckooCacheAdapter) flushOnDisk(t time.Time) error {
	ca.Lock()
	defer ca.Unlock()

	err := ca.storage.writeDumpFile(ca.prefix(), ca.suffix(), t, func(f *os.File) error {
		data := ca.inner.Encode()

		_, err := f.Write(data)

		return err
	})
	if err != nil {
		return err
	}

	return nil
}
