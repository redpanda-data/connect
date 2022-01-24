package cache

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFile] = TypeSpec{
		constructor: NewFile,
		Summary: `
Stores each item in a directory as a file, where an item ID is the path relative
to the configured directory.`,
		Description: `
This type currently offers no form of item expiry or garbage collection, and is
intended to be used for development and debugging purposes only.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("directory", "The directory within which to store items."),
		},
	}
}

//------------------------------------------------------------------------------

// FileConfig contains config fields for the File cache type.
type FileConfig struct {
	Directory string `json:"directory" yaml:"directory"`
}

// NewFileConfig creates a FileConfig populated with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Directory: "",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File cache type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return cache.NewV2ToV1Cache(&fileV2{dir: conf.File.Directory}, stats), nil
}

type fileV2 struct {
	dir string
}

func (f *fileV2) Get(_ context.Context, key string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(f.dir, key))
	if os.IsNotExist(err) {
		return nil, types.ErrKeyNotFound
	}
	return b, err
}

func (f *fileV2) Set(_ context.Context, key string, value []byte, _ *time.Duration) error {
	return os.WriteFile(filepath.Join(f.dir, key), value, 0o644)
}

func (f *fileV2) SetMulti(ctx context.Context, keyValues map[string]types.CacheTTLItem) error {
	for k, v := range keyValues {
		if err := f.Set(ctx, k, v.Value, v.TTL); err != nil {
			return err
		}
	}
	return nil
}

func (f *fileV2) Add(_ context.Context, key string, value []byte, _ *time.Duration) error {
	file, err := os.OpenFile(filepath.Join(f.dir, key), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if os.IsExist(err) {
			return types.ErrKeyAlreadyExists
		}
		return err
	}
	if _, err = file.Write(value); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func (f *fileV2) Delete(_ context.Context, key string) error {
	return os.Remove(filepath.Join(f.dir, key))
}

func (f *fileV2) Close(context.Context) error {
	return nil
}
