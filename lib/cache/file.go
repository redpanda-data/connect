package cache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFile] = TypeSpec{
		constructor: NewFile,
		Description: `
The file cache stores each item in a directory as a file, where an item ID is
the path relative to the configured directory.

This type currently offers no form of item expiry or garbage collection, and is
intended to be used for development and debugging purposes only.`,
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

// File is a file system based cache implementation.
type File struct {
	dir string
}

// NewFile creates a new File cache type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error) {
	return &File{
		dir: conf.File.Directory,
	}, nil
}

//------------------------------------------------------------------------------

// Get attempts to locate and return a cached value by its key, returns an error
// if the key does not exist.
func (f *File) Get(key string) ([]byte, error) {
	b, err := ioutil.ReadFile(filepath.Join(f.dir, key))
	if os.IsNotExist(err) {
		return nil, types.ErrKeyNotFound
	}
	return b, err
}

// Set attempts to set the value of a key.
func (f *File) Set(key string, value []byte) error {
	return ioutil.WriteFile(filepath.Join(f.dir, key), value, 0644)
}

// SetMulti attempts to set the value of multiple keys, returns an error if any
// keys fail.
func (f *File) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		if err := f.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Add attempts to set the value of a key only if the key does not already exist
// and returns an error if the key already exists.
func (f *File) Add(key string, value []byte) error {
	file, err := os.OpenFile(filepath.Join(f.dir, key), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
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

// Delete attempts to remove a key.
func (f *File) Delete(key string) error {
	return os.Remove(filepath.Join(f.dir, key))
}

// CloseAsync shuts down the cache.
func (f *File) CloseAsync() {
}

// WaitForClose blocks until the cache has closed down.
func (f *File) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
