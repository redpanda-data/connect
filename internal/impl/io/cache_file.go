package io

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/public/service"
)

func fileCacheConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Summary(`Stores each item in a directory as a file, where an item ID is the path relative to the configured directory.`).
		Description(`This type currently offers no form of item expiry or garbage collection, and is intended to be used for development and debugging purposes only.`).
		Field(service.NewStringField("directory").
			Description("The directory within which to store items."))

	return spec
}

func init() {
	err := service.RegisterCache(
		"file", fileCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			f, err := newFileCacheFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return f, nil
		})
	if err != nil {
		panic(err)
	}
}

func newFileCacheFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*fileCache, error) {
	directory, err := conf.FieldString("directory")
	if err != nil {
		return nil, err
	}
	return newFileCache(directory, mgr), nil
}

//------------------------------------------------------------------------------

func newFileCache(dir string, mgr *service.Resources) *fileCache {
	return &fileCache{mgr: mgr, dir: dir}
}

type fileCache struct {
	mgr *service.Resources
	dir string
}

func (f *fileCache) Get(_ context.Context, key string) ([]byte, error) {
	b, err := ifs.ReadFile(f.mgr.FS(), filepath.Join(f.dir, key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, service.ErrKeyNotFound
	}
	return b, err
}

func (f *fileCache) Set(_ context.Context, key string, value []byte, _ *time.Duration) error {
	return ifs.WriteFile(f.mgr.FS(), filepath.Join(f.dir, key), value, 0o644)
}

func (f *fileCache) Add(_ context.Context, key string, value []byte, _ *time.Duration) error {
	file, err := f.mgr.FS().OpenFile(filepath.Join(f.dir, key), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return service.ErrKeyAlreadyExists
		}
		return err
	}
	if _, err = ifs.FileWrite(file, value); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

func (f *fileCache) Delete(_ context.Context, key string) error {
	return f.mgr.FS().Remove(filepath.Join(f.dir, key))
}

func (f *fileCache) Close(context.Context) error {
	return nil
}
