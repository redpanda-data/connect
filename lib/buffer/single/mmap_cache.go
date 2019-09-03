// Copyright (c) 2014 Ashley Jeffs
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

// +build !wasm

package single

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/util/disk"
	mmap "github.com/edsrzf/mmap-go"
)

//------------------------------------------------------------------------------

// MmapCacheConfig is config options for the MmapCache type.
type MmapCacheConfig struct {
	Path              string `json:"directory" yaml:"directory"`
	FileSize          int    `json:"file_size" yaml:"file_size"`
	RetryPeriod       string `json:"retry_period" yaml:"retry_period"`
	CleanUp           bool   `json:"clean_up" yaml:"clean_up"`
	ReservedDiskSpace uint64 `json:"reserved_disk_space" yaml:"reserved_disk_space"`
}

// NewMmapCacheConfig creates a new MmapCacheConfig oject with default values.
func NewMmapCacheConfig() MmapCacheConfig {
	return MmapCacheConfig{
		Path:              "",
		FileSize:          250 * 1024 * 1024, // 250MiB
		RetryPeriod:       "1s",              // 1 second
		CleanUp:           true,
		ReservedDiskSpace: 100 * 1024 * 1024, // 50MiB
	}
}

// CachedMmap is a struct containing a cached Mmap file and the file handler.
type CachedMmap struct {
	f *os.File
	m mmap.MMap
}

// MmapCache keeps track of any Mmap files cached in memory and cleans up
// resources as they are unclaimed. This type works similarly to sync.Cond,
// where if you wish to use it you need to lock it.
type MmapCache struct {
	config MmapCacheConfig

	logger log.Modular
	stats  metrics.Type

	tracker    CachedMmap
	cache      map[int]CachedMmap
	inProgress map[int]struct{}

	*sync.Cond
}

// NewMmapCache creates a cache for managing open mmap files.
func NewMmapCache(config MmapCacheConfig, log log.Modular, stats metrics.Type) (*MmapCache, error) {
	f := &MmapCache{
		config:     config,
		logger:     log,
		stats:      stats,
		cache:      make(map[int]CachedMmap),
		inProgress: make(map[int]struct{}),
		Cond:       sync.NewCond(&sync.Mutex{}),
	}

	if err := f.openTracker(); err != nil {
		return nil, err
	}
	return f, nil
}

//------------------------------------------------------------------------------

var (
	// ErrWrongTrackerLength means the length of a read tracker was not correct.
	ErrWrongTrackerLength = errors.New("tracker was unexpected length")

	// ErrNotEnoughSpace means the target disk lacked the space needed for a new
	// file.
	ErrNotEnoughSpace = errors.New("target disk is at capacity")
)

// openTracker opens a tracker file for recording reader and writer indexes.
func (f *MmapCache) openTracker() error {
	defer f.Broadcast()

	var fileInfo os.FileInfo
	var err error

	// Attempt to create the directory tree, ignore errors.
	os.MkdirAll(f.config.Path, 0755)

	fPath := path.Join(f.config.Path, "tracker")

	fileInfo, err = os.Stat(fPath)
	// If the tracker file doesn't exist we make a blank one.
	if os.IsNotExist(err) {
		f.tracker.f, err = os.Create(fPath)
		block := make([]byte, 16)
		if err == nil {
			_, err = f.tracker.f.Write(block)
		}
	} else if err == nil && fileInfo.Size() == 16 {
		f.tracker.f, err = os.OpenFile(fPath, os.O_RDWR, 0644)
	} else if err == nil {
		err = ErrWrongTrackerLength
	}

	// Create the memory mapping.
	if err == nil {
		f.tracker.m, err = mmap.MapRegion(f.tracker.f, 16, mmap.RDWR, 0, 0)
	}
	return err
}

//------------------------------------------------------------------------------

// GetTracker returns the []byte from the tracker file memory mapping.
func (f *MmapCache) GetTracker() []byte {
	return f.tracker.m
}

// Get returns the []byte from a memory mapped file index.
func (f *MmapCache) Get(index int) []byte {
	if c, exists := f.cache[index]; exists {
		return c.m
	}
	return []byte{}
}

// EnsureCached checks that a particular index is cached, and if not then read
// the index, this call blocks until either the index is successfully cached or
// an error occurs.
func (f *MmapCache) EnsureCached(index int) error {
	var cache CachedMmap
	var err error

	// If we are already in the process of caching this index wait until that
	// attempt is finished.
	if _, inProgress := f.inProgress[index]; inProgress {
		for inProgress {
			f.Wait()
			_, inProgress = f.inProgress[index]
		}
	}

	// If the index is cached then return nil.
	if f.IsCached(index) {
		return nil
	}

	// Place the index in our inProgress map to indicate we are caching it on
	// this goroutine.
	f.inProgress[index] = struct{}{}

	// Unlock our mutex as we are about to perform blocking, thread safe
	// operations.
	f.L.Unlock()

	// Prefix index files with "mmap_"
	fPath := path.Join(f.config.Path, fmt.Sprintf("mmap_%v", index))

	// Check if file already exists
	_, err = os.Stat(fPath)
	if os.IsNotExist(err) {
		// If we lack the space needed (reserved space + file size) then return
		// error
		if uint64(f.config.FileSize)+f.config.ReservedDiskSpace >
			disk.TotalRemaining(f.config.Path) {
			err = ErrNotEnoughSpace
		} else {
			// If not then we create it with our configured file size
			if cache.f, err = os.Create(fPath); err == nil {
				block := make([]byte, f.config.FileSize)
				if _, err = cache.f.Write(block); err != nil {
					os.Remove(fPath)
				}
			}
		}
	} else if err == nil {
		cache.f, err = os.OpenFile(fPath, os.O_RDWR, 0644)
	}

	// Lock our mutex again
	f.L.Lock()

	// Defer broadcast and deletion of inProgress flag.
	defer func() {
		delete(f.inProgress, index)
		f.Broadcast()
	}()

	// Create the memory mapping.
	if err == nil {
		cache.m, err = mmap.Map(cache.f, mmap.RDWR, 0)
		if err != nil {
			cache.f.Close()
			os.Remove(fPath)
		} else {
			f.cache[index] = cache
		}
	}
	return err
}

// IsCached returns a bool indicating whether the current memory mapped file
// index is cached.
func (f *MmapCache) IsCached(index int) bool {
	_, exists := f.cache[index]
	return exists
}

// RemoveAll removes all indexes from the cache as well as the tracker.
func (f *MmapCache) RemoveAll() error {
	for _, c := range f.cache {
		c.m.Flush()
		c.m.Unmap()
		c.f.Close()
	}
	f.cache = map[int]CachedMmap{}

	f.tracker.m.Flush()
	f.tracker.m.Unmap()
	f.tracker.f.Close()

	f.tracker = CachedMmap{}
	return nil
}

// Remove removes the index from our cache, the file is NOT deleted.
func (f *MmapCache) Remove(index int) error {
	if c, ok := f.cache[index]; ok {
		delete(f.cache, index)

		// Now we are flushing the cache, this could block so we unlock
		// temporarily.
		f.L.Unlock()
		defer f.L.Lock()

		// TODO: What happens if we subsequently opened the same map file during
		// this operation?
		c.m.Flush()
		c.m.Unmap()
		c.f.Close()
	}
	return nil
}

// Delete deletes the file for an index.
func (f *MmapCache) Delete(index int) error {
	p := path.Join(f.config.Path, fmt.Sprintf("mmap_%v", index))

	// This could be a blocking call, and there's no reason to keep the cache
	// locked.
	f.L.Unlock()
	defer f.L.Lock()

	return os.Remove(p)
}

//------------------------------------------------------------------------------
