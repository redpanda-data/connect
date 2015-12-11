/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package blob

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	mmap "github.com/edsrzf/mmap-go"
)

//--------------------------------------------------------------------------------------------------

// FileCacheConfig - Config options for the FileCache type.
type FileCacheConfig struct {
	Path          string `json:"directory" yaml:"directory"`
	FileSize      int    `json:"file_size" yaml:"file_size"`
	RetryPeriodMS int    `json:"retry_period_ms" yaml:"retry_period_ms"`
	CleanUp       bool   `json:"clean_up" yaml:"clean_up"`
}

// NewFileCacheConfig - Creates a new FileCacheConfig oject with default values.
func NewFileCacheConfig() FileCacheConfig {
	return FileCacheConfig{
		Path:          "",
		FileSize:      250 * 1024 * 1024, // ~ 250MB
		RetryPeriodMS: 1000,              // 1 second
		CleanUp:       true,
	}
}

// CachedMMap - A struct containing a cached MMap file and the file handler.
type CachedMMap struct {
	f *os.File
	m mmap.MMap
}

/*
FileCache - Keeps track of any MMap files cached in memory and cleans up resources as they are
unclaimed. This type works similarly to sync.Cond, where if you wish to use it you need to lock it.
*/
type FileCache struct {
	config FileCacheConfig

	tracker    CachedMMap
	cache      map[int]CachedMMap
	inProgress map[int]struct{}

	*sync.Cond
}

// NewFileCache - Creates a cache for managing open mmap files.
func NewFileCache(config FileCacheConfig) (*FileCache, error) {
	f := &FileCache{
		config:     config,
		cache:      make(map[int]CachedMMap),
		inProgress: make(map[int]struct{}),
		Cond:       sync.NewCond(&sync.Mutex{}),
	}

	if err := f.openTracker(); err != nil {
		return nil, err
	}
	return f, nil
}

//--------------------------------------------------------------------------------------------------

// ErrWrongTrackerLength - The length of a read tracker was not correct.
var ErrWrongTrackerLength = errors.New("tracker was unexpected length")

// openTracker - opens a tracker file for recording reader and writer indexes.
func (f *FileCache) openTracker() error {
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

//--------------------------------------------------------------------------------------------------

// GetTracker - Returns the []byte from the tracker file memory mapping.
func (f *FileCache) GetTracker() []byte {
	return f.tracker.m
}

// Get - Returns the []byte from a memory mapped file index.
func (f *FileCache) Get(index int) []byte {
	if c, exists := f.cache[index]; exists {
		return c.m
	}
	return []byte{}
}

/*
EnsureCached - Check that a particular index is cached, and if not then read the index, this call
blocks until either the index is successfully cached or an error occurs.
*/
func (f *FileCache) EnsureCached(index int) error {
	var cache CachedMMap
	var err error

	// If we are already in the process of caching this index wait until that attempt is finished.
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

	// Place the index in our inProgress map to indicate we are caching it on this goroutine.
	f.inProgress[index] = struct{}{}

	// Unlock our mutex as we are about to perform blocking, thread safe operations.
	f.L.Unlock()

	// Prefix index files with "mmap_"
	fPath := path.Join(f.config.Path, fmt.Sprintf("mmap_%v", index))

	// Check if file already exists
	_, err = os.Stat(fPath)
	if os.IsNotExist(err) {
		// If not then we create it with our configured file size
		cache.f, err = os.Create(fPath)
		block := make([]byte, f.config.FileSize)
		_, err = cache.f.Write(block)
	} else {
		cache.f, err = os.OpenFile(fPath, os.O_RDWR, 0644)
	}

	// Lock our mutex again
	f.L.Lock()

	// Defer broadcast and deletion of inProgress flag.
	defer f.Broadcast()
	defer delete(f.inProgress, index)

	// Create the memory mapping.
	if err == nil {
		cache.m, err = mmap.Map(cache.f, mmap.RDWR, 0)
		if err != nil {
			cache.f.Close()
		} else {
			f.cache[index] = cache
		}
	}
	return err
}

// IsCached - Returns a bool indicating whether the current memory mapped file index is cached.
func (f *FileCache) IsCached(index int) bool {
	_, exists := f.cache[index]
	return exists
}

// RemoveAll - Removes all indexes from the cache as well as the tracker.
func (f *FileCache) RemoveAll() error {
	for _, c := range f.cache {
		c.m.Flush()
		c.m.Unmap()
		c.f.Close()
	}
	f.cache = map[int]CachedMMap{}

	f.tracker.m.Flush()
	f.tracker.m.Unmap()
	f.tracker.f.Close()

	f.tracker = CachedMMap{}
	return nil
}

// Remove - Removes the index from our cache, the file is NOT deleted.
func (f *FileCache) Remove(index int) error {
	if c, ok := f.cache[index]; ok {
		c.m.Flush()
		c.m.Unmap()
		c.f.Close()

		delete(f.cache, index)
	}
	return nil
}

// Delete - Deletes the file for an index.
func (f *FileCache) Delete(index int) error {
	p := path.Join(f.config.Path, fmt.Sprintf("mmap_%v", index))
	return os.Remove(p)
}

//--------------------------------------------------------------------------------------------------
