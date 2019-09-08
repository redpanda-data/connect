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
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// MmapBufferConfig is config options for a memory-map based buffer reader.
type MmapBufferConfig MmapCacheConfig

// NewMmapBufferConfig creates a MmapBufferConfig oject with default values.
func NewMmapBufferConfig() MmapBufferConfig {
	return MmapBufferConfig(NewMmapCacheConfig())
}

// MmapBuffer is a buffer implemented around rotated memory mapped files.
type MmapBuffer struct {
	config MmapBufferConfig
	cache  *MmapCache

	logger log.Modular
	stats  metrics.Type

	retryPeriod time.Duration

	mCacheErr metrics.StatCounter

	readFrom  int
	readIndex int

	writtenTo  int
	writeIndex int

	closed bool
}

// NewMmapBuffer creates a memory-map based buffer.
func NewMmapBuffer(config MmapBufferConfig, log log.Modular, stats metrics.Type) (*MmapBuffer, error) {
	cache, err := NewMmapCache(MmapCacheConfig(config), log, stats)
	if err != nil {
		return nil, fmt.Errorf("MMAP Cache: %v", err)
	}
	cache.L.Lock()
	defer cache.L.Unlock()

	f := &MmapBuffer{
		config:     config,
		cache:      cache,
		logger:     log,
		stats:      stats,
		mCacheErr:  stats.GetCounter("open.error"),
		readFrom:   0,
		readIndex:  0,
		writtenTo:  0,
		writeIndex: 0,
		closed:     false,
	}

	if tout := config.RetryPeriod; len(tout) > 0 {
		var err error
		if f.retryPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse retry period string: %v", err)
		}
	}

	f.readTracker()

	f.logger.Infof("Storing messages to file in: %s\n", f.config.Path)

	// Try to ensure both the starting write and read indexes are cached
	if err = cache.EnsureCached(f.readIndex); err != nil {
		return nil, fmt.Errorf("MMAP index read: %v", err)
	}
	if err = cache.EnsureCached(f.writeIndex); err != nil {
		log.Errorf("MMAP index write: %v, benthos will block writes until this is resolved.\n", err)
	}

	go f.cacheManagerLoop(&f.writeIndex)
	go f.cacheManagerLoop(&f.readIndex)

	return f, nil
}

//------------------------------------------------------------------------------

// readTracker reads our cached values from the tracker file for recording
// reader/writer indexes.
func (f *MmapBuffer) readTracker() {
	if !f.closed {
		trackerBlock := f.cache.GetTracker()

		f.writeIndex = readMessageSize(trackerBlock, 0)
		f.writtenTo = readMessageSize(trackerBlock, 4)
		f.readIndex = readMessageSize(trackerBlock, 8)
		f.readFrom = readMessageSize(trackerBlock, 12)
	}
}

// writeTracker writes our current state to the tracker.
func (f *MmapBuffer) writeTracker() {
	if !f.closed {
		trackerBlock := f.cache.GetTracker()

		writeMessageSize(trackerBlock, 0, f.writeIndex)
		writeMessageSize(trackerBlock, 4, f.writtenTo)
		writeMessageSize(trackerBlock, 8, f.readIndex)
		writeMessageSize(trackerBlock, 12, f.readFrom)
	}
}

//------------------------------------------------------------------------------

// cacheManagerLoop continuously checks whether the cache contains maps of our
// next indexes.
func (f *MmapBuffer) cacheManagerLoop(indexPtr *int) {
	bootstrapped := false

	loop := func() bool {
		f.cache.L.Lock()
		defer f.cache.L.Unlock()

		if f.closed {
			return false
		}

		targetIndex := *indexPtr
		if bootstrapped {
			targetIndex++
		}

		if err := f.cache.EnsureCached(targetIndex); err != nil {
			// Failed to read, log the error and wait before trying again.
			f.logger.Errorf("Failed to cache mmap file for index %v: %v\n", targetIndex, err)
			f.mCacheErr.Incr(1)

			f.cache.L.Unlock()
			<-time.After(f.retryPeriod)
			f.cache.L.Lock()
		} else if !bootstrapped {
			bootstrapped = true
		} else if *indexPtr < targetIndex {
			// NOTE: It's possible that while we were waiting for ensure target
			// was indexed the actual index caught up with us, in which case we
			// should loop straight back into ensuring the new index rather than
			// waiting.
			f.cache.Wait()
		}
		return true
	}
	for loop() {
	}
}

//------------------------------------------------------------------------------

// backlog reads the current backlog of messages stored.
func (f *MmapBuffer) backlog() int {
	// NOTE: For speed, the following calculation assumes that all mmap files
	// are the size of limit.
	return ((f.writeIndex - f.readIndex) * f.config.FileSize) + f.writtenTo - f.readFrom
}

//------------------------------------------------------------------------------

// CloseOnceEmpty closes the mmap buffer once the backlog reaches 0.
func (f *MmapBuffer) CloseOnceEmpty() {
	defer func() {
		f.cache.L.Unlock()
		f.Close()
	}()
	f.cache.L.Lock()

	// Until the backlog is cleared.
	for f.backlog() > 0 {
		// Wait for a broadcast from our reader.
		f.cache.Wait()
	}
}

// Close unblocks any blocked calls and prevents further writing to the block.
func (f *MmapBuffer) Close() {
	f.cache.L.Lock()
	f.closed = true
	f.cache.Broadcast()
	f.cache.L.Unlock()

	f.cache.L.Lock()
	f.cache.RemoveAll()
	f.cache.L.Unlock()
}

// ShiftMessage removes the last message. Returns the backlog count.
func (f *MmapBuffer) ShiftMessage() (int, error) {
	f.cache.L.Lock()
	defer func() {
		f.writeTracker()
		f.cache.Broadcast()
		f.cache.L.Unlock()
	}()

	if !f.closed && f.cache.IsCached(f.readIndex) {
		msgSize := readMessageSize(f.cache.Get(f.readIndex), f.readFrom)
		f.readFrom = f.readFrom + int(msgSize) + 4
	}
	return f.backlog(), nil
}

// NextMessage reads the next message, blocks until there's something to read.
func (f *MmapBuffer) NextMessage() (types.Message, error) {
	f.cache.L.Lock()
	defer func() {
		f.writeTracker()
		f.cache.Broadcast()
		f.cache.L.Unlock()
	}()

	// If reader is the same position as the writer then we wait.
	for f.writeIndex == f.readIndex && f.readFrom == f.writtenTo && !f.closed {
		f.cache.Wait()
	}
	if f.closed {
		return nil, types.ErrTypeClosed
	}

	index := f.readFrom
	block := f.cache.Get(f.readIndex)

	msgSize := readMessageSize(block, index)

	// Messages are written in a contiguous array of bytes, therefore when the
	// writer reaches the end it will zero the next four bytes (zero size
	// message) to indicate to the reader that it should move onto the next
	// file.
	for msgSize <= 0 {
		// If we need to switch
		for !f.cache.IsCached(f.readIndex+1) && !f.closed {
			// Block until the next file is ready to read.
			f.cache.Wait()
		}
		if f.closed {
			return nil, types.ErrTypeClosed
		}

		// If we are meant to delete files as we are done with them
		if f.config.CleanUp {
			// The delete is done asynchronously as it has no impact on the
			// reader
			go func(prevIndex int) {
				f.cache.L.Lock()
				defer f.cache.L.Unlock()

				// Remove and delete the previous index
				f.cache.Remove(prevIndex)
				f.cache.Delete(prevIndex)
			}(f.readIndex)
		}

		f.readIndex = f.readIndex + 1
		f.readFrom = 0

		block = f.cache.Get(f.readIndex)
		index = 0

		f.cache.Broadcast()

		// If reader is the same position as the writer then we wait.
		for f.writeIndex == f.readIndex && f.readFrom == f.writtenTo && !f.closed {
			f.cache.Wait()
		}
		if f.closed {
			return nil, types.ErrTypeClosed
		}

		// Read the next message.
		msgSize = readMessageSize(block, index)
	}

	index = index + 4
	if index+int(msgSize) > len(block) {
		return nil, types.ErrBlockCorrupted
	}

	return message.FromBytes(block[index : index+int(msgSize)])
}

// PushMessage pushes a new message, returns the backlog count.
func (f *MmapBuffer) PushMessage(msg types.Message) (int, error) {
	f.cache.L.Lock()
	defer func() {
		f.writeTracker()
		f.cache.Broadcast()
		f.cache.L.Unlock()
	}()

	blob := message.ToBytes(msg)
	index := f.writtenTo

	if len(blob)+4 > f.config.FileSize {
		return 0, types.ErrMessageTooLarge
	}

	for !f.cache.IsCached(f.writeIndex) && !f.closed {
		f.cache.Wait()
	}
	if f.closed {
		return 0, types.ErrTypeClosed
	}

	block := f.cache.Get(f.writeIndex)

	// If we can't fit our next message in the remainder of the buffer we will
	// move onto the next file. In order to prevent the reader from reading
	// garbage we set the next message size to 0, which tells the reader to loop
	// back to index 0.
	for len(blob)+4+index > len(block) {
		// Write zeroes into remainder of the block.
		for i := index; i < len(block) && i < index+4; i++ {
			block[i] = byte(0)
		}

		// Wait until our next file is ready.
		for !f.cache.IsCached(f.writeIndex+1) && !f.closed {
			f.cache.Wait()
		}
		if f.closed {
			return 0, types.ErrTypeClosed
		}

		// If the read index is behind then don't keep our writer block cached.
		if f.readIndex < f.writeIndex-1 {
			// But do not block while doing so.
			go func(prevIndex int) {
				f.cache.L.Lock()
				defer f.cache.L.Unlock()

				// Remove the previous index from cache.
				f.cache.Remove(prevIndex)
			}(f.writeIndex)
		}

		// Set counters
		f.writeIndex = f.writeIndex + 1
		f.writtenTo = 0

		block = f.cache.Get(f.writeIndex)
		index = 0

		f.cache.Broadcast()
	}

	writeMessageSize(block, index, len(blob))
	copy(block[index+4:], blob)

	// Move writtenTo ahead.
	f.writtenTo = (index + len(blob) + 4)

	return f.backlog(), nil
}

//------------------------------------------------------------------------------
