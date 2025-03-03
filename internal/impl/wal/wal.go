// MIT License
//
// Copyright (c) 2023 Aarthik Rao
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// This singular file is a fork of https://github.com/aarthikrao/wal, which is simplified
// modified specifically for this package.

package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type file interface {
	io.Closer
	io.ReadWriteSeeker
	Sync() error
}

type WALOptions struct {
	// LogDir is where the wal logs will be stored
	LogDir string

	// Maximum size in bytes for each file
	MaxLogSize int64

	// The entire wal is broken down into smaller segments.
	// This will be helpful during log rotation and management
	// maximum number of log segments
	MaxSegments int

	MaxWaitBeforeSync time.Duration
	SyncMaxBytes      int64

	Log *zap.Logger
}

const (
	lengthBufferSize   = 4
	checkSumBufferSize = 4
)

// A Write Ahead Log (WAL) is a data structure used to record changes to a database or
// any persistent storage system in a sequential and durable manner. This allows for
// crash recovery and data integrity.
type WriteAheadLog struct {
	logFileName  string
	file         file
	mu           sync.Mutex
	maxLogSize   int64
	logSize      int64
	segmentCount int

	maxSegments      int
	currentSegmentID int

	log *zap.Logger

	curOffset int64
	bufWriter *bufio.Writer
	sizeBuf   [lengthBufferSize]byte

	// syncTimer is used to wait for either the specified time interval
	// or until a syncMaxBytes amount of data has been accumulated before Syncing to the disk
	syncTimer         *time.Ticker
	maxWaitBeforeSync time.Duration // TODO: Yet to implement
	syncMaxBytes      int64
}

// NewWriteAheadLog creates a new instance of the WriteAheadLog with the provided options.
func NewWriteAheadLog(opts *WALOptions) (*WriteAheadLog, error) {
	walLogFilePrefix := opts.LogDir + "wal"

	wal := &WriteAheadLog{
		logFileName:       walLogFilePrefix,
		maxLogSize:        opts.MaxLogSize,
		maxSegments:       opts.MaxSegments,
		log:               opts.Log,
		syncMaxBytes:      opts.SyncMaxBytes,
		syncTimer:         time.NewTicker(opts.MaxWaitBeforeSync),
		maxWaitBeforeSync: opts.MaxWaitBeforeSync,
	}
	err := wal.openExistingOrCreateNew(opts.LogDir)
	if err != nil {
		return nil, err
	}

	go wal.keepSyncing()

	return wal, nil
}

// isDirectoryEmpty returns true if the directory, false otherwise.
func isDirectoryEmpty(dirPath string) (bool, error) {
	// Open the directory
	dir, err := os.Open(dirPath)
	if err != nil {
		return false, err
	}
	defer dir.Close()

	// Read the directory contents
	fileList, err := dir.ReadDir(1) // Read the first entry
	if err != nil && err != io.EOF {
		return false, err
	}

	// If the list of files is empty, the directory is empty
	return len(fileList) == 0, nil
}

func ensureDir(dirName string) error {
	err := os.Mkdir(dirName, 0777)
	if err == nil {
		return nil
	}
	if os.IsExist(err) {
		// check that the existing path is a directory
		info, err := os.Stat(dirName)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("path exists but is not a directory")
		}
		return nil
	}
	return err
}

func (wal *WriteAheadLog) openExistingOrCreateNew(dirPath string) error {
	// Create the directory if it doesnt exist
	err := ensureDir(dirPath)
	if err != nil {
		return err
	}

	empty, err := isDirectoryEmpty(dirPath)
	if err != nil {
		return err
	}

	if empty {

		// Create the first log file
		firstLogFileName := wal.logFileName + ".0.0" // prefix + . {segmentID} + . {starting_offset}
		file, err := os.OpenFile(firstLogFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}

		// Set default values since this is the first log we are opening
		wal.file = file
		wal.bufWriter = bufio.NewWriter(file)
		wal.logSize = 0 // since fi.Size() is 0 for newly created file
		wal.segmentCount = 0
		wal.currentSegmentID = 0
		wal.curOffset = -1

	} else {
		// Fetch all the file names in the path and sort them
		logFiles, err := filepath.Glob(wal.logFileName + "*")
		if err != nil {
			return err
		}
		sort.Strings(logFiles)

		// open the last file
		fileName := logFiles[len(logFiles)-1]
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}

		fi, err := file.Stat()
		if err != nil {
			return err
		}

		// Find the current segment count and the latest offset from file name
		s := strings.Split(fileName, ".")
		lastSegment, err := strconv.Atoi(s[1])
		if err != nil {
			return err
		}

		latestOffset, err := strconv.Atoi(s[2])
		if err != nil {
			return err
		}
		offset := int64(latestOffset)

		// Go to the end of file and calculate the offset
		file.Seek(0, io.SeekStart)
		bufReader := bufio.NewReader(file)
		n, err := wal.seekOffset(math.MaxInt, offset, *bufReader)
		if err != nil && err != io.EOF {
			return err
		}
		offset += n

		wal.file = file
		file.Seek(0, io.SeekEnd)
		wal.bufWriter = bufio.NewWriter(file)
		wal.logSize = fi.Size()
		wal.currentSegmentID = lastSegment
		wal.curOffset = offset
		wal.segmentCount = len(logFiles) - 1

		wal.log.Info("appending wal",
			zap.String("file", fileName),
			zap.Int64("latestOffset", wal.curOffset),
			zap.Int("latestSegment", lastSegment),
			zap.Int("segmentCount", wal.segmentCount),
		)

	}

	return nil
}

// keepSyncing periodically triggers a synchronous write to the disk to ensure data durability.
func (wal *WriteAheadLog) keepSyncing() {
	for {
		<-wal.syncTimer.C

		wal.mu.Lock()
		err := wal.Sync()
		wal.mu.Unlock()

		if err != nil {
			wal.log.Error("Error while performing sync", zap.Error(err))
		}
	}
}

// resetTimer resets the synchronization timer.
func (wal *WriteAheadLog) resetTimer() {
	wal.syncTimer.Reset(wal.maxWaitBeforeSync)
}

// Write appends the provided data to the log, ensuring log rotation and syncing if necessary.
func (wal *WriteAheadLog) Write(data []byte) (int64, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	entrySize := lengthBufferSize + checkSumBufferSize + len(data)

	if wal.logSize+int64(entrySize) > wal.maxLogSize {
		// Flushing all the in-memory changes to disk, and rotating the log
		if err := wal.Sync(); err != nil {
			return 0, err
		}

		if err := wal.rotateLog(); err != nil {
			return 0, err
		}

		wal.resetTimer()
	}

	_, err := wal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Write the size prefix to the buffer
	binary.LittleEndian.PutUint32(wal.sizeBuf[:], uint32(len(data)))
	if _, err := wal.bufWriter.Write(wal.sizeBuf[:]); err != nil {
		return 0, err
	}

	// Calculate the checksum and append it to the buffer. We reuse sizeBuf here for checksum also since it is 4 byte
	checksum := crc32.ChecksumIEEE(data)
	binary.LittleEndian.PutUint32(wal.sizeBuf[:], checksum)
	if _, err := wal.bufWriter.Write(wal.sizeBuf[:]); err != nil {
		return 0, err
	}

	// Write data payload to the buffer
	if _, err := wal.bufWriter.Write(data); err != nil {
		return 0, err
	}

	wal.logSize += int64(entrySize)
	wal.curOffset++
	return wal.curOffset, nil
}

// Close closes the underneath storage file, it flushes data remaining in the memory buffer
// and file systems in-memory copy of recently written data to file to ensure persistent commit of the log
func (wal *WriteAheadLog) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Flush all data to disk
	if err := wal.Sync(); err != nil {
		return err
	}

	return wal.file.Close()
}

// GetOffset returns the current log offset.
func (wal *WriteAheadLog) GetOffset() int64 {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.curOffset
}

// Sync writes all the data to the disk ensuring data durability.
// Since it is a expensive call, calling this often will slow down the throughput.
func (wal *WriteAheadLog) Sync() error {
	err := wal.bufWriter.Flush()
	if err != nil {
		return err
	}
	return wal.file.Sync()
}

// rotateLog closes the current file, opens a new one.
// It also cleans up the oldest log files if the number of log files are greater than maxSegments
func (wal *WriteAheadLog) rotateLog() error {
	if err := wal.file.Close(); err != nil {
		return err
	}

	if wal.segmentCount >= wal.maxSegments {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}

	}
	wal.currentSegmentID++
	wal.segmentCount++

	newFileName := fmt.Sprintf("%s.%d.%d", wal.logFileName, wal.currentSegmentID, wal.curOffset)

	file, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	wal.file = file
	wal.bufWriter.Reset(file)
	wal.logSize = 0
	return nil
}

// deleteOldestSegment removes the oldest log file if the number of log files exceeds the limit.
func (wal *WriteAheadLog) deleteOldestSegment() error {
	oldestSegment := fmt.Sprintf("%s.%d.*", wal.logFileName, wal.currentSegmentID-wal.maxSegments)

	files, err := filepath.Glob(oldestSegment)
	if err != nil {
		return err
	}

	// We will have only one file to delete but still ...
	for _, f := range files {
		wal.log.Info("Removing wal file", zap.String("segment", f))
		if err := os.Remove(f); err != nil {
			return err
		}

		// Update the segment count
		wal.segmentCount--
	}

	return nil
}

// findStartingLogFile searches for the starting log file during replay.
func (wal *WriteAheadLog) findStartingLogFile(offset int64, files []string) (i int, previousOffset int64, err error) {
	i = -1
	for index, file := range files {
		parts := strings.Split(file, ".")
		startingOffsetStr := parts[len(parts)-1]
		startingOffset, err := strconv.ParseInt(startingOffsetStr, 10, 64)
		if err != nil {
			return -1, -1, err
		}
		if previousOffset <= offset && offset <= startingOffset {
			return index, previousOffset, nil
		}
		previousOffset = startingOffset
	}
	return -1, -1, errors.New("offset doesn't exsist")
}

// seekOffset seeks to a specific offset in the log during replay.
func (wal *WriteAheadLog) seekOffset(offset int64, startingOffset int64, file bufio.Reader) (n int64, err error) {
	var readBytes []byte
	for startingOffset < offset {
		readBytes, err = file.Peek(lengthBufferSize)
		if err != nil {
			break
		}
		dataSize := binary.LittleEndian.Uint32(readBytes)
		_, err = file.Discard(lengthBufferSize + int(dataSize))
		if err != nil {
			break
		}
		startingOffset++ // Check logic
		n++
	}
	return n, err
}

// Replay replays log entries starting from the specified offset, invoking the provided callback.
func (wal *WriteAheadLog) Replay(offset int64, f func([]byte) error) error {
	logFiles, err := filepath.Glob(wal.logFileName + "*")
	if err != nil {
		return err
	}
	sort.Strings(logFiles)
	index, startingOffset, err := wal.findStartingLogFile(offset, logFiles)
	if err != nil {
		return err
	}
	var bufReader bufio.Reader
	for i, logFile := range logFiles[index:] {
		file, err := os.Open(logFile)
		if err != nil {
			return err
		}
		bufReader.Reset(file)
		if i == 0 {
			if _, err = wal.seekOffset(offset, startingOffset, bufReader); err != nil {
				return err
			}
		}
		err = wal.iterateFile(bufReader, f)
		if err != nil {
			file.Close()
			return err
		}
		file.Close()
	}

	return nil
}

// iterateFile iterates through log entries in a file and invokes the provided callback.
func (wal *WriteAheadLog) iterateFile(bufReader bufio.Reader, callback func([]byte) error) error {
	var readBytes []byte
	var err error
	for err == nil {

		readBytes, err = bufReader.Peek(lengthBufferSize)
		if err != nil {
			break
		}
		usize := binary.LittleEndian.Uint32(readBytes)
		size := int(usize)
		_, err = bufReader.Discard(lengthBufferSize)
		if err != nil {
			break
		}

		readBytes, err = bufReader.Peek(checkSumBufferSize)
		if err != nil {
			break
		}
		diskChecksum := binary.LittleEndian.Uint32(readBytes)
		_, err = bufReader.Discard(checkSumBufferSize)
		if err != nil {
			break
		}

		readBytes, err = bufReader.Peek(size)
		if err != nil {
			break
		}

		dataChecksum := crc32.ChecksumIEEE(readBytes)
		if dataChecksum != diskChecksum {
			return errors.New("checksum mismatch")
		}

		if err = callback(readBytes); err != nil {
			break
		}
		_, err = bufReader.Discard(size)
	}
	if err == io.EOF {
		return nil
	}
	return err
}
