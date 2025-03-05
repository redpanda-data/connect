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
// and modified specifically for this package.

package wal

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type file interface {
	io.Closer
	io.ReadWriteSeeker
	Sync() error
}

// WALOptions are options for the WAL
type WALOptions struct {
	// LogDir is where the wal logs will be stored
	LogDir string
	// Maximum size in bytes for each file
	MaxLogSize int64
	// Maximum age in time for each file
	MaxLogAge time.Duration

	Log *service.Logger
}

type SegmentID int

const (
	lengthBufferSize   = 4
	checkSumBufferSize = 4
	// InvalidSegmentID is an segment ID that will never be used
	InvalidSegmentID SegmentID = -1
)

var (
	ErrCannotDeleteCurrentSegment = errors.New("cannot delete current segment")
)

// A Write Ahead Log (WAL) is a data structure used to record changes to a database or
// any persistent storage system in a sequential and durable manner. This allows for
// crash recovery and data integrity.
//
// This struct is not thread safe and requires external synchronization.
type WriteAheadLog struct {
	logFileName            string
	maxSegmentSize         int64
	maxSegmentAge          time.Duration
	currentSegment         file
	currentSegmentSize     int64
	currentSegmentDeadline time.Time
	currentSegmentID       SegmentID

	closedSegmentCount int

	log *service.Logger

	bufWriter *bufio.Writer
	sizeBuf   [lengthBufferSize]byte
}

// NewWriteAheadLog creates a new instance of the WriteAheadLog with the provided options.
func NewWriteAheadLog(opts *WALOptions) (*WriteAheadLog, error) {
	if opts.MaxLogAge < time.Millisecond {
		return nil, fmt.Errorf(
			"unable to create WAL log segments with shorter time period than 1 millisecond: %v",
			opts.MaxLogAge,
		)
	}
	if opts.MaxLogSize < humanize.KiByte {
		return nil, fmt.Errorf(
			"unable to create WAL log segments smaller than 1KiB: %s",
			humanize.Bytes(uint64(opts.MaxLogSize)),
		)
	}

	walLogFilePrefix := filepath.Join(opts.LogDir, "wal")
	wal := &WriteAheadLog{
		logFileName:    walLogFilePrefix,
		maxSegmentSize: opts.MaxLogSize,
		maxSegmentAge:  opts.MaxLogAge,
		log:            opts.Log,
	}
	err := wal.openExistingOrCreateNew(opts.LogDir)
	if err != nil {
		return nil, err
	}

	return wal, nil
}

func (wal *WriteAheadLog) openExistingOrCreateNew(dirPath string) error {
	err := os.MkdirAll(dirPath, 0777)
	if err != nil {
		return err
	}

	// Fetch all the file names in the path and sort them
	logFiles, err := wal.loadAllSegmentsFromDisk()
	if err != nil {
		return err
	}

	if len(logFiles) == 0 {
		// Create the first log file
		now := time.Now()
		firstLogFileName := fmt.Sprintf("%s.%d.%d", wal.logFileName, 0, now.UnixMilli())
		file, err := os.OpenFile(firstLogFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		// Set default values since this is the first log we are opening
		wal.currentSegment = file
		wal.bufWriter = bufio.NewWriter(file)
		wal.currentSegmentSize = 0
		wal.closedSegmentCount = 0
		wal.currentSegmentID = 0
		wal.currentSegmentDeadline = now.Add(wal.maxSegmentAge)
	} else {
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
		wal.currentSegment = file
		wal.currentSegment.Seek(0, io.SeekEnd)
		wal.bufWriter = bufio.NewWriter(file)
		wal.currentSegmentSize = fi.Size()
		wal.closedSegmentCount = len(logFiles) - 1
		wal.currentSegmentID, wal.currentSegmentDeadline, _ = wal.parseSegmentName(fileName)
		wal.currentSegmentDeadline = wal.currentSegmentDeadline.Add(wal.maxSegmentAge)
		wal.log.Tracef(
			"opened wal latestSegment=%d, closedSegmentCount=%d",
			fileName,
			wal.closedSegmentCount,
		)
	}
	return nil
}

// Append appends the provided data to the log, ensuring log rotation and syncing if necessary.
func (wal *WriteAheadLog) Append(data []byte) (SegmentID, error) {
	entrySize := lengthBufferSize + checkSumBufferSize + len(data)

	if wal.currentSegmentSize+int64(entrySize) > wal.maxSegmentSize || time.Now().After(wal.currentSegmentDeadline) {
		// Flushing all the in-memory changes to disk, and rotating the log
		if err := wal.Sync(); err != nil {
			return 0, err
		}
		if err := wal.RotateSegment(); err != nil {
			return 0, err
		}
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

	wal.currentSegmentSize += int64(entrySize)
	return wal.currentSegmentID, nil
}

// Close closes the underneath storage file, it flushes data remaining in the memory buffer
// and file systems in-memory copy of recently written data to file to ensure persistent commit of the log
func (wal *WriteAheadLog) Close() error {
	// Flush all data to disk
	if err := wal.Sync(); err != nil {
		return err
	}

	return wal.currentSegment.Close()
}

// GetOffset returns the current log offset.
func (wal *WriteAheadLog) CurrentSegment() SegmentID {
	return wal.currentSegmentID
}

// Sync writes all the data to the disk ensuring data durability.
// Since it is a expensive call, calling this often will slow down the throughput.
func (wal *WriteAheadLog) Sync() error {
	err := wal.bufWriter.Flush()
	if err != nil {
		return err
	}
	return wal.currentSegment.Sync()
}

// RotateSegment closes the current file, opens a new one.
func (wal *WriteAheadLog) RotateSegment() error {
	if err := wal.currentSegment.Close(); err != nil {
		return err
	}

	now := time.Now()
	newFileName := fmt.Sprintf("%s.%d.%d", wal.logFileName, wal.currentSegmentID+1, now.UnixMilli())
	file, err := os.OpenFile(newFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	wal.currentSegmentID++
	wal.closedSegmentCount++

	wal.currentSegment = file
	wal.bufWriter.Reset(file)
	wal.currentSegmentSize = 0
	wal.currentSegmentDeadline = now.Add(wal.maxSegmentAge)
	return nil
}

// DeleteSegment deletes an old closed segment.
func (wal *WriteAheadLog) DeleteSegment(id SegmentID) error {
	if id == wal.currentSegmentID {
		return ErrCannotDeleteCurrentSegment
	}
	fileName, err := filepath.Glob(fmt.Sprintf("%s.%d.*", wal.logFileName, id))
	if err == nil && len(fileName) == 1 {
		err = os.Remove(fileName[0])
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err == nil && len(fileName) > 1 {
		return fmt.Errorf("cannot delete multiple segments with the same ID: %s", strings.Join(fileName, ", "))
	}
	return err
}

func (wal *WriteAheadLog) parseSegmentName(filename string) (id SegmentID, start time.Time, ok bool) {
	splits := strings.SplitN(filepath.Base(filename), ".", 3)
	if len(splits) != 3 {
		return id, start, false
	}
	rawID, err := strconv.Atoi(splits[1])
	if err != nil {
		return id, start, false
	}
	unixMilli, err := strconv.Atoi(splits[2])
	if err != nil {
		return id, start, false
	}
	return SegmentID(rawID), time.UnixMilli(int64(unixMilli)), true
}

func (wal *WriteAheadLog) loadAllSegmentsFromDisk() ([]string, error) {
	logFiles, err := filepath.Glob(wal.logFileName + "*")
	if err != nil {
		return nil, err
	}
	slices.SortFunc(logFiles, func(a, b string) int {
		aID, _, _ := wal.parseSegmentName(a)
		bID, _, _ := wal.parseSegmentName(b)
		return cmp.Compare(aID, bID)
	})
	if len(logFiles) != 0 {
		_, _, ok := wal.parseSegmentName(logFiles[0])
		if !ok {
			return nil, fmt.Errorf("invalid segment file name: %s", logFiles[0])
		}
	}
	return logFiles, nil
}

// Replay replays log entries starting from the start of the log, invoking the provided callback.
func (wal *WriteAheadLog) Replay(f func(SegmentID, []byte) error) error {
	logFiles, err := wal.loadAllSegmentsFromDisk()
	if err != nil {
		return err
	}
	var bufReader bufio.Reader
	for _, logFile := range logFiles {
		segmentID, _, _ := wal.parseSegmentName(logFile)
		file, err := os.Open(logFile)
		if err != nil {
			return err
		}
		bufReader.Reset(file)
		err = wal.iterateFile(bufReader, func(b []byte) error { return f(segmentID, b) })
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

		readBytes := make([]byte, size)
		_, err = io.ReadFull(&bufReader, readBytes)
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
	}
	if err == io.EOF {
		return nil
	}
	return err
}
