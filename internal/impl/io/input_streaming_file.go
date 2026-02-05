// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// splitKeepNewline is a custom scanner split function that keeps delimiters
var splitKeepNewline = func(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i+1], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// StreamingFileInputConfig holds configuration for the streaming file input.
type StreamingFileInputConfig struct {
	Path            string
	MaxBufferSize   int
	MaxLineSize     int
	PollInterval    time.Duration
	DisableFSNotify bool // When true, use polling only (more CPU-efficient at high write rates)
}

// FilePosition represents the current read position in a file.
// This is exposed as metadata on messages so users can implement their own
// persistence logic using Bento's cache system in their pipelines.
type FilePosition struct {
	FilePath   string `json:"file_path"`
	Inode      uint64 `json:"inode"`
	ByteOffset int64  `json:"byte_offset"`
}

// StreamingFileInput reads from a file continuously like 'tail -F'.
type StreamingFileInput struct {
	config        StreamingFileInputConfig
	logger        *service.Logger
	position      FilePosition
	positionMutex sync.RWMutex
	file          *os.File
	fileMu        sync.RWMutex
	reader        *bufio.Reader
	buffer        chan []byte
	stopCh        chan struct{}
	readLoopDone  chan struct{}
	wg            sync.WaitGroup
	connected     bool
	connMutex     sync.RWMutex
	inFlightCount atomic.Int64
	bufferClosed  atomic.Bool
	watcher       *fsnotify.Watcher
}

// NewStreamingFileInput creates a new streaming file input.
func NewStreamingFileInput(cfg StreamingFileInputConfig, logger *service.Logger) (*StreamingFileInput, error) {
	if cfg.Path == "" {
		return nil, errors.New("path is required")
	}
	if cfg.MaxBufferSize <= 0 {
		cfg.MaxBufferSize = 1000
	}
	if cfg.MaxLineSize <= 0 {
		cfg.MaxLineSize = 1024 * 1024 // 1MB default
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second // Default 1s polling, like tail -F
	}

	return &StreamingFileInput{
		config:       cfg,
		logger:       logger,
		buffer:       make(chan []byte, cfg.MaxBufferSize),
		stopCh:       make(chan struct{}),
		readLoopDone: make(chan struct{}),
		position: FilePosition{
			FilePath: cfg.Path,
		},
	}, nil
}

func streamingFileInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Local").
		Summary("Streaming file input with log rotation and truncation handling").
		Description(`
Reads from a file continuously with automatic handling of log rotation and truncation.

## Core Features

- **Log Rotation**: Detects when a file is rotated (renamed/recreated) via inode changes and seamlessly switches to the new file
- **Truncation**: Detects when a file is truncated and resets to read from the beginning
- **Position Metadata**: Each message includes file position metadata (path, inode, byte_offset) that can be used with Bento's cache system to implement custom persistence

## Position Tracking

This component exposes file position as metadata on each message. To implement crash recovery, you can:

1. Store the position in a cache on each message
2. On startup, read the cached position and use a processor to filter already-processed lines

This approach keeps the input stateless while enabling persistence through pipeline composition.

## Metadata Fields

Each message includes the following metadata:

- ` + "`streaming_file_path`" + ` - The file path being read
- ` + "`streaming_file_inode`" + ` - The file's inode (for rotation detection)
- ` + "`streaming_file_offset`" + ` - Byte offset where this line started

## Performance Considerations

By default, this component uses polling-only mode for better CPU efficiency at high write rates.
This is based on findings from large-scale deployments where inotify/fsnotify can cause significant
CPU overhead when files are written to frequently (each write triggers an event, leading to excessive
fstat calls). See ` + "`disable_fsnotify`" + ` option below.

For low-volume log files where you want sub-second latency, you can enable fsnotify by setting
` + "`disable_fsnotify: false`" + `.

### Platform Limitations

When fsnotify is enabled:

- **NFS/Network Filesystems**: fsnotify does not work reliably on NFS or other network filesystems
- **Supported Platforms**: Linux (inotify), macOS (FSEvents), Windows (ReadDirectoryChangesW), BSD variants (kqueue)
- **Container Environments**: Ensure the file path is mounted from the host, not a container-internal path
`).
		Field(service.NewStringField("path").
			Description("Path to the file to read from").
			Example("/var/log/app.log")).
		Field(service.NewIntField("max_buffer_size").
			Description("Maximum number of lines to buffer").
			Default(1000).
			Example(1000)).
		Field(service.NewIntField("max_line_size").
			Description("Maximum line size in bytes to prevent OOM").
			Default(1048576).
			Example(1048576)).
		Field(service.NewDurationField("poll_interval").
			Description("How often to poll the file for new data. This is the primary mechanism for detecting new data. Lower values mean lower latency but higher CPU usage.").
			Default("1s").
			Example("1s").
			Example("200ms")).
		Field(service.NewBoolField("disable_fsnotify").
			Description("When true (default), only use polling to detect file changes. This is more CPU-efficient for high-throughput log files where inotify would fire constantly. Set to false to enable fsnotify for lower latency on low-volume files.").
			Default(true).
			Example(true).
			Example(false))
}

// Connect opens the file and starts monitoring for changes.
func (sfi *StreamingFileInput) Connect(ctx context.Context) error {
	sfi.connMutex.Lock()
	defer sfi.connMutex.Unlock()

	if sfi.connected {
		return nil
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before opening file: %s: %w", sfi.config.Path, ctx.Err())
	default:
	}

	file, err := os.Open(sfi.config.Path)
	if err != nil {
		return fmt.Errorf("failed to open file: %s: %w", sfi.config.Path, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to stat file: %s: %w", sfi.config.Path, err)
	}

	currentInode, hasInode := inodeOf(stat)

	sfi.positionMutex.Lock()
	if hasInode {
		sfi.position.Inode = currentInode
	}
	sfi.position.ByteOffset = 0
	sfi.positionMutex.Unlock()

	sfi.fileMu.Lock()
	sfi.file = file
	sfi.reader = bufio.NewReader(file)
	sfi.fileMu.Unlock()

	// Only create fsnotify watcher if not disabled.
	// Polling-only mode (DisableFSNotify=true) is more CPU-efficient for high-volume logs.
	if !sfi.config.DisableFSNotify {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			sfi.logger.Warnf("Failed to create fsnotify watcher, falling back to polling: %v", err)
		} else {
			if err := watcher.Add(sfi.config.Path); err != nil {
				watcher.Close()
				sfi.logger.Warnf("Failed to watch file, falling back to polling: %v", err)
			} else {
				parentDir := filepath.Dir(sfi.config.Path)
				if err := watcher.Add(parentDir); err != nil {
					sfi.logger.Warnf("Failed to watch parent directory '%s', rotation detection may be degraded: %v", parentDir, err)
				}
				sfi.watcher = watcher
			}
		}
	}

	sfi.connected = true

	sfi.wg.Add(1)
	go sfi.monitorFile(ctx)

	return nil
}

// monitorFile is the primary goroutine for watching and reading the file.
// Supports two modes:
// - Polling-only (default, DisableFSNotify=true): More CPU-efficient for high-volume logs
// - Event-driven with polling fallback (DisableFSNotify=false): Lower latency for low-volume logs
func (sfi *StreamingFileInput) monitorFile(ctx context.Context) {
	defer sfi.wg.Done()
	defer close(sfi.readLoopDone)
	if sfi.watcher != nil {
		defer sfi.watcher.Close()
	}

	// Do an initial drain of any existing file content
	sfi.drainAvailableData()

	// Polling ticker - the primary mechanism in polling-only mode,
	// or a fallback for missed fsnotify events in event-driven mode.
	pollInterval := time.NewTicker(sfi.config.PollInterval)
	defer pollInterval.Stop()

	// If no watcher, run in pure polling mode (more CPU-efficient)
	if sfi.watcher == nil {
		sfi.logger.Debugf("Running in polling-only mode (interval: %v)", sfi.config.PollInterval)
		for {
			select {
			case <-pollInterval.C:
				sfi.checkStateAndReact()
				sfi.drainAvailableData()
			case <-sfi.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}

	// Event-driven mode with polling fallback
	sfi.logger.Debugf("Running in fsnotify mode with polling fallback (interval: %v)", sfi.config.PollInterval)
	for {
		select {
		case event, ok := <-sfi.watcher.Events:
			if !ok {
				return
			}

			if event.Has(fsnotify.Write) && event.Name == sfi.config.Path {
				sfi.drainAvailableData()
			}

			if event.Has(fsnotify.Create) && event.Name == sfi.config.Path {
				if err := sfi.handleRotation(); err != nil {
					sfi.logger.Errorf("Error handling rotation: %v", err)
				}
			}

			if event.Has(fsnotify.Rename) || event.Has(fsnotify.Remove) {
				time.Sleep(100 * time.Millisecond)
				sfi.checkStateAndReact()
			}

		case <-pollInterval.C:
			// Fallback polling: check for rotation/truncation AND try to read
			// any new data. This handles cases where fsnotify misses events.
			sfi.checkStateAndReact()
			sfi.drainAvailableData()

		case err, ok := <-sfi.watcher.Errors:
			if !ok {
				return
			}
			sfi.logger.Errorf("fsnotify watcher error: %v", err)

		case <-sfi.stopCh:
			return

		case <-ctx.Done():
			return
		}
	}
}

// checkStateAndReact performs a stat check to detect rotation or truncation.
func (sfi *StreamingFileInput) checkStateAndReact() {
	rotated, truncated, err := sfi.detectFileChanges()
	if err != nil {
		sfi.logger.Warnf("Error during state check: %v", err)
		return
	}
	if rotated {
		if err := sfi.handleRotation(); err != nil {
			sfi.logger.Errorf("Error handling rotation: %v", err)
		}
	} else if truncated {
		if err := sfi.handleTruncation(); err != nil {
			sfi.logger.Errorf("Error handling truncation: %v", err)
		}
	}
}

// detectFileChanges checks for rotation and truncation using inode comparison.
func (sfi *StreamingFileInput) detectFileChanges() (rotated, truncated bool, err error) {
	currentStat, err := os.Stat(sfi.config.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, false, nil
		}
		return false, false, err
	}

	currentInode, _ := inodeOf(currentStat)
	currentSize := currentStat.Size()

	sfi.positionMutex.RLock()
	lastInode := sfi.position.Inode
	lastOffset := sfi.position.ByteOffset
	sfi.positionMutex.RUnlock()

	if currentInode != 0 && lastInode != 0 && currentInode != lastInode {
		return true, false, nil
	}

	if currentInode == lastInode && currentSize < lastOffset {
		sfi.logger.Warnf("File truncation detected: current size=%d is less than last offset=%d", currentSize, lastOffset)
		return false, true, nil
	}

	return false, false, nil
}

// handleTruncation resets the position for the current file.
func (sfi *StreamingFileInput) handleTruncation() error {
	sfi.logger.Infof("Handling file truncation, resetting position to zero")

	sfi.positionMutex.Lock()
	sfi.position.ByteOffset = 0
	sfi.positionMutex.Unlock()

	sfi.drainBufferChannel()

	sfi.fileMu.Lock()
	if sfi.file != nil {
		if _, err := sfi.file.Seek(0, 0); err != nil {
			sfi.logger.Errorf("Failed to seek to start after truncation, will reopen: %v", err)
			err2 := sfi.reopenFileLocked()
			sfi.fileMu.Unlock()
			return err2
		}
		sfi.reader.Reset(sfi.file)
	}
	sfi.fileMu.Unlock()

	sfi.drainAvailableData()

	return nil
}

// handleRotation manages the full file rotation process.
func (sfi *StreamingFileInput) handleRotation() error {
	sfi.logger.Infof("File rotation detected, handling transition")

	sfi.fileMu.Lock()
	if sfi.file != nil {
		sfi.logger.Debugf("Draining remaining data from old file handle before closing")
		sfi.drainFromReaderLocked()
		sfi.file.Close()
		sfi.file = nil
		sfi.reader = nil
	}
	sfi.fileMu.Unlock()

	var err error
	for attempt := 0; attempt < 10; attempt++ {
		if err = sfi.reopenFile(); err == nil {
			break
		}
		if !os.IsNotExist(err) {
			sfi.logger.Errorf("Failed to open new file after rotation: %v", err)
			if sfi.watcher != nil {
				if err2 := sfi.watcher.Add(sfi.config.Path); err2 != nil {
					sfi.logger.Warnf("Failed to re-add file to watcher after rotation: %v", err2)
				}
			}
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		sfi.logger.Errorf("Failed to open new file after rotation after retries: %v", err)
		if sfi.watcher != nil {
			if err2 := sfi.watcher.Add(sfi.config.Path); err2 != nil {
				sfi.logger.Warnf("Failed to re-add file to watcher after rotation: %v", err2)
			}
		}
		return err
	}

	if sfi.watcher != nil {
		if err := sfi.watcher.Add(sfi.config.Path); err != nil {
			sfi.logger.Warnf("Failed to re-add file to watcher after rotation: %v", err)
		}
	}

	sfi.drainAvailableData()

	sfi.logger.Infof("Successfully switched to new file after rotation")
	return nil
}

// reopenFile opens the configured path and updates the position.
func (sfi *StreamingFileInput) reopenFile() error {
	sfi.fileMu.Lock()
	defer sfi.fileMu.Unlock()
	return sfi.reopenFileLocked()
}

// reopenFileLocked opens the configured path (assumes lock is held).
func (sfi *StreamingFileInput) reopenFileLocked() error {
	file, err := os.Open(sfi.config.Path)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	newInode, _ := inodeOf(info)

	sfi.positionMutex.Lock()
	sfi.position.Inode = newInode
	sfi.position.ByteOffset = 0
	sfi.positionMutex.Unlock()

	sfi.file = file
	sfi.reader = bufio.NewReader(file)
	return nil
}

// drainAvailableData reads data from the file and buffers it.
func (sfi *StreamingFileInput) drainAvailableData() {
	sfi.fileMu.RLock()
	file := sfi.file
	reader := sfi.reader
	sfi.fileMu.RUnlock()

	if reader == nil || file == nil {
		return
	}

	rotated, truncated, err := sfi.detectFileChanges()
	if err != nil {
		sfi.logger.Errorf("Error detecting file changes: %v", err)
	}
	if rotated {
		if err := sfi.handleRotation(); err != nil {
			sfi.logger.Errorf("Error handling rotation: %v", err)
		}
		return
	}
	if truncated {
		if err := sfi.handleTruncation(); err != nil {
			sfi.logger.Errorf("Error handling truncation: %v", err)
		}
		return
	}

	sfi.drainFromReader(reader)
}

// drainFromReader reads lines from the reader and buffers them.
func (sfi *StreamingFileInput) drainFromReader(reader *bufio.Reader) {
	scanner := bufio.NewScanner(reader)
	maxScanTokenSize := sfi.config.MaxLineSize + 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxScanTokenSize)
	scanner.Split(splitKeepNewline)

	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		if len(lineBytes) == 0 {
			continue
		}

		lineCopy := make([]byte, len(lineBytes))
		copy(lineCopy, lineBytes)

		if sfi.bufferClosed.Load() {
			return
		}

		select {
		case sfi.buffer <- lineCopy:
		case <-sfi.stopCh:
			return
		}
	}

	if err := scanner.Err(); err != nil && err != bufio.ErrTooLong {
		sfi.logger.Warnf("Error while draining data: %v", err)
	}
}

// drainFromReaderLocked drains data from the reader (assumes fileMu is held).
func (sfi *StreamingFileInput) drainFromReaderLocked() {
	if sfi.reader == nil || sfi.file == nil {
		return
	}
	sfi.drainFromReader(sfi.reader)
}

// drainBufferChannel drains all pending data from the buffer channel.
func (sfi *StreamingFileInput) drainBufferChannel() {
	for {
		select {
		case _, ok := <-sfi.buffer:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

// Read returns the next message from the file.
// Like tail -F, this blocks indefinitely until data is available,
// the input is closed, or the context is cancelled.
func (sfi *StreamingFileInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	sfi.connMutex.RLock()
	connected := sfi.connected
	sfi.connMutex.RUnlock()

	if !connected {
		return nil, nil, service.ErrNotConnected
	}

	// Block until data is available, shutdown, or context cancellation.
	// This matches tail -F behavior: no artificial timeout, just wait for data.
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-sfi.stopCh:
		// On shutdown, drain any remaining buffered data
		select {
		case lineBytes, ok := <-sfi.buffer:
			if !ok {
				return nil, nil, io.EOF
			}
			return sfi.createMessage(lineBytes)
		default:
			return nil, nil, io.EOF
		}
	case lineBytes, ok := <-sfi.buffer:
		if !ok {
			return nil, nil, io.EOF
		}
		return sfi.createMessage(lineBytes)
	}
}

// createMessage creates a message with position metadata.
func (sfi *StreamingFileInput) createMessage(lineBytes []byte) (*service.Message, service.AckFunc, error) {
	sfi.positionMutex.RLock()
	pos := FilePosition{
		FilePath:   sfi.position.FilePath,
		Inode:      sfi.position.Inode,
		ByteOffset: sfi.position.ByteOffset,
	}
	sfi.positionMutex.RUnlock()

	delta := int64(len(lineBytes))

	sfi.positionMutex.Lock()
	sfi.position.ByteOffset += delta
	sfi.positionMutex.Unlock()

	msg := service.NewMessage(lineBytes)
	msg.MetaSet("streaming_file_path", pos.FilePath)
	msg.MetaSet("streaming_file_inode", strconv.FormatUint(pos.Inode, 10))
	msg.MetaSet("streaming_file_offset", strconv.FormatInt(pos.ByteOffset, 10))

	sfi.inFlightCount.Add(1)

	return msg, func(_ context.Context, _ error) error {
		sfi.inFlightCount.Add(-1)
		return nil
	}, nil
}

// Close shuts down the input.
func (sfi *StreamingFileInput) Close(ctx context.Context) error {
	sfi.connMutex.Lock()
	if !sfi.connected {
		sfi.connMutex.Unlock()
		return nil
	}
	sfi.connected = false
	sfi.connMutex.Unlock()

	select {
	case <-sfi.stopCh:
	default:
		close(sfi.stopCh)
	}

	sfi.fileMu.Lock()
	if sfi.file != nil {
		sfi.file.Close()
		sfi.file = nil
	}
	sfi.fileMu.Unlock()

	select {
	case <-sfi.readLoopDone:
	case <-ctx.Done():
		sfi.logger.Warnf("Close context cancelled before read loop finished")
		return ctx.Err()
	case <-time.After(5 * time.Second):
		sfi.logger.Warnf("Read loop did not finish within 5 seconds")
	}

	if sfi.bufferClosed.CompareAndSwap(false, true) {
		close(sfi.buffer)
	}

	drainDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		defer close(drainDone)

		for {
			if sfi.inFlightCount.Load() == 0 {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-drainDone:
		sfi.logger.Infof("All in-flight messages acknowledged")
	case <-ctx.Done():
		remaining := sfi.inFlightCount.Load()
		if remaining > 0 {
			sfi.logger.Warnf("Shutdown with %d in-flight messages", remaining)
		}
	}

	sfi.wg.Wait()

	sfi.fileMu.Lock()
	sfi.file = nil
	sfi.fileMu.Unlock()

	sfi.logger.Infof("Streaming file input closed successfully")
	return nil
}

func init() {
	err := service.RegisterInput("streaming_file", streamingFileInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.Input, error) {
			path, err := pConf.FieldString("path")
			if err != nil {
				return nil, err
			}
			maxBufferSize, err := pConf.FieldInt("max_buffer_size")
			if err != nil {
				return nil, err
			}
			maxLineSize, err := pConf.FieldInt("max_line_size")
			if err != nil {
				return nil, err
			}
			pollInterval, err := pConf.FieldDuration("poll_interval")
			if err != nil {
				return nil, err
			}
			disableFSNotify, err := pConf.FieldBool("disable_fsnotify")
			if err != nil {
				return nil, err
			}

			cfg := StreamingFileInputConfig{
				Path:            path,
				MaxBufferSize:   maxBufferSize,
				MaxLineSize:     maxLineSize,
				PollInterval:    pollInterval,
				DisableFSNotify: disableFSNotify,
			}

			return NewStreamingFileInput(cfg, res.Logger())
		})
	if err != nil {
		panic(err)
	}
}
