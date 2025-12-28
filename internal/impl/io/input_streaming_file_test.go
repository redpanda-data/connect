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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stripNewline removes trailing newline from line bytes for comparison
// The custom scanner keeps delimiters, so we need to strip them for testing
func stripNewline(b []byte) string {
	return string(bytes.TrimSuffix(b, []byte("\n")))
}

func TestStreamingFileInput_BasicReading(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	// Create test file
	testData := "line1\nline2\nline3\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := StreamingFileInputConfig{
		Path:          filePath,
		MaxBufferSize: 10,
		MaxLineSize:   1024 * 1024,
	}

	input, err := NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	// Give event loop time to process initial file
	time.Sleep(100 * time.Millisecond)

	// Read lines
	msg1, ack1, err := input.Read(ctx)
	require.NoError(t, err)
	b1, err := msg1.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line1", stripNewline(b1))
	require.NoError(t, ack1(ctx, nil))

	msg2, ack2, err := input.Read(ctx)
	require.NoError(t, err)
	b2, err := msg2.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line2", stripNewline(b2))
	require.NoError(t, ack2(ctx, nil))

	msg3, ack3, err := input.Read(ctx)
	require.NoError(t, err)
	b3, err := msg3.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line3", stripNewline(b3))
	require.NoError(t, ack3(ctx, nil))
}

func TestStreamingFileInput_PositionMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	// Create test file with multiple lines
	testData := "line1\nline2\nline3\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := StreamingFileInputConfig{
		Path:          filePath,
		MaxBufferSize: 10,
		MaxLineSize:   1024 * 1024,
	}

	input, err := NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read first line and check metadata
	msg1, ack1, err := input.Read(ctx)
	require.NoError(t, err)

	// Check metadata fields exist
	path, ok := msg1.MetaGet("streaming_file_path")
	assert.True(t, ok, "streaming_file_path metadata should exist")
	assert.Equal(t, filePath, path)

	inode, ok := msg1.MetaGet("streaming_file_inode")
	assert.True(t, ok, "streaming_file_inode metadata should exist")
	assert.NotEmpty(t, inode)

	offset, ok := msg1.MetaGet("streaming_file_offset")
	assert.True(t, ok, "streaming_file_offset metadata should exist")
	assert.Equal(t, "0", offset, "first line should start at offset 0")

	require.NoError(t, ack1(ctx, nil))

	// Read second line - offset should have advanced
	msg2, ack2, err := input.Read(ctx)
	require.NoError(t, err)

	offset2, ok := msg2.MetaGet("streaming_file_offset")
	assert.True(t, ok)
	assert.Equal(t, "6", offset2, "second line should start at offset 6 (after 'line1\\n')")

	require.NoError(t, ack2(ctx, nil))

	// Read third line
	msg3, ack3, err := input.Read(ctx)
	require.NoError(t, err)

	offset3, ok := msg3.MetaGet("streaming_file_offset")
	assert.True(t, ok)
	assert.Equal(t, "12", offset3, "third line should start at offset 12")
	require.NoError(t, ack3(ctx, nil))
}

func TestStreamingFileInput_FileRotation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("File rotation test requires Unix-like system")
	}

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	// Create initial file
	testData := "line1\nline2\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := StreamingFileInputConfig{
		Path:          filePath,
		MaxBufferSize: 10,
		MaxLineSize:   1024 * 1024,
	}

	input, err := NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read first line
	msg1, ack1, err := input.Read(ctx)
	require.NoError(t, err)
	b1, err := msg1.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line1", stripNewline(b1))
	require.NoError(t, ack1(ctx, nil))

	// Read second line (to consume it from buffer before rotation)
	msg2, ack2, err := input.Read(ctx)
	require.NoError(t, err)
	b2, err := msg2.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line2", stripNewline(b2))
	require.NoError(t, ack2(ctx, nil))

	// Simulate file rotation by moving the old file and creating a new one
	// This changes the inode, which is how real log rotation works
	rotatedPath := filepath.Join(tmpDir, "test.log.1")
	require.NoError(t, os.Rename(filePath, rotatedPath))

	// Create new file with new content
	newData := "line3\nline4\n"
	require.NoError(t, os.WriteFile(filePath, []byte(newData), 0644))

	// Give time for fsnotify event processing
	time.Sleep(500 * time.Millisecond)

	// Should read from new file
	msg3, ack3, err := input.Read(ctx)
	require.NoError(t, err)
	b3, err := msg3.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "line3", stripNewline(b3))

	// Verify metadata shows offset reset to 0 for new file
	offset, ok := msg3.MetaGet("streaming_file_offset")
	assert.True(t, ok)
	assert.Equal(t, "0", offset, "offset should reset to 0 after rotation")
	require.NoError(t, ack3(ctx, nil))
}

func TestStreamingFileInput_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	// Create test file with many lines
	var testData string
	for i := 1; i <= 100; i++ {
		testData += fmt.Sprintf("line%d\n", i)
	}
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := StreamingFileInputConfig{
		Path:          filePath,
		MaxBufferSize: 50,
		MaxLineSize:   1024 * 1024,
	}

	input, err := NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	// Use a longer timeout for setup, but we'll cancel once all lines are read
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read all lines concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex
	readLines := make(map[string]bool)
	var readCount int

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, ack, err := input.Read(ctx)
				if err != nil {
					return
				}
				b, _ := msg.AsBytes()
				line := stripNewline(b)
				mu.Lock()
				readLines[line] = true
				readCount++
				done := readCount >= 100
				mu.Unlock()
				_ = ack(ctx, nil)
				if done {
					cancel() // Signal all readers to stop
				}
			}
		}()
	}

	wg.Wait()

	// Verify we read all lines
	assert.Len(t, readLines, 100)
	for i := 1; i <= 100; i++ {
		assert.True(t, readLines[fmt.Sprintf("line%d", i)])
	}
}

func TestStreamingFileInput_FileTruncation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("File truncation test requires Unix-like system")
	}

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	// Create initial file with multiple lines
	testData := "line1\nline2\nline3\nline4\nline5\nline6\nline7\nline8\n"
	require.NoError(t, os.WriteFile(filePath, []byte(testData), 0644))

	cfg := StreamingFileInputConfig{
		Path:          filePath,
		MaxBufferSize: 10,
		MaxLineSize:   1024 * 1024,
	}

	input, err := NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Read first 7 lines to consume most of the initial content
	for i := 0; i < 7; i++ {
		msg, ack, err := input.Read(ctx)
		require.NoError(t, err)
		require.NoError(t, ack(ctx, nil))
		b, _ := msg.AsBytes()
		assert.Equal(t, fmt.Sprintf("line%d", i+1), stripNewline(b))
	}

	// Truncate the file (write much fewer lines to ensure size is smaller than current offset)
	truncatedData := "x\n"
	require.NoError(t, os.WriteFile(filePath, []byte(truncatedData), 0644))

	// Give time for truncation detection and event processing
	time.Sleep(500 * time.Millisecond)

	// Should read from the beginning of the truncated file
	msg, ack, err := input.Read(ctx)
	require.NoError(t, err)
	b, _ := msg.AsBytes()
	assert.Equal(t, "x", stripNewline(b))

	// Verify metadata shows offset reset to 0 after truncation
	offset, ok := msg.MetaGet("streaming_file_offset")
	assert.True(t, ok)
	assert.Equal(t, "0", offset, "offset should reset to 0 after truncation")
	require.NoError(t, ack(ctx, nil))
}

func TestStreamingFileInput_LiveAppend(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.log")

	// Create initial empty file
	require.NoError(t, os.WriteFile(filePath, []byte{}, 0644))

	cfg := StreamingFileInputConfig{
		Path:          filePath,
		MaxBufferSize: 10,
		MaxLineSize:   1024 * 1024,
	}

	input, err := NewStreamingFileInput(cfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, input.Connect(ctx))
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Append lines to the file
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)

	_, err = f.WriteString("appended1\n")
	require.NoError(t, err)
	require.NoError(t, f.Sync())

	// Give fsnotify time to detect the change
	time.Sleep(200 * time.Millisecond)

	// Read the appended line
	msg1, ack1, err := input.Read(ctx)
	require.NoError(t, err)
	b1, err := msg1.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "appended1", stripNewline(b1))
	require.NoError(t, ack1(ctx, nil))

	// Append another line
	_, err = f.WriteString("appended2\n")
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	time.Sleep(200 * time.Millisecond)

	// Read the second appended line
	msg2, ack2, err := input.Read(ctx)
	require.NoError(t, err)
	b2, err := msg2.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "appended2", stripNewline(b2))
	require.NoError(t, ack2(ctx, nil))
}

func TestStreamingFileInput_FilePositionStruct(t *testing.T) {
	// Test that FilePosition struct is correctly defined for metadata use
	pos := FilePosition{
		FilePath:   "/var/log/test.log",
		Inode:      12345,
		ByteOffset: 1024,
	}

	assert.Equal(t, "/var/log/test.log", pos.FilePath)
	assert.Equal(t, uint64(12345), pos.Inode)
	assert.Equal(t, int64(1024), pos.ByteOffset)
}
