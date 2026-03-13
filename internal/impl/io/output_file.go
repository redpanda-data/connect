// Copyright 2026 Redpanda Data, Inc.
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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	foFieldPath  = "path"
	foFieldCodec = "codec"
)

func fileOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Local").
		Summary(`Writes messages to files on disk based on a chosen codec.`).
		Description(`Messages can be written to different files by using xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions] in the path field. However, only one file is ever open at a given time, and therefore when the path changes the previously open file is closed.`).
		Fields(
			service.NewInterpolatedStringField(foFieldPath).
				Description("The file to write to, if the file does not yet exist it will be created.").
				Examples(
					"/tmp/data.txt",
					"/tmp/${! timestamp_unix() }.txt",
					`/tmp/${! json("document.id") }.json`,
				).
				Version("3.33.0"),
			service.NewStringAnnotatedEnumField(foFieldCodec, map[string]string{
				"all-bytes": "Only applicable to file based outputs. Writes each message to a file in full, if the file already exists the old content is deleted.",
				"append":    "Append each message to the output stream without any delimiter or special encoding.",
				"lines":     "Append each message to the output stream followed by a line break.",
				"delim:x":   "Append each message to the output stream followed by a custom delimiter.",
			}).
				Description("The way in which the bytes of messages should be written out into the output data stream. It's possible to write lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.").
				LintRule("").
				Examples("lines", "delim:\t", "delim:foobar").
				Default("lines").
				Version("3.33.0"),
		)
}

func init() {
	service.MustRegisterOutput("file", fileOutputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (out service.Output, mif int, err error) {
			var conf fileOutputConfig
			if conf, err = fileOutputConfigFromParsed(pConf); err != nil {
				return
			}
			mif = 1
			out, err = newFileWriter(conf.Path, conf.Codec, res)
			return
		})
}

//------------------------------------------------------------------------------

type fileOutputConfig struct {
	Path  *service.InterpolatedString
	Codec string
}

func fileOutputConfigFromParsed(pConf *service.ParsedConfig) (conf fileOutputConfig, err error) {
	if conf.Path, err = pConf.FieldInterpolatedString(foFieldPath); err != nil {
		return
	}
	if conf.Codec, err = pConf.FieldString(foFieldCodec); err != nil {
		return
	}
	return
}

//------------------------------------------------------------------------------

type suffixFn func(data []byte) ([]byte, bool)

func getWriterCodec(codec string) (sFn suffixFn, appendMode bool, err error) {
	switch codec {
	case "all-bytes":
		return func([]byte) ([]byte, bool) { return nil, false }, false, nil
	case "append":
		return customDelimSuffix(""), true, nil
	case "lines":
		return customDelimSuffix("\n"), true, nil
	}
	if after, ok := strings.CutPrefix(codec, "delim:"); ok {
		if after == "" {
			return nil, false, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return customDelimSuffix(after), true, nil
	}
	return nil, false, fmt.Errorf("codec was not recognised: %v", codec)
}

func customDelimSuffix(suffix string) suffixFn {
	suffixB := []byte(suffix)
	return func(data []byte) ([]byte, bool) {
		if len(suffixB) == 0 {
			return nil, false
		}
		if !bytes.HasSuffix(data, suffixB) {
			return suffixB, true
		}
		return nil, false
	}
}

//------------------------------------------------------------------------------

type fileWriter struct {
	log *service.Logger
	nm  *service.Resources

	path       *service.InterpolatedString
	suffixFn   suffixFn
	appendMode bool

	handleMut  sync.Mutex
	handlePath string
	handle     io.WriteCloser
}

func newFileWriter(path *service.InterpolatedString, codecStr string, mgr *service.Resources) (*fileWriter, error) {
	codec, appendMode, err := getWriterCodec(codecStr)
	if err != nil {
		return nil, err
	}
	return &fileWriter{
		suffixFn:   codec,
		appendMode: appendMode,
		path:       path,
		log:        mgr.Logger(),
		nm:         mgr,
	}, nil
}

func (*fileWriter) Connect(_ context.Context) error {
	return nil
}

func (w *fileWriter) writeTo(wtr io.Writer, p *service.Message) error {
	mBytes, err := p.AsBytes()
	if err != nil {
		return err
	}

	suffix, addSuffix := w.suffixFn(mBytes)

	if _, err := wtr.Write(mBytes); err != nil {
		return err
	}
	if addSuffix {
		if _, err := wtr.Write(suffix); err != nil {
			return err
		}
	}
	return nil
}

func (w *fileWriter) Write(_ context.Context, msg *service.Message) error {
	path, err := w.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	if err := validateFilePath(path, runtime.GOOS); err != nil {
		return err
	}

	w.handleMut.Lock()
	defer w.handleMut.Unlock()

	if w.handle != nil && path == w.handlePath {
		return w.writeTo(w.handle, msg)
	}
	if w.handle != nil {
		if err := w.handle.Close(); err != nil {
			return err
		}
		w.handle = nil
	}

	flag := os.O_CREATE | os.O_RDWR
	if w.appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	if err := w.nm.FS().MkdirAll(filepath.Dir(path), fs.FileMode(0o777)); err != nil {
		return err
	}

	file, err := w.nm.FS().OpenFile(path, flag, fs.FileMode(0o666))
	if err != nil {
		return err
	}

	handle, ok := file.(io.WriteCloser)
	if !ok {
		_ = file.Close()
		return errors.New("opening file for writing")
	}

	w.handlePath = path
	if err := w.writeTo(handle, msg); err != nil {
		_ = handle.Close()
		return err
	}

	if w.appendMode {
		w.handle = handle
	} else {
		_ = handle.Close()
	}
	return nil
}

func (w *fileWriter) Close(_ context.Context) error {
	w.handleMut.Lock()
	defer w.handleMut.Unlock()

	var err error
	if w.handle != nil {
		err = w.handle.Close()
		w.handle = nil
	}
	return err
}

//------------------------------------------------------------------------------

// validateFilePath checks that the base file name component of path does not
// contain characters that would cause silent data loss or confusing behavior on
// the target operating system.
//
// The goos parameter mirrors runtime.GOOS and is accepted explicitly so that
// tests can exercise all platform branches regardless of the build host.
//
// Rules:
//   - All platforms: NUL bytes are rejected in any path component.
//   - Windows: <, >, :, ", |, ?, * and control characters 0x01–0x1F are rejected
//     in the base file name. The drive-letter colon (C:) is not part of the base
//     name and is therefore not rejected.
//   - macOS/Darwin: colons are rejected in the base file name because HFS+/APFS
//     maps ':' to '/', silently placing the file in a different directory.
func validateFilePath(path, goos string) error {
	if strings.ContainsRune(path, '\x00') {
		return fmt.Errorf(
			"file name %q in path %q contains a NUL byte which is invalid on all platforms",
			crossPlatformBase(path), path,
		)
	}

	base := crossPlatformBase(path)

	switch goos {
	case "windows":
		return validateWindowsFileName(base, path)
	case "darwin":
		return validateDarwinFileName(base, path)
	}
	return nil
}

// crossPlatformBase returns the last element of path, considering both forward
// and back slashes as separators. This is necessary because filepath.Base only
// recognises the build-host separator, but validateFilePath must correctly
// extract the base name for any target OS.
func crossPlatformBase(path string) string {
	// Find the last separator (either / or \).
	i := strings.LastIndexAny(path, `/\`)
	if i >= 0 {
		path = path[i+1:]
	}
	if path == "" {
		return "."
	}
	return path
}

func validateWindowsFileName(base, fullPath string) error {
	const badChars = `<>:"|?*`
	var found []string
	for i, r := range base {
		switch {
		case r >= '\x01' && r <= '\x1F':
			found = append(found, fmt.Sprintf("control character 0x%02X at position %d", r, i))
		case strings.ContainsRune(badChars, r):
			found = append(found, fmt.Sprintf("%q at position %d", string(r), i))
		}
	}
	if len(found) > 0 {
		return fmt.Errorf(
			"file name %q in path %q contains characters invalid on windows: %s",
			base, fullPath, strings.Join(found, ", "),
		)
	}
	return nil
}

func validateDarwinFileName(base, fullPath string) error {
	if strings.ContainsRune(base, ':') {
		return fmt.Errorf(
			"file name %q in path %q contains a colon which is invalid on macOS "+
				"(HFS+/APFS maps ':' to '/', creating a file in the wrong directory)",
			base, fullPath,
		)
	}
	return nil
}
