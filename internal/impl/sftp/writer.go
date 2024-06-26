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

package sftp

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

type codecSuffixFn func(data []byte) ([]byte, bool)

func codecGetWriter(codec string) (sFn codecSuffixFn, appendMode bool, err error) {
	switch codec {
	case "all-bytes":
		return func(data []byte) ([]byte, bool) { return nil, false }, false, nil
	case "append":
		return customDelimSuffixFn(""), true, nil
	case "lines":
		return customDelimSuffixFn("\n"), true, nil
	}
	if strings.HasPrefix(codec, "delim:") {
		by := strings.TrimPrefix(codec, "delim:")
		if by == "" {
			return nil, false, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return customDelimSuffixFn(by), true, nil
	}
	return nil, false, fmt.Errorf("codec was not recognised: %v", codec)
}

func customDelimSuffixFn(suffix string) codecSuffixFn {
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
