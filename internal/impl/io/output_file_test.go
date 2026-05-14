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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateFilePath(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		goos        string
		errContains string
	}{
		// NUL bytes rejected on every platform.
		{
			name:        "NUL byte rejected on linux",
			path:        "/tmp/foo\x00bar.txt",
			goos:        "linux",
			errContains: "NUL byte",
		},
		{
			name:        "NUL byte rejected on windows",
			path:        `C:\tmp\foo` + "\x00" + `bar.txt`,
			goos:        "windows",
			errContains: "NUL byte",
		},
		{
			name:        "NUL byte rejected on darwin",
			path:        "/tmp/foo\x00bar.txt",
			goos:        "darwin",
			errContains: "NUL byte",
		},

		// Valid paths across platforms.
		{
			name: "simple path valid on linux",
			path: "/tmp/data.txt",
			goos: "linux",
		},
		{
			name: "simple path valid on darwin",
			path: "/tmp/data.txt",
			goos: "darwin",
		},
		{
			name: "simple path valid on windows",
			path: `C:\tmp\data.txt`,
			goos: "windows",
		},
		{
			name: "deep nested path valid on linux",
			path: "/var/log/app/2024/01/data.log",
			goos: "linux",
		},

		// Linux allows colons in file names.
		{
			name: "colon in name valid on linux",
			path: "/tmp/sample_2021-10-10T23:45.json",
			goos: "linux",
		},

		// macOS rejects colons in the base file name.
		{
			name:        "colon in name rejected on darwin",
			path:        "/tmp/sample_2021-10-10T23:45.json",
			goos:        "darwin",
			errContains: "colon",
		},
		{
			name: "no colon in base name valid on darwin",
			path: "/tmp/mydir/data.log",
			goos: "darwin",
		},

		// Windows rejects several characters in the base file name.
		{
			name:        "colon in base name rejected on windows",
			path:        `C:\tmp\12:00:00.log`,
			goos:        "windows",
			errContains: "invalid on windows",
		},
		{
			name:        "angle brackets rejected on windows",
			path:        `C:\tmp\foo<bar>.txt`,
			goos:        "windows",
			errContains: "invalid on windows",
		},
		{
			name:        "pipe rejected on windows",
			path:        `C:\tmp\foo|bar.txt`,
			goos:        "windows",
			errContains: "invalid on windows",
		},
		{
			name:        "question mark rejected on windows",
			path:        `C:\tmp\foo?.txt`,
			goos:        "windows",
			errContains: "invalid on windows",
		},
		{
			name:        "asterisk rejected on windows",
			path:        `C:\tmp\foo*.txt`,
			goos:        "windows",
			errContains: "invalid on windows",
		},
		{
			name:        "double quote rejected on windows",
			path:        `C:\tmp\foo"bar.txt`,
			goos:        "windows",
			errContains: "invalid on windows",
		},
		{
			name:        "control character rejected on windows",
			path:        "C:\\tmp\\foo\x01bar.txt",
			goos:        "windows",
			errContains: "invalid on windows",
		},

		// Windows drive-letter colon is valid (it's not in the base name).
		{
			name: "drive letter colon valid on windows",
			path: `C:\Users\data.txt`,
			goos: "windows",
		},
		{
			name: "relative path valid on windows",
			path: `tmp\data.txt`,
			goos: "windows",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateFilePath(test.path, test.goos)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
