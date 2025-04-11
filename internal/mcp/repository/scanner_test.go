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

package repository_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/mcp/repository"
)

func TestScannerHappy(t *testing.T) {
	s := repository.NewScanner(fstest.MapFS{
		filepath.Clean("resources/caches/foo.yaml"): &fstest.MapFile{
			Data: []byte(`foo cache conf`),
		},
		filepath.Clean("resources/caches/ignore.meow"): &fstest.MapFile{
			Data: []byte(`IGNORE ME`),
		},
		filepath.Clean("resources/caches/nope/notthis.what"): &fstest.MapFile{
			Data: []byte(`IGNORE ME`),
		},
		filepath.Clean("resources/processors/deeper/bar.yml"): &fstest.MapFile{
			Data: []byte(`bar proc conf`),
		},
		filepath.Clean("resources/inputs/baz.yml"): &fstest.MapFile{
			Data: []byte(`baz input conf`),
		},
		filepath.Clean("resources/outputs/moo.yml"): &fstest.MapFile{
			Data: []byte(`moo output conf`),
		},
		filepath.Clean("o11y/tracer.yaml"): &fstest.MapFile{
			Data: []byte(`tracer conf`),
		},
		filepath.Clean("o11y/metrics.yaml"): &fstest.MapFile{
			Data: []byte(`metrics conf`),
		},
	})

	exp := map[string]string{
		"resources/caches/foo.yaml/cache":               "foo cache conf",
		"resources/processors/deeper/bar.yml/processor": "bar proc conf",
		"resources/inputs/baz.yml/input":                "baz input conf",
		"resources/outputs/moo.yml/output":              "moo output conf",
		"o11y/metrics.yaml":                             "metrics conf",
		"o11y/tracer.yaml":                              "tracer conf",
	}
	act := map[string]string{}

	s.OnResourceFile(func(resourceType string, filePath string, contents []byte) error {
		act[filePath+"/"+resourceType] = string(contents)
		return nil
	})
	s.OnMetricsFile(func(filePath string, contents []byte) error {
		act[filePath] = string(contents)
		return nil
	})
	s.OnTracerFile(func(filePath string, contents []byte) error {
		act[filePath] = string(contents)
		return nil
	})

	require.NoError(t, s.Scan("."))
	assert.Equal(t, exp, act)
}

func TestScannerRoot(t *testing.T) {
	s := repository.NewScanner(fstest.MapFS{
		filepath.Clean("foo/resources/caches/foo.yaml"): &fstest.MapFile{
			Data: []byte(`foo cache conf`),
		},
		filepath.Clean("foo/resources/processors/bar.yml"): &fstest.MapFile{
			Data: []byte(`bar proc conf`),
		},
		filepath.Clean("foo/resources/inputs/baz.yml"): &fstest.MapFile{
			Data: []byte(`baz input conf`),
		},
		filepath.Clean("foo/resources/outputs/moo.yml"): &fstest.MapFile{
			Data: []byte(`moo output conf`),
		},
		filepath.Clean("foo/o11y/tracer.yaml"): &fstest.MapFile{
			Data: []byte(`tracer conf`),
		},
		filepath.Clean("foo/o11y/metrics.yaml"): &fstest.MapFile{
			Data: []byte(`metrics conf`),
		},
	})

	exp := map[string]string{
		"foo/resources/caches/foo.yaml/cache":        "foo cache conf",
		"foo/resources/processors/bar.yml/processor": "bar proc conf",
		"foo/resources/inputs/baz.yml/input":         "baz input conf",
		"foo/resources/outputs/moo.yml/output":       "moo output conf",
		"foo/o11y/metrics.yaml":                      "metrics conf",
		"foo/o11y/tracer.yaml":                       "tracer conf",
	}
	act := map[string]string{}

	s.OnResourceFile(func(resourceType string, filePath string, contents []byte) error {
		act[filePath+"/"+resourceType] = string(contents)
		return nil
	})
	s.OnMetricsFile(func(filePath string, contents []byte) error {
		act[filePath] = string(contents)
		return nil
	})
	s.OnTracerFile(func(filePath string, contents []byte) error {
		act[filePath] = string(contents)
		return nil
	})

	require.NoError(t, s.Scan("foo"))
	assert.Equal(t, exp, act)
}
