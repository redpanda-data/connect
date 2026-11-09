// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

const testMod = "example.com/m"

func TestAffectedPackages(t *testing.T) {
	pkgs := []TestPackage{
		{Path: "./internal/impl/kafka"},
		{Path: "./internal/impl/mysql"},
		{Path: "./internal/impl/redis"},
	}
	deps := map[string]map[string]bool{
		"./internal/impl/kafka": {
			testMod + "/internal/impl/kafka": true,
			testMod + "/internal/license":    true,
			testMod + "/public/service":      true,
		},
		"./internal/impl/mysql": {
			testMod + "/internal/impl/mysql":  true,
			testMod + "/internal/license":     true,
			testMod + "/internal/replication": true,
			testMod + "/public/service":       true,
		},
		"./internal/impl/redis": {
			testMod + "/internal/impl/redis": true,
			testMod + "/public/service":      true,
		},
	}
	pkgDirs := map[string]bool{
		"internal/impl/kafka":   true,
		"internal/impl/mysql":   true,
		"internal/impl/redis":   true,
		"internal/license":      true,
		"internal/replication":  true,
		"public/service":        true,
		"cmd/tools/integration": true,
	}
	isPkgDir := func(d string) bool { return pkgDirs[d] }

	tests := []struct {
		name    string
		changed []string
		want    []string
	}{
		{
			name:    "shared package outside internal/impl triggers its dependents",
			changed: []string{"internal/license/service.go"},
			want:    []string{"./internal/impl/kafka", "./internal/impl/mysql"},
		},
		{
			name:    "package imported by one component",
			changed: []string{"internal/replication/signalling.go"},
			want:    []string{"./internal/impl/mysql"},
		},
		{
			name:    "component source file",
			changed: []string{"internal/impl/redis/cache.go"},
			want:    []string{"./internal/impl/redis"},
		},
		{
			name:    "test file of a shared package triggers nothing",
			changed: []string{"internal/license/service_test.go"},
			want:    nil,
		},
		{
			name:    "test file of a component triggers only that component",
			changed: []string{"internal/impl/kafka/integration_test.go"},
			want:    []string{"./internal/impl/kafka"},
		},
		{
			name:    "testdata triggers only the owning package",
			changed: []string{"internal/impl/mysql/testdata/nested/dump.sql"},
			want:    []string{"./internal/impl/mysql"},
		},
		{
			name:    "non-Go file in a subdirectory counts for the enclosing package",
			changed: []string{"internal/license/resources/key.pem"},
			want:    []string{"./internal/impl/kafka", "./internal/impl/mysql"},
		},
		{
			name: "files outside any imported package trigger nothing",
			changed: []string{
				".github/workflows/integration_test.yml",
				"docs/modules/foo.adoc",
				"README.md",
				"cmd/tools/integration/run.go",
				"",
			},
			want: nil,
		},
		{
			name:    "public API triggers every component",
			changed: []string{"public/service/message.go"},
			want:    []string{"./internal/impl/kafka", "./internal/impl/mysql", "./internal/impl/redis"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := affectedPackages(pkgs, deps, testMod, tt.changed, isPkgDir)
			assert.Equal(t, tt.want, res.Packages)
		})
	}
}

func TestParseGoListTestDeps(t *testing.T) {
	out := []byte(testMod + "/a|fmt," + testMod + "/lib\n" +
		testMod + "/a.test|fmt," + testMod + "/a [" + testMod + "/a.test]," + testMod + "/lib\n" +
		testMod + "/a [" + testMod + "/a.test]|fmt\n" +
		testMod + "/a_test [" + testMod + "/a.test]|" + testMod + "/other\n" +
		testMod + "/b.test|fmt\n")
	got := parseGoListTestDeps(out, map[string]string{testMod + "/a.test": "./a"})
	assert.Equal(t, map[string]map[string]bool{
		"./a": {"fmt": true, testMod + "/a": true, testMod + "/lib": true},
	}, got)
}

func TestWriteAffected(t *testing.T) {
	res := affectedResult{
		Packages: []string{"./internal/impl/kafka", "./internal/impl/redis"},
		Reasons: map[string][]string{
			"./internal/impl/kafka": {"x"},
			"./internal/impl/redis": {"x"},
		},
	}

	var out, log bytes.Buffer
	assert.NoError(t, writeAffected(&out, &log, res, 10))
	assert.Equal(t, "affected=./internal/impl/kafka ./internal/impl/redis\n"+
		"filters=./internal/impl/kafka ./internal/impl/redis\n"+
		"budget_exceeded=false\n", out.String())

	// ./internal/impl/kafka also selects ./internal/impl/kafka/enterprise, so
	// these two filters select three packages.
	out.Reset()
	assert.NoError(t, writeAffected(&out, &log, res, 2))
	assert.Equal(t, "affected=./internal/impl/kafka ./internal/impl/redis\n"+
		"filters=\n"+
		"budget_exceeded=true\n", out.String())

	out.Reset()
	assert.NoError(t, writeAffected(&out, &log, affectedResult{}, 2))
	assert.Equal(t, "affected=\nfilters=\nbudget_exceeded=false\n", out.String())
}
