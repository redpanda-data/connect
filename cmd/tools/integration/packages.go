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

package main

import (
	_ "embed"
	"encoding/json"
	"log"
)

const defaultTimeout = "5m"

// TestPackage defines a package to test with an optional custom timeout.
type TestPackage struct {
	Path    string `json:"path"`
	Timeout string `json:"timeout"`
}

// TimeoutStr returns the timeout for go test -timeout, defaulting to 10m.
func (tp TestPackage) TimeoutStr() string {
	if tp.Timeout == "" {
		return defaultTimeout
	}
	return tp.Timeout
}

//go:embed packages.json
var packagesJSON []byte

// allPackages is the CI matrix package list, loaded from packages.json.
var allPackages = func() []TestPackage {
	var pkgs []TestPackage
	if err := json.Unmarshal(packagesJSON, &pkgs); err != nil {
		log.Fatalf("failed to parse packages.json: %v", err)
	}
	return pkgs
}()
