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

package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/redpanda-data/connect/v4/internal/plugins"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/connect/v4/public/components/all"

	_ "embed"
)

func create(t, path string, resBytes []byte) {
	if existing, err := os.ReadFile(path); err == nil {
		if bytes.Equal(existing, resBytes) {
			return
		}
	}
	if err := os.WriteFile(path, resBytes, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("Content for '%v' has changed, updating: %v\n", t, path)
}

func main() {
	plugins.BaseInfo.Hydrate(service.GlobalEnvironment())
	csvBytes, err := plugins.BaseInfo.FormatCSV()
	if err != nil {
		panic(fmt.Sprintf("Failed to format plugins csv: %v", err))
	}

	create("plugins csv fmt", "internal/plugins/info.csv", csvBytes)
}
