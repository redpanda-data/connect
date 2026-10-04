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

// componentlist prints the standard schema, as JSON, for the components that
// the build registers. docs_gen runs it with CGO_ENABLED=0 and no build tags,
// the way the standard release binaries are built, to find the components
// that only cgo builds include.
package main

import (
	"os"

	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/cmd/tools/docs_gen/allcomponents"
)

func main() {
	raw, err := schema.Standard("", "").MarshalJSONV0()
	if err != nil {
		panic(err)
	}
	if _, err := os.Stdout.Write(raw); err != nil {
		panic(err)
	}
}
