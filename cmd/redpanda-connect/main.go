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
	"github.com/redpanda-data/connect/v4/internal/cli"
	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

var (
	// Version version set at compile time.
	Version string
	// DateBuilt date built set at compile time.
	DateBuilt string
	// BinaryName binary name.
	BinaryName string = "redpanda-connect"
)

func main() {
	cli.InitEnterpriseCLI(BinaryName, Version, DateBuilt, schema.Standard(Version, DateBuilt))
}
