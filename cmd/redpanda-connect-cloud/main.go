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
	"strings"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/cli"

	// Only import a subset of components for execution.
	_ "github.com/redpanda-data/connect/v4/public/components/amqp09"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/changelog"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/crypto"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/maxmind"
	_ "github.com/redpanda-data/connect/v4/public/components/memcached"
	_ "github.com/redpanda-data/connect/v4/public/components/msgpack"
	_ "github.com/redpanda-data/connect/v4/public/components/nats"
	_ "github.com/redpanda-data/connect/v4/public/components/opensearch"
	_ "github.com/redpanda-data/connect/v4/public/components/otlp"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/pure/extended"
	_ "github.com/redpanda-data/connect/v4/public/components/redis"
	_ "github.com/redpanda-data/connect/v4/public/components/sftp"
	_ "github.com/redpanda-data/connect/v4/public/components/sql/base"

	// Import all (supported) sql drivers.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"

	_ "embed"
)

var (
	// Version version set at compile time.
	Version string
	// DateBuilt date built set at compile time.
	DateBuilt string
	// BinaryName binary name.
	BinaryName string = "redpanda-connect"
)

//go:embed allow_list.txt
var allowList string

func main() {
	var allowSlice []string
	for _, s := range strings.Split(allowList, "\n") {
		s = strings.TrimSpace(s)
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		allowSlice = append(allowSlice, s)
	}

	// Observability and scanner plugins aren't necessarily present in our
	// internal lists and so we allow everything that's imported
	env := service.GlobalEnvironment().
		WithBuffers(allowSlice...).
		WithCaches(allowSlice...).
		WithInputs(allowSlice...).
		WithOutputs(allowSlice...).
		WithProcessors(allowSlice...).
		WithRateLimits(allowSlice...)

	// Allow only pure methods and functions within Bloblang.
	benv := bloblang.GlobalEnvironment()
	env.UseBloblangEnvironment(benv.OnlyPure())

	cli.InitEnterpriseCLI(BinaryName, Version, DateBuilt, true, service.CLIOptSetEnvironment(env))
}
