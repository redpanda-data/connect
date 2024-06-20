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
	"context"
	"log/slog"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"

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

func redpandaTopLevelConfigField() *service.ConfigField {
	return service.NewObjectField("redpanda", enterprise.TopicLoggerFields()...)
}

func main() {
	rpLogger := enterprise.NewTopicLogger()

	service.RunCLI(
		context.Background(),
		service.CLIOptSetVersion(Version, DateBuilt),
		service.CLIOptSetBinaryName(BinaryName),
		service.CLIOptSetProductName("Redpanda Connect"),
		service.CLIOptSetDefaultConfigPaths(
			"redpanda-connect.yaml",
			"/redpanda-connect.yaml",
			"/etc/redpanda-connect/config.yaml",
			"/etc/redpanda-connect.yaml",

			"connect.yaml",
			"/connect.yaml",
			"/etc/connect/config.yaml",
			"/etc/connect.yaml",

			// Keep these for now, for backwards compatibility
			"/benthos.yaml",
			"/etc/benthos/config.yaml",
			"/etc/benthos.yaml",
		),
		service.CLIOptSetDocumentationURL("https://docs.redpanda.com/redpanda-connect"),
		service.CLIOptSetShowRunCommand(true),
		service.CLIOptSetMainSchemaFrom(func() *service.ConfigSchema {
			return service.NewEnvironment().FullConfigSchema(Version, DateBuilt).
				Field(redpandaTopLevelConfigField())
		}),
		service.CLIOptOnLoggerInit(func(l *service.Logger) {
			rpLogger.SetFallbackLogger(l)
		}),
		service.CLIOptAddTeeLogger(slog.New(rpLogger)),
		service.CLIOptOnConfigParse(func(fn *service.ParsedConfig) error {
			return rpLogger.InitOutputFromParsed(fn.Namespace("redpanda"))
		}),
	)
}
