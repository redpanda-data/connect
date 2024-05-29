package main

import (
	"context"
	"log/slog"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

var (
	Version    string
	DateBuilt  string
	BinaryName string = "redpanda-connect"
)

func redpandaTopLevelConfigField() *service.ConfigField {
	return service.NewObjectField("redpanda", kafka.TopicLoggerFields()...)
}

func main() {
	rpLogger := kafka.NewTopicLogger()

	service.RunCLI(
		context.Background(),
		service.CLIOptSetVersion(Version, DateBuilt),
		service.CLIOptSetBinaryName(BinaryName),
		service.CLIOptSetProductName("Redpanda Connect"),
		service.CLIOptSetDocumentationURL("https://docs.redpanda.com/redpanda-connect"),
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
