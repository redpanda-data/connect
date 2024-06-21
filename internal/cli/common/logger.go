package common

import (
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

// CreateLogger from a CLI context and a stream config.
func CreateLogger(c *cli.Context, conf config.Type, streamsMode bool) (logger log.Modular, err error) {
	if overrideLogLevel := c.String("log.level"); overrideLogLevel != "" {
		conf.Logger.LogLevel = strings.ToUpper(overrideLogLevel)
	}

	defaultStream := os.Stdout
	if !streamsMode && conf.Output.Type == "stdout" {
		defaultStream = os.Stderr
	}
	logger, err = log.New(defaultStream, ifs.OS(), conf.Logger)
	return
}
