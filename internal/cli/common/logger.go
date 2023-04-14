package common

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/urfave/cli/v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

// CreateLogger from a CLI context and a stream config.
func CreateLogger(c *cli.Context, conf config.Type, streamsMode bool) (logger log.Modular, err error) {
	if overrideLogLevel := c.String("log.level"); len(overrideLogLevel) > 0 {
		conf.Logger.LogLevel = strings.ToUpper(overrideLogLevel)
	}
	if conf.Logger.File.Path != "" {
		var writer io.Writer
		if conf.Logger.File.Rotate {
			writer = &lumberjack.Logger{
				Filename:   conf.Logger.File.Path,
				MaxSize:    10,
				MaxAge:     conf.Logger.File.RotateMaxAge,
				MaxBackups: 1,
				Compress:   true,
			}
		} else {
			var fw fs.File
			if fw, err = ifs.OS().OpenFile(conf.Logger.File.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666); err == nil {
				var isw bool
				if writer, isw = fw.(io.Writer); !isw {
					err = errors.New("failed to open a writable file")
				}
			}
		}
		if err == nil {
			logger, err = log.New(writer, conf.Logger)
		}
	} else {
		// Note: Only log to Stderr if our output is stdout, brokers aren't counted
		// here as this is only a special circumstance for very basic use cases.
		if !streamsMode && conf.Output.Type == "stdout" {
			logger, err = log.New(os.Stderr, conf.Logger)
		} else {
			logger, err = log.New(os.Stdout, conf.Logger)
		}
	}
	return
}
