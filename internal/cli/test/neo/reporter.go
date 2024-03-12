package neo

import (
	"fmt"
	"io"
)

type Format string

var (
	DefaultFormat = Format("default")
)

func GetFormatter(format string, w io.Writer, ew io.Writer) (Formatter, error) {
	switch format {
	case "default":
		return DefaultReporter{w: w, ew: ew}, nil
	}
	return nil, fmt.Errorf("unrecognised format: %v", format)
}

type Formatter interface {
	Error(err error)
	Warn(msg string)

	Render(executions map[string]*SuiteExecution)
}
