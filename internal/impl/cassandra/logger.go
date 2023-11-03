package cassandra

import (
	"bytes"
	"fmt"

	"github.com/gocql/gocql"
)

var _ gocql.StdLogger = (*debugWrapper)(nil)

type logModular interface {
	Debugf(format string, v ...interface{})
}

func newDebugWrapper(log logModular) gocql.StdLogger {
	return &debugWrapper{
		log: log,
	}
}

type debugWrapper struct {
	log logModular
}

func (l *debugWrapper) Printf(format string, v ...interface{}) {
	l.log.Debugf(format, v...)
}

func (l *debugWrapper) Print(v ...interface{}) {
	var buff bytes.Buffer
	_, _ = fmt.Fprint(&buff, v...)

	l.log.Debugf(buff.String())
}

func (l *debugWrapper) Println(v ...interface{}) {
	l.Print(v...)
}
