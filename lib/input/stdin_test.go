package input

import (
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestSTDINClose(t *testing.T) {
	s, err := NewSTDIN(NewConfig(), nil, log.Noop(), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	s.CloseAsync()
	if err := s.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
