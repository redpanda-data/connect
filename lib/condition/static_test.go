package condition

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestStatic(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	conf := NewConfig()
	conf.Type = "static"
	conf.Static = false

	c, err := New(conf, nil, testLog, testMet)
	if err != nil {
		t.Fatal(err)
	}

	if c.Check(message.New(nil)) {
		t.Error("True on static false")
	}

	conf = NewConfig()
	conf.Type = "static"
	conf.Static = true

	c, err = New(conf, nil, testLog, testMet)
	if err != nil {
		t.Fatal(err)
	}

	if !c.Check(message.New(nil)) {
		t.Error("False on static true")
	}
}
