package condition

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestStatic(t *testing.T) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	testMet := metrics.DudType{}

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
