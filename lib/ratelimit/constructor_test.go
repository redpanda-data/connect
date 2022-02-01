package ratelimit_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
)

func TestConstructorBadType(t *testing.T) {
	conf := ratelimit.NewConfig()
	conf.Type = "not_exist"

	if _, err := ratelimit.New(conf, nil, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error, received nil for invalid type")
	}
}
