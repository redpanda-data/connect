// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIncrementalHooksNilStateIsNoop(t *testing.T) {
	var inc *State
	assert.Equal(t, 0, inc.TouchRecords(nil))
	inc.ObserveRecords("s", nil)
	inc.ObserveIdle("s", time.Now())
	inc.Exhausted("s")
	inc.Register("s")
	inc.RefreshDone(nil)
}
