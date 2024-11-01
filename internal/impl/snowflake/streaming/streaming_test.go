/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package streaming

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDebugModeDisabled(t *testing.T) {
	// So I can't forget to disable this!
	require.False(t, debug)
}
