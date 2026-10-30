// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToStreamMessageIgnoresNonDataMessages(t *testing.T) {
	for name, msg := range map[string]Message{
		"type":     &TypeMessage{DataType: 16385, Namespace: "public", Name: "mood"},
		"truncate": &TruncateMessage{RelationNum: 1, RelationIDs: []uint32{1}},
		"origin":   &OriginMessage{CommitLSN: 1, Name: "repro_origin"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := toStreamMessage(msg, map[uint32]*RelationMessage{}, nil, nil)
			assert.NoError(t, err)
			assert.Nil(t, got)
		})
	}
}
