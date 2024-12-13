// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import "fmt"

// TableFQN is both a table name AND a schema name
//
// TableFQN should always be SAFE and validated before creating
type TableFQN struct {
	Schema string
	Table  string
}

// String satifies the Stringer interface
func (t TableFQN) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}
