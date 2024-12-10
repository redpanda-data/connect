// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"math"
	"strconv"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestBinlogString(t *testing.T) {
	good := []mysql.Position{
		{Name: "log.0000", Pos: 32},
		{Name: "log@0000", Pos: 32},
		{Name: "log.09999999", Pos: 0},
		{Name: "custom-binlog.9999999", Pos: math.MaxUint32},
	}
	for _, expected := range good {
		str := binlogPositionToString(expected)
		actual, err := parseBinlogPosition(str)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	bad := []string{
		"log.000",
		"log.000@" + strconv.FormatUint(math.MaxUint64, 16),
		"log.000.FF",
	}
	for _, str := range bad {
		_, err := parseBinlogPosition(str)
		require.Error(t, err)
	}
}
