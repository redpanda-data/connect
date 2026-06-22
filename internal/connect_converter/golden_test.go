// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files")

func TestGolden(t *testing.T) {
	cases := []string{
		"s3_sink", "gcs_sink", "bigquery_sink", "snowflake_sink",
		"jdbc_source", "jdbc_sink", "mirror", "avro_s3", "smt_chain",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			in, err := os.ReadFile(filepath.Join("testdata", name+".input.json"))
			require.NoError(t, err)

			res, err := Convert(in)
			require.NoError(t, err)
			assertValidRPCN(t, res.YAML)

			goldenPath := filepath.Join("testdata", name+".expected.yaml")
			if *update {
				require.NoError(t, os.WriteFile(goldenPath, res.YAML, 0o644))
				return
			}
			want, err := os.ReadFile(goldenPath)
			require.NoError(t, err)
			require.Equal(t, string(want), string(res.YAML))
		})
	}
}
