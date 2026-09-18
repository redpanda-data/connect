// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// testsupport_test.go holds the shared test harness for the whole package:
// the component-bundle blank-imports the benthos linter needs, plus the
// assertValidRPCN / gapConvert helpers used across the connector, converter,
// SMT, and regression test files.

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Register component bundles so service.SetYAML can validate the
	// generated configs. The benthos linter only knows about components
	// that are blank-imported into the test binary.
	_ "github.com/redpanda-data/connect/v4/public/components/avro"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp"
	_ "github.com/redpanda-data/connect/v4/public/components/iceberg"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/mssqlserver"
	_ "github.com/redpanda-data/connect/v4/public/components/mysql"
	_ "github.com/redpanda-data/connect/v4/public/components/oracledb"
	_ "github.com/redpanda-data/connect/v4/public/components/postgresql"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/pure/extended"
	_ "github.com/redpanda-data/connect/v4/public/components/snowflake"
	_ "github.com/redpanda-data/connect/v4/public/components/sql"
)

// assertValidRPCN parses YAML through the benthos stream builder to prove it is
// valid Redpanda Connect config, not just well-formed YAML.
func assertValidRPCN(t *testing.T, yamlBytes []byte) {
	t.Helper()
	b := service.NewStreamBuilder()
	require.NoError(t, b.SetYAML(string(yamlBytes)), "generated YAML is not valid RPCN config:\n%s", yamlBytes)
}

// gapConvert converts a Kafka Connect config and asserts the result is
// lint-valid RPCN, returning the YAML for further assertions.
func gapConvert(t *testing.T, in string) string {
	t.Helper()
	res, err := Convert([]byte(in))
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	return string(res.YAML)
}
