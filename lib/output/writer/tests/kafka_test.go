package tests

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestKafkaConfigurationManualPartitioner(t *testing.T) {
	conf := writer.NewKafkaConfig()
	require.NoError(t, createKafkaWriter(conf), "Expected no error with default configuration")

	conf.Partitioner = "manual"
	require.Error(t, createKafkaWriter(conf), "Expected error with manual partitioner set and partition unset")

	conf.Partition = "test"
	require.NoError(t, createKafkaWriter(conf), "Expected no error with manual partitioner set and partition set")

	conf.Partitioner = "random"
	require.Error(t, createKafkaWriter(conf), "Expected error with non-manual partitioner set and partition set")

	conf.Partition = ""
	require.NoError(t, createKafkaWriter(conf), "Expected no error with non-manual partitioner set and partition unset")
}

func createKafkaWriter(conf writer.KafkaConfig) error {
	_, err := writer.NewKafka(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	return err
}
