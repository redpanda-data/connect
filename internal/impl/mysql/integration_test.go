// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationMySQLCDC(t *testing.T) {
	mysqlDsn := os.Getenv("LOCAL_MYSQL_DSN")
	tmpDir := t.TempDir()

	fmt.Println("TMP Ditr: ", tmpDir)
	template := fmt.Sprintf(`
mysql_stream:
  dsn: %s
  stream_snapshot: false
  checkpoint_key: binlogpos
  tables:
    - users
  checkpoint_key: foocache
  flavor: mysql
`, mysqlDsn)

	cacheConf := fmt.Sprintf(`
label: mysql_stream_cache
file:
  directory: %v`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: INFO`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		err = streamOut.Run(context.Background())
		require.NoError(t, err)
	}()

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 10000
	}, time.Minute*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}
