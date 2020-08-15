// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/ory/dockertest/v3"
)

func TestAMQP1Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("rmohr/activemq", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

	url := fmt.Sprintf("amqp://guest:guest@localhost:%v/", resource.GetPort("5672/tcp"))

	if err = pool.Retry(func() error {
		client, err := amqp.Dial(url)
		if err == nil {
			client.Close()
		}
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestAMQP1StreamsALOAsync", func(te *testing.T) {
		testAMQP1StreamsALOAsync(url, te)
	})
}

func testAMQP1StreamsALOAsync(url string, t *testing.T) {
	target := "queue:/benthos_test_streams_alo_async"

	outConf := writer.NewAMQP1Config()
	outConf.URL = url
	outConf.TargetAddress = target

	inConf := reader.NewAMQP1Config()
	inConf.URL = url
	inConf.SourceAddress = target

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewAMQP1(outConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mOutput.Connect()
		return
	}
	inputCtr := func() (mInput reader.Async, err error) {
		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		if mInput, err = reader.NewAMQP1(inConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mInput.ConnectWithContext(ctx)
		return
	}

	checkALOSynchronousAsync(outputCtr, inputCtr, t)

	target = "queue:/benthos_test_streams_alo_with_dc_async"

	outConf.TargetAddress = target
	inConf.SourceAddress = target

	checkALOSynchronousAndDieAsync(outputCtr, inputCtr, t)

	target = "queue:/benthos_test_streams_alo_parallel_async"

	outConf.TargetAddress = target
	inConf.SourceAddress = target

	checkALOParallelAsync(outputCtr, inputCtr, 50, t)

	target = "queue:/benthos_test_streams_alo_parallel_async_parallel_writes"

	outConf.TargetAddress = target
	inConf.SourceAddress = target

	checkALOAsyncParallelWrites(outputCtr, inputCtr, 50, t)
}
