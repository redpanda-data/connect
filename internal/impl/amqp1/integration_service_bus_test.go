package amqp1

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestIntegrationAzureServiceBus(t *testing.T) {
	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	url := os.Getenv("TEST_SB_URL")
	sourceAddress := os.Getenv("TEST_SB_SOURCE_ADDRESS")
	if url == "" || sourceAddress == "" {
		t.Skip("Skipping because of missing TEST_SB_URL or TEST_SB_SOURCE_ADDRESS. Those should be point to Azure Service Bus configured with Message lock duration to 5 seconds.")
	}

	t.Run("TestAMQP1Connected", func(t *testing.T) {
		testAMQP1Connected(url, sourceAddress, t)
	})
	t.Run("TestAMQP1Disconnected", func(t *testing.T) {
		testAMQP1Disconnected(url, sourceAddress, t)
	})
}

func testAMQP1Connected(url, sourceAddress string, t *testing.T) {
	ctx := context.Background()

	conf := input.NewAMQP1Config()
	conf.URL = url
	conf.SourceAddress = sourceAddress
	conf.AzureRenewLock = true

	m, err := newAMQP1Reader(conf, mock.NewManager())
	require.NoError(t, err)

	err = m.Connect(ctx)
	require.NoError(t, err)

	defer func() {
		_ = m.Close(context.Background())
	}()

	client, err := amqp.Dial(url)
	require.NoError(t, err)
	defer client.Close()

	session, err := client.NewSession()
	require.NoError(t, err)
	defer session.Close(ctx)

	sender, err := session.NewSender(
		amqp.LinkTargetAddress("/test"),
	)
	require.NoError(t, err)
	defer sender.Close(ctx)

	N := 10
	wg := sync.WaitGroup{}

	testMsgs := map[string]bool{}
	for i := 0; i < N; i++ {
		wg.Add(1)

		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = true
		go func(testStr string) {
			defer wg.Done()

			contentType := "plain/text"
			contentEncoding := "utf-8"
			createdAt := time.Date(2020, time.January, 30, 1, 0, 0, 0, time.UTC)
			err := sender.Send(ctx, &amqp.Message{
				Properties: &amqp.MessageProperties{
					ContentType:     &contentType,
					ContentEncoding: &contentEncoding,
					CreationTime:    &createdAt,
				},
				Data: [][]byte{[]byte(str)},
			})
			require.NoError(t, err)
		}(str)
	}

	for i := 0; i < N; i++ {
		actM, ackFn, err := m.ReadBatch(ctx)
		assert.NoError(t, err)
		wg.Add(1)

		go func() {
			defer wg.Done()

			assert.True(t, testMsgs[string(actM.Get(0).AsBytes())], "Unexpected message")
			assert.Equal(t, "plain/text", actM.Get(0).MetaGetStr("amqp_content_type"))
			assert.Equal(t, "utf-8", actM.Get(0).MetaGetStr("amqp_content_encoding"))

			time.Sleep(6 * time.Second) // Simulate long processing before ack so message lock expires and lock renewal is requires

			assert.NoError(t, ackFn(ctx, nil))
		}()
	}
	wg.Wait()

	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _, err = m.ReadBatch(readCtx)
	assert.Error(t, err, "got unexpected message (redelivery?)")
}

func testAMQP1Disconnected(url, sourceAddress string, t *testing.T) {
	ctx := context.Background()

	conf := input.NewAMQP1Config()
	conf.URL = url
	conf.SourceAddress = sourceAddress
	conf.AzureRenewLock = true

	m, err := newAMQP1Reader(conf, mock.NewManager())
	require.NoError(t, err)

	err = m.Connect(ctx)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = m.Close(context.Background())
		wg.Done()
	}()

	if _, _, err = m.ReadBatch(ctx); err != component.ErrTypeClosed && err != component.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, component.ErrTypeClosed)
	}

	wg.Wait()
}
