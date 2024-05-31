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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
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

	conf, err := amqp1InputSpec().ParseYAML(fmt.Sprintf(`
url: %v
source_address: %v
azure_renew_lock: true
`, url, sourceAddress), nil)
	require.NoError(t, err)

	m, err := amqp1ReaderFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	err = m.Connect(ctx)
	require.NoError(t, err)

	defer func() {
		_ = m.Close(context.Background())
	}()

	client, err := amqp.Dial(ctx, url, nil)
	require.NoError(t, err)
	defer client.Close()

	session, err := client.NewSession(ctx, nil)
	require.NoError(t, err)
	defer session.Close(ctx)

	sender, err := session.NewSender(ctx, "/test", nil)
	require.NoError(t, err)
	defer sender.Close(ctx)

	wg := sync.WaitGroup{}

	tests := []struct {
		data            string
		value           interface{}
		expectedContent string
	}{
		{"hello world: 0", nil, "hello world: 0"},
		{"hello world: 1", nil, "hello world: 1"},
		{"hello world: 2", nil, "hello world: 2"},
		{"", "hello world: 3", "hello world: 3"},
		{"", "hello world: 4", "hello world: 4"},
		{"", "hello world: 5", "hello world: 5"},
	}

	for _, test := range tests {
		wg.Add(1)

		go func(data string, value interface{}) {
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
				Data:  [][]byte{[]byte(data)},
				Value: value,
			}, nil)
			require.NoError(t, err)
		}(test.data, test.value)
	}

	want := map[string]bool{}
	for _, test := range tests {
		want[test.expectedContent] = true
	}

	for range tests {
		actM, ackFn, err := m.ReadBatch(ctx)
		assert.NoError(t, err)
		wg.Add(1)

		go func() {
			defer wg.Done()

			content, err := actM[0].AsBytes()
			require.NoError(t, err)
			assert.True(t, want[string(content)], "Unexpected message")

			m, _ := actM[0].MetaGetMut("amqp_content_type")
			assert.Equal(t, "plain/text", m)

			m, _ = actM[0].MetaGetMut("amqp_content_encoding")
			assert.Equal(t, "utf-8", m)

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

	conf, err := amqp1InputSpec().ParseYAML(fmt.Sprintf(`
url: %v
source_address: %v
azure_renew_lock: true
`, url, sourceAddress), nil)
	require.NoError(t, err)

	m, err := amqp1ReaderFromParsed(conf, service.MockResources())
	require.NoError(t, err)

	err = m.Connect(ctx)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_ = m.Close(context.Background())
		wg.Done()
	}()

	if _, _, err = m.ReadBatch(ctx); err != service.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, service.ErrNotConnected)
	}

	wg.Wait()
}
