package reader

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAMQP1Integration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

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

	conf := NewAMQP1Config()
	conf.URL = url
	conf.SourceAddress = sourceAddress
	conf.AzureRenewLock = true

	m, err := NewAMQP1(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = m.ConnectWithContext(ctx)
	require.NoError(t, err)

	defer func() {
		m.CloseAsync()
		err := m.WaitForClose(time.Second)
		assert.NoError(t, err)
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
		actM, ackFn, err := m.ReadWithContext(ctx)
		assert.NoError(t, err)
		wg.Add(1)

		go func() {
			defer wg.Done()

			assert.True(t, testMsgs[string(actM.Get(0).Get())], "Unexpected message")
			assert.Equal(t, "plain/text", actM.Get(0).MetaGet("amqp_content_type"))
			assert.Equal(t, "utf-8", actM.Get(0).MetaGet("amqp_content_encoding"))

			time.Sleep(6 * time.Second) // Simulate long processing before ack so message lock expires and lock renewal is requires

			assert.NoError(t, ackFn(ctx, response.NewAck()))
		}()
	}
	wg.Wait()

	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _, err = m.ReadWithContext(readCtx)
	assert.Error(t, err, "got unexpected message (redelivery?)")

}

func testAMQP1Disconnected(url, sourceAddress string, t *testing.T) {
	ctx := context.Background()

	conf := NewAMQP1Config()
	conf.URL = url
	conf.SourceAddress = sourceAddress
	conf.AzureRenewLock = true

	m, err := NewAMQP1(conf, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	err = m.ConnectWithContext(ctx)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		m.CloseAsync()
		err := m.WaitForClose(time.Second)
		require.NoError(t, err)
		wg.Done()
	}()

	if _, _, err = m.ReadWithContext(ctx); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
