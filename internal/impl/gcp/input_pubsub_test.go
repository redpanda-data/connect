package gcp

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestGCPPubSubReaderRead(t *testing.T) {
	t.Run("respects context cancellation", func(t *testing.T) {
		reader := &gcpPubSubReader{
			msgsChan: make(chan *pubsub.Message),
			log:      service.MockResources().Logger(),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, _, err := reader.Read(ctx)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("returns ErrNotConnected when msgsChan is nil", func(t *testing.T) {
		reader := &gcpPubSubReader{
			msgsChan: nil,
			log:      service.MockResources().Logger(),
		}

		_, _, err := reader.Read(context.Background())
		assert.Equal(t, service.ErrNotConnected, err)
	})

	t.Run("returns ErrNotConnected when channel is closed", func(t *testing.T) {
		ch := make(chan *pubsub.Message)
		close(ch)

		reader := &gcpPubSubReader{
			msgsChan: ch,
			log:      service.MockResources().Logger(),
		}

		_, _, err := reader.Read(context.Background())
		assert.Equal(t, service.ErrNotConnected, err)
	})

	t.Run("correctly processes message", func(t *testing.T) {
		ch := make(chan *pubsub.Message, 1)

		publishTime := time.Now()
		deliveryAttempt := int(3)

		// Create a pubsub message with test data
		psMsg := &pubsub.Message{
			Data:            []byte("test data"),
			ID:              "test-id",
			PublishTime:     publishTime,
			Attributes:      map[string]string{"key1": "value1", "key2": "value2"},
			DeliveryAttempt: &deliveryAttempt,
			OrderingKey:     "test-ordering-key",
		}

		ch <- psMsg

		reader := &gcpPubSubReader{
			msgsChan: ch,
			log:      service.MockResources().Logger(),
		}

		msg, ackFn, err := reader.Read(context.Background())
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.NotNil(t, ackFn)

		data, err := msg.AsBytes()
		assert.NoError(t, err)
		// Verify message content
		assert.Equal(t, "test data", string(data))

		// Verify metadata
		metaValue, found := msg.MetaGet("key1")
		require.True(t, found)
		assert.Equal(t, "value1", metaValue)

		metaValue, found = msg.MetaGet("key2")
		require.True(t, found)
		assert.Equal(t, "value2", metaValue)

		metaValue, found = msg.MetaGet(metaMessageID)
		require.True(t, found)
		assert.Equal(t, "test-id", metaValue)

		gotTime, found := msg.MetaGetMut(metaPublishTimeUnix)
		require.True(t, found)
		assert.Equal(t, publishTime.Unix(), gotTime.(int64))

		metaValue, found = msg.MetaGet(metaDeliveryAttempt)
		require.True(t, found)
		assert.Equal(t, "3", metaValue)

		metaValue, found = msg.MetaGet(metaOrderingKey)
		require.True(t, found)
		assert.Equal(t, "test-ordering-key", metaValue)
	})
}
