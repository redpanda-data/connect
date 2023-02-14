package aws

import (
	"testing"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewKinesisBalancedReaderConfig(t *testing.T) {
	config := input.NewAWSKinesisConfig()
	config.Streams = []string{"test-kds-1","test-kds-2"}
	mockManager := mock.NewManager()
	kinesisReader, err := newKinesisReader(config, mockManager)
	require.NoError(t, err)
	assert.Equal(t,len(kinesisReader.balancedStreams), 2)
}

func TestNewKinesisBalancedReaderMaxShardConfig(t *testing.T) {
	config := input.NewAWSKinesisConfig()
	config.Streams = []string{"test-kds-1","test-kds-2"}
	config.StreamsMaxShard = []string{"test-kds-1:5","test-kds-2:10"}
	mockManager := mock.NewManager()
	kinesisReader, err := newKinesisReader(config, mockManager)
	require.NoError(t, err)
	assert.Equal(t,len(kinesisReader.balancedStreams), 2)
	assert.Equal(t,len(kinesisReader.streamMaxShards), 2)
	assert.Equal(t, kinesisReader.streamMaxShards["test-kds-1"], 5)
	assert.Equal(t, kinesisReader.streamMaxShards["test-kds-2"], 10)
}

func Test
