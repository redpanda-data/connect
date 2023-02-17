package aws

import (
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDynamoDbForKinesis struct {
	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamoDbForKinesis) UpdateItemWithContext(aws.Context, *dynamodb.UpdateItemInput, ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}
func (m *mockDynamoDbForKinesis) GetItemWithContext(aws.Context, *dynamodb.GetItemInput, ...request.Option) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamoDbForKinesis) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

type mockKinesisReader struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisReader) GetShardIteratorWithContext(aws.Context, *kinesis.GetShardIteratorInput, ...request.Option) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{}, nil
}

func TestNewKinesisBalancedReaderConfig(t *testing.T) {
	config := input.NewAWSKinesisConfig()
	config.Streams = []string{"test-kds-1", "test-kds-2"}
	mockManager := mock.NewManager()
	kinesisReader, err := newKinesisReader(config, mockManager)
	require.NoError(t, err)
	assert.Equal(t, len(kinesisReader.balancedStreams), 2)
}

func TestNewKinesisBalancedReaderMaxShardConfig(t *testing.T) {
	config := input.NewAWSKinesisConfig()
	config.Streams = []string{"test-kds-1", "test-kds-2"}
	config.StreamsMaxShard = []string{"test-kds-1:5", "test-kds-2:10"}
	mockManager := mock.NewManager()
	kinesisReader, err := newKinesisReader(config, mockManager)
	require.NoError(t, err)
	assert.Equal(t, 2, len(kinesisReader.balancedStreams))
	assert.Equal(t, 2, len(kinesisReader.streamMaxShards))
	assert.Equal(t, 5, kinesisReader.streamMaxShards["test-kds-1"])
	assert.Equal(t, 10, kinesisReader.streamMaxShards["test-kds-2"])
}

func TestRunMaxShardConfigBalancedShards(t *testing.T) {

	testCases := []struct {
		testName                string
		streams                 []string
		streamsMaxShard         []string
		unclaimedShards         map[string]string
		streamNameToClaim       string
		shardsAlreadyClaimed    int
		expectedIsError         bool
		expectedNumShardClaimed int
	}{
		{
			testName:        "TestMaxConfigShard",
			streams:         []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard: []string{"test-kds-1:5", "test-kds-2:10"},
			unclaimedShards: map[string]string{
				"shard-id-1": "client-id-1",
				"shard-id-2": "client-id-2",
				"shard-id-3": "client-id-3",
				"shard-id-4": "client-id-4",
				"shard-id-5": "client-id-5",
			},
			streamNameToClaim:       "test-kds-1",
			shardsAlreadyClaimed:    1,
			expectedIsError:         false,
			expectedNumShardClaimed: 4,
		},
		{
			testName:        "TestMaxConfigShardLessUnclaimedShards",
			streams:         []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard: []string{"test-kds-1:5", "test-kds-2:10"},
			unclaimedShards: map[string]string{
				"shard-id-1": "client-id-1",
				"shard-id-2": "client-id-2",
				"shard-id-3": "client-id-3",
				"shard-id-4": "client-id-4",
				"shard-id-5": "client-id-5",
				"shard-id-6": "client-id-6",
				"shard-id-7": "client-id-7",
			},
			streamNameToClaim:       "test-kds-2",
			shardsAlreadyClaimed:    0,
			expectedIsError:         false,
			expectedNumShardClaimed: 7,
		},
		{
			testName:        "TestNoMaxShardSet",
			streams:         []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard: []string{""},
			unclaimedShards: map[string]string{
				"shard-id-1": "client-id-1",
				"shard-id-2": "client-id-2",
				"shard-id-3": "client-id-3",
				"shard-id-4": "client-id-4",
				"shard-id-5": "client-id-5",
				"shard-id-6": "client-id-6",
				"shard-id-7": "client-id-7",
			},
			streamNameToClaim:       "test-kds-1",
			shardsAlreadyClaimed:    0,
			expectedIsError:         false,
			expectedNumShardClaimed: 7,
		},
	}

	// Setup for mocks to be used.
	mockDynamo := mockDynamoDbForKinesis{}
	mockKinesis := mockKinesisReader{}
	mockManager := mock.NewManager()

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			config := input.NewAWSKinesisConfig()
			config.Streams = testCase.streams
			config.StreamsMaxShard = testCase.streamsMaxShard
			kinesisReader, _ := newKinesisReader(config, mockManager)
			kinesisReader.svc = &mockKinesis
			kinesisReader.checkpointer = &awsKinesisCheckpointer{input.NewDynamoDBCheckpointConfig(), "", 1, 1, &mockDynamo}
			numShardClaimed, err := kinesisReader.maxShardRunBalancedConsumer(&sync.WaitGroup{}, testCase.streamNameToClaim, testCase.unclaimedShards, testCase.shardsAlreadyClaimed)
			assert.Equal(t, testCase.expectedIsError, err != nil)
			assert.Equal(t, testCase.expectedNumShardClaimed, numShardClaimed)

		})
	}

}
