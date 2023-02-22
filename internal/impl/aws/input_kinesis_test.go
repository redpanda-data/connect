package aws

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/session"

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

// mockDynamoDbForKinesis and mockKinesisReader is used to mock the underlying AWS API that is called by runConsumer()
// The following tests only runs through the happy path of the runConsumer() code
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
func (m *mockDynamoDbForKinesis) DeleteItemWithContext(aws.Context, *dynamodb.DeleteItemInput, ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil

}

type mockKinesisReader struct {
	kinesisiface.KinesisAPI
}

func (m *mockKinesisReader) GetShardIteratorWithContext(aws.Context, *kinesis.GetShardIteratorInput, ...request.Option) (*kinesis.GetShardIteratorOutput, error) {
	shardId := "2"
	return &kinesis.GetShardIteratorOutput{ShardIterator: &shardId}, nil
}

func (m *mockKinesisReader) GetRecordsWithContext(aws.Context, *kinesis.GetRecordsInput, ...request.Option) (*kinesis.GetRecordsOutput, error) {
	milisecondsBehind := int64(0)
	return &kinesis.GetRecordsOutput{
		ChildShards:        []*kinesis.ChildShard{},
		MillisBehindLatest: &milisecondsBehind,
		NextShardIterator:  nil,
		Records:            []*kinesis.Record{},
	}, nil
}

func NewAwsTestConfig(streams []string, streamsMaxShard []string) input.AWSKinesisConfig {
	return input.AWSKinesisConfig{
		Config:          session.NewConfig(),
		Streams:         streams,
		StreamsMaxShard: streamsMaxShard,
		DynamoDB:        input.NewDynamoDBCheckpointConfig(),
		CheckpointLimit: 1024,
		CommitPeriod:    "5s",
		LeasePeriod:     "30s",
		RebalancePeriod: "30s",
		StartFromOldest: true,
		Batching:        batchconfig.NewConfig(),
	}
}

func TestNewKinesisBalancedReaderConfig(t *testing.T) {
	config := input.NewAWSKinesisConfig()
	config.Streams = []string{"test-kds-1", "test-kds-2"}
	kinesisReader, err := newKinesisReader(config, mock.NewManager())
	require.NoError(t, err)
	assert.Equal(t, len(kinesisReader.balancedStreams), 2)
}

func TestNewKinesisBalancedReaderMaxShardConfig(t *testing.T) {
	config := NewAwsTestConfig([]string{"test-kds-1", "test-kds-2"}, []string{"test-kds-1:5", "test-kds-2:10"})
	kinesisReader, err := newKinesisReader(config, mock.NewManager())
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
			//Client has capacity to claim 4 more shards
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
			// Client has capacity to claim all 7 shards since it has max 10 shards
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
			//Client has no max shard set so it can claim all
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
			config := NewAwsTestConfig(testCase.streams, testCase.streamsMaxShard)
			kinesisReader, _ := newKinesisReader(config, mockManager)
			kinesisReader.svc = &mockKinesis
			kinesisReader.checkpointer = &awsKinesisCheckpointer{input.NewDynamoDBCheckpointConfig(), "", 1, 1, &mockDynamo}
			numShardClaimed, err := kinesisReader.maxShardRunBalancedConsumer(&sync.WaitGroup{}, testCase.streamNameToClaim, testCase.unclaimedShards, testCase.shardsAlreadyClaimed)
			assert.Equal(t, testCase.expectedIsError, err != nil)
			assert.Equal(t, testCase.expectedNumShardClaimed, numShardClaimed)

		})
	}

}

func createKinesisClientClaim(numShards int) []awsKinesisClientClaim {
	var clientClaims []awsKinesisClientClaim
	for i := 0; i < numShards; i++ {
		clientClaims = append(clientClaims, awsKinesisClientClaim{ShardID: strconv.Itoa(i), LeaseTimeout: time.Now()})
	}
	return clientClaims
}

func TestStealShard(t *testing.T) {

	testCases := []struct {
		testName          string
		streams           []string
		streamsMaxShard   []string
		clientClaims      map[string][]awsKinesisClientClaim
		streamNameToClaim string
		clientId          string
		expectedIsError   bool
		expectedIsSuccess bool
	}{
		{
			//ClientId has capacity to steal 1 shard so it steals
			testName:          "SucessfullyStealShard",
			streams:           []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard:   []string{"test-kds-1:5", "test-kds-2:10"},
			streamNameToClaim: "test-kds-1",
			clientClaims: map[string][]awsKinesisClientClaim{
				"clientId":    createKinesisClientClaim(4),
				"otherClient": createKinesisClientClaim(6),
			},
			clientId:          "clientId",
			expectedIsError:   false,
			expectedIsSuccess: true,
		},
		{
			//Other client doesnt have enough shards so it will not steal from it
			testName:          "OtherClientNotEnoughShards",
			streams:           []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard:   []string{"test-kds-1:5", "test-kds-2:10"},
			streamNameToClaim: "test-kds-1",
			clientClaims: map[string][]awsKinesisClientClaim{
				"clientId":    createKinesisClientClaim(4),
				"otherClient": createKinesisClientClaim(5),
			},
			clientId:          "clientId",
			expectedIsError:   false,
			expectedIsSuccess: false,
		},
		{
			//ClientId already has max shard of 5, so should not steal shards
			testName:          "MaxShardCapacityNoSteal",
			streams:           []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard:   []string{"test-kds-1:5", "test-kds-2:10"},
			streamNameToClaim: "test-kds-1",
			clientClaims: map[string][]awsKinesisClientClaim{
				"clientId":    createKinesisClientClaim(5),
				"otherClient": createKinesisClientClaim(7),
			},
			clientId:          "clientId",
			expectedIsError:   false,
			expectedIsSuccess: false,
		},

		{
			//Since there is no maxShardConfig, it should steal the other client shards
			testName:          "NoStreamsMaxShard",
			streams:           []string{"test-kds-1", "test-kds-2"},
			streamsMaxShard:   []string{""},
			streamNameToClaim: "test-kds-1",
			clientClaims: map[string][]awsKinesisClientClaim{
				"clientId":    createKinesisClientClaim(5),
				"otherClient": createKinesisClientClaim(8),
			},
			clientId:          "clientId",
			expectedIsError:   false,
			expectedIsSuccess: true,
		},
	}

	// Setup for mocks to be used.
	mockDynamo := mockDynamoDbForKinesis{}
	mockKinesis := mockKinesisReader{}
	mockManager := mock.NewManager()

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			config := NewAwsTestConfig(testCase.streams, testCase.streamsMaxShard)
			kinesisReader, _ := newKinesisReader(config, mockManager)
			kinesisReader.svc = &mockKinesis
			kinesisReader.clientID = testCase.clientId
			kinesisReader.checkpointer = &awsKinesisCheckpointer{input.NewDynamoDBCheckpointConfig(), "", 1, 1, &mockDynamo}
			isSuccess, err := kinesisReader.stealShard(&sync.WaitGroup{}, testCase.streamNameToClaim, testCase.clientClaims)
			assert.Equal(t, testCase.expectedIsError, err != nil)
			assert.Equal(t, testCase.expectedIsSuccess, isSuccess)

		})
	}

}
