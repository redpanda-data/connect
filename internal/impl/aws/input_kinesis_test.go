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

type mockDynamoDbKinesis struct {
	dynamodbiface.DynamoDBAPI
	updateItemCount int
	getItemCount int
	putItemCount int
}

func (m *mockDynamoDbKinesis) UpdateItemWithContext(aws.Context, *dynamodb.UpdateItemInput, ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	m.updateItemCount++
	return &dynamodb.UpdateItemOutput{}, nil
}
func (m *mockDynamoDbKinesis) GetItemWithContext(aws.Context, *dynamodb.GetItemInput, ...request.Option) (*dynamodb.GetItemOutput, error){
	m.getItemCount++
	return &dynamodb.GetItemOutput{}, nil
}

func (m * mockDynamoDbKinesis) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error){
	m.putItemCount++
	return &dynamodb.PutItemOutput{}, nil
}

type mockKinesisReader struct {
	kinesisiface.KinesisAPI
	getShardIteratorContextCount int
}
func (m *mockKinesisReader) GetShardIteratorWithContext(aws.Context, *kinesis.GetShardIteratorInput, ...request.Option) (*kinesis.GetShardIteratorOutput, error){
	m.getShardIteratorContextCount++
	return &kinesis.GetShardIteratorOutput{}, nil
}


func TestRunMaxShardConfigBalancedShards(t *testing.T){

	testCases := []struct{
		testName string
		streams        []string
		streamsMaxShard []string
		unclaimedShards map[string]string
		expectedIsError         bool
		expectedNumShardClaimed int
	}{
		{
			testName: "TestMaxConfigShard",
			streams: []string{"test-kds-1","test-kds-2"},
			streamsMaxShard: []string{"test-kds-1:5","test-kds-2:10"},
			unclaimedShards: map[string]string{
				"shard-id-1": "client-id-1",
				"shard-id-2": "client-id-2",
				"shard-id-3": "client-id-3",
				"shard-id-4": "client-id-4",
				"shard-id-5": "client-id-5",
			},
			expectedIsError: false,
			expectedNumShardClaimed: 4,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			config := input.NewAWSKinesisConfig()
			config.Streams = testCase.streams
			config.StreamsMaxShard = testCase.streamsMaxShard
			mockManager := mock.NewManager()
			kinesisReader, _ := newKinesisReader(config, mockManager)
			mockDynamo := mockDynamoDbKinesis{}
			mockKinesis := mockKinesisReader{}
			kinesisReader.svc = &mockKinesis
			kinesisReader.checkpointer = &awsKinesisCheckpointer{input.NewDynamoDBCheckpointConfig(), "",1, 1, &mockDynamo}
			numShardClaimed, err := kinesisReader.maxShardRunBalancedConsumer(&sync.WaitGroup{}, "test-kds-1",testCase.unclaimedShards, 1)
			if (err != nil) != testCase.expectedIsError {
				t.Errorf("Run balanced shard error error = %v, expected=%v", err, testCase.expectedIsError)
				return
			}
			assert.Equal(t,numShardClaimed, testCase.expectedNumShardClaimed)

		})
	}

}