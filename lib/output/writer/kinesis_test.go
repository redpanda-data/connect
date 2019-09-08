package writer

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff"
	"github.com/ory/dockertest"
)

var (
	mockStats = metrics.DudType{}
)

var (
	mThrottled       = mockStats.GetCounter("send.throttled")
	mThrottledF      = mockStats.GetCounter("send.throttled")
	mPartsThrottled  = mockStats.GetCounter("parts.send.throttled")
	mPartsThrottledF = mockStats.GetCounter("parts.send.throttled")
)

type mockKinesis struct {
	kinesisiface.KinesisAPI
	fn func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

func (m *mockKinesis) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	return m.fn(input)
}

func TestKinesisWriteSinglePartMessage(t *testing.T) {
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				if exp, act := 1, len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				if exp, act := "123", input.Records[0].PartitionKey; exp != *act {
					return nil, fmt.Errorf("expected record to have partition key %s, got %s", exp, *act)
				}
				return &kinesis.PutRecordsOutput{}, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	part := message.NewPart([]byte(`{"foo":"bar","id":123}`))
	msg.Append(part)

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisWriteMultiPartMessage(t *testing.T) {
	parts := []struct {
		data []byte
		key  string
	}{
		{[]byte(`{"foo":"bar","id":123}`), "123"},
		{[]byte(`{"foo":"baz","id":456}`), "456"},
	}
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				if exp, act := len(parts), len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				for i, p := range parts {
					if exp, act := p.key, input.Records[i].PartitionKey; exp != *act {
						return nil, fmt.Errorf("expected record %d to have partition key %s, got %s", i, exp, *act)
					}
				}
				return &kinesis.PutRecordsOutput{}, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	for _, p := range parts {
		part := message.NewPart(p.data)
		msg.Append(part)
	}

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisWriteChunk(t *testing.T) {
	batchLengths := []int{}
	n := 1200
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				batchLengths = append(batchLengths, len(input.Records))
				return &kinesis.PutRecordsOutput{}, nil
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	for i := 0; i < n; i++ {
		part := message.NewPart([]byte(`{"foo":"bar","id":123}`))
		msg.Append(part)
	}

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
	if exp, act := n/kinesisMaxRecordsCount+1, len(batchLengths); act != exp {
		t.Errorf("Expected kinesis PutRecords to have call count %d, got %d", exp, act)
	}
	for i, act := range batchLengths {
		exp := n
		if exp > kinesisMaxRecordsCount {
			exp = kinesisMaxRecordsCount
			n -= kinesisMaxRecordsCount
		}
		if act != exp {
			t.Errorf("Expected kinesis PutRecords call %d to have batch size %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteChunkWithThrottling(t *testing.T) {
	t.Parallel()
	batchLengths := []int{}
	n := 1200
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				count := len(input.Records)
				batchLengths = append(batchLengths, count)
				var failed int64
				output := kinesis.PutRecordsOutput{
					Records: make([]*kinesis.PutRecordsResultEntry, count),
				}
				for i := 0; i < count; i++ {
					var entry kinesis.PutRecordsResultEntry
					if i >= 300 {
						failed++
						entry.SetErrorCode(kinesis.ErrCodeProvisionedThroughputExceededException)
					}
					output.Records[i] = &entry
				}
				output.SetFailedRecordCount(failed)
				return &output, nil
			},
		},
		mThrottled:       mThrottled,
		mThrottledF:      mThrottledF,
		mPartsThrottled:  mPartsThrottled,
		mPartsThrottledF: mPartsThrottledF,
		log:              log.Noop(),
		partitionKey:     text.NewInterpolatedString("${!json_field:id}"),
		hashKey:          text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	for i := 0; i < n; i++ {
		part := message.NewPart([]byte(`{"foo":"bar","id":123}`))
		msg.Append(part)
	}

	expectedLengths := []int{
		500, 500, 500, 300,
	}

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
	if exp, act := len(expectedLengths), len(batchLengths); act != exp {
		t.Errorf("Expected kinesis PutRecords to have call count %d, got %d", exp, act)
	}
	for i, act := range batchLengths {
		if exp := expectedLengths[i]; act != exp {
			t.Errorf("Expected kinesis PutRecords call %d to have batch size %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteError(t *testing.T) {
	t.Parallel()
	var calls int
	k := Kinesis{
		backoff: backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				calls++
				return nil, errors.New("blah")
			},
		},
		log:          log.Noop(),
		partitionKey: text.NewInterpolatedString("${!json_field:id}"),
		hashKey:      text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar"}`)))

	if exp, err := "blah", k.Write(msg); err.Error() != exp {
		t.Errorf("Expected err to equal %s, got %v", exp, err)
	}
	if exp, act := 3, calls; act != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, act)
	}
}

func TestKinesisWriteMessageThrottling(t *testing.T) {
	t.Parallel()
	var calls [][]*kinesis.PutRecordsRequestEntry
	k := Kinesis{
		backoff: backoff.NewExponentialBackOff(),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				records := make([]*kinesis.PutRecordsRequestEntry, len(input.Records))
				copy(records, input.Records)
				calls = append(calls, records)
				var failed int64
				var output kinesis.PutRecordsOutput
				for i := 0; i < len(input.Records); i++ {
					entry := kinesis.PutRecordsResultEntry{}
					if i > 0 {
						failed++
						entry.SetErrorCode(kinesis.ErrCodeProvisionedThroughputExceededException)
					}
					output.Records = append(output.Records, &entry)
				}
				output.SetFailedRecordCount(failed)
				return &output, nil
			},
		},
		mThrottled:       mThrottled,
		mThrottledF:      mThrottledF,
		mPartsThrottled:  mPartsThrottled,
		mPartsThrottledF: mPartsThrottledF,
		log:              log.Noop(),
		partitionKey:     text.NewInterpolatedString("${!json_field:id}"),
		hashKey:          text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar","id":123}`)))
	msg.Append(message.NewPart([]byte(`{"foo":"baz","id":456}`)))
	msg.Append(message.NewPart([]byte(`{"foo":"qux","id":789}`)))

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
	if exp, act := msg.Len(), len(calls); act != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, act)
	}
	for i, c := range calls {
		if exp, act := msg.Len()-i, len(c); act != exp {
			t.Errorf("Expected kinesis.PutRecords call %d input to have Records with length %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteBackoffMaxRetriesExceeded(t *testing.T) {
	t.Parallel()
	var calls int
	k := Kinesis{
		backoff: backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2),
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		kinesis: &mockKinesis{
			fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
				calls++
				var output kinesis.PutRecordsOutput
				output.FailedRecordCount = aws.Int64(1)
				output.Records = append(output.Records, &kinesis.PutRecordsResultEntry{
					ErrorCode: aws.String(kinesis.ErrCodeProvisionedThroughputExceededException),
				})
				return &output, nil
			},
		},
		mThrottled:       mThrottled,
		mThrottledF:      mThrottledF,
		mPartsThrottled:  mPartsThrottled,
		mPartsThrottledF: mPartsThrottledF,
		log:              log.Noop(),
		partitionKey:     text.NewInterpolatedString("${!json_field:id}"),
		hashKey:          text.NewInterpolatedString(""),
	}

	msg := message.New(nil)
	msg.Append(message.NewPart([]byte(`{"foo":"bar","id":123}`)))

	if err := k.Write(msg); err == nil {
		t.Error(errors.New("expected kinesis.Write to error"))
	}
	if exp := 3; calls != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, calls)
	}
}

func TestKinesisIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	// start mysql container with binlog enabled
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "vsouza/kinesis-local",
		Cmd: []string{
			"--createStreamMs=5",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	port, err := strconv.ParseInt(resource.GetPort("4567/tcp"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("http://localhost:%d", port)
	config := KinesisConfig{
		Stream:       "foo",
		PartitionKey: "${!json_field:id}",
	}
	config.Region = "us-east-1"
	config.Endpoint = endpoint
	config.Credentials = sess.CredentialsConfig{
		ID:     "xxxxxx",
		Secret: "xxxxxx",
		Token:  "xxxxxx",
	}

	// bootstrap kinesis
	client := kinesis.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String("us-east-1"),
	})))
	if err := pool.Retry(func() error {
		_, err := client.CreateStream(&kinesis.CreateStreamInput{
			ShardCount: aws.Int64(1),
			StreamName: aws.String(config.Stream),
		})
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("testKinesisConnect", func(t *testing.T) {
		testKinesisConnect(t, config, client)
	})
}

func testKinesisConnect(t *testing.T, c KinesisConfig, client *kinesis.Kinesis) {
	met := metrics.DudType{}
	log := log.Noop()
	r, err := NewKinesis(c, log, met)
	if err != nil {
		t.Fatal(err)
	}

	if err := r.Connect(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.CloseAsync()
		if err := r.WaitForClose(time.Millisecond); err != nil {
			t.Error(err)
		}
	}()

	records := [][]byte{
		[]byte(`{"foo":"bar","id":123}`),
		[]byte(`{"foo":"baz","id":456}`),
		[]byte(`{"foo":"qux","id":789}`),
	}

	msg := message.New(nil)
	for _, record := range records {
		msg.Append(message.NewPart(record))
	}

	if err := r.Write(msg); err != nil {
		t.Fatal(err)
	}

	iterator, err := client.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: aws.String(kinesis.ShardIteratorTypeTrimHorizon),
		StreamName:        aws.String(c.Stream),
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetRecords(&kinesis.GetRecordsInput{
		Limit:         aws.Int64(10),
		ShardIterator: iterator.ShardIterator,
	})
	if err != nil {
		t.Error(err)
	}
	if act, exp := len(out.Records), len(records); act != exp {
		t.Fatalf("Expected GetRecords response to have records with length of %d, got %d", exp, act)
	}
	for i, record := range records {
		if string(out.Records[i].Data) != string(record) {
			t.Errorf("Expected record %d to equal %v, got %v", i, record, out.Records[i])
		}
	}
}
