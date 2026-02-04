package aws

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSNSClient struct {
	lastInput  *sns.PublishInput
	publishErr error
}

func (m *mockSNSClient) Publish(ctx context.Context, input *sns.PublishInput, _ ...func(*sns.Options)) (*sns.PublishOutput, error) {
	m.lastInput = input
	return &sns.PublishOutput{}, m.publishErr
}

func TestSNSWriter_SubjectBackwardCompatible(t *testing.T) {
	topic, err := service.NewInterpolatedString("arn:aws:sns:us-east-1:123456789012:MyTopic")
	require.NoError(t, err)
	conf := snsoConfig{
		TopicArn: topic,
		Timeout:  1 * time.Second,
	}
	mockSNS := &mockSNSClient{}
	w, err := newSNSWriter(conf, service.MockResources(), mockSNS)
	require.NoError(t, err)

	msg := service.NewMessage([]byte("hello"))
	err = w.Write(context.Background(), msg)
	assert.NoError(t, err)
	assert.Nil(t, mockSNS.lastInput.Subject, "Subject should be nil for legacy behavior")
}

func TestSNSWriter_SubjectSet(t *testing.T) {
	topic, err := service.NewInterpolatedString("arn:aws:sns:us-east-1:123456789012:MyTopic")
	require.NoError(t, err)
	subj, err := service.NewInterpolatedString("TestSubject")
	require.NoError(t, err)
	conf := snsoConfig{
		TopicArn: topic,
		Timeout:  1 * time.Second,
		Subject:  subj,
	}
	mockSNS := &mockSNSClient{}
	w, err := newSNSWriter(conf, service.MockResources(), mockSNS)
	require.NoError(t, err)

	msg := service.NewMessage([]byte("hello"))
	err = w.Write(context.Background(), msg)
	assert.NoError(t, err)
	if assert.NotNil(t, mockSNS.lastInput.Subject, "Subject should be set") {
		assert.Equal(t, "TestSubject", *mockSNS.lastInput.Subject)
	}
}

func TestSNSWriter_SubjectEmpty(t *testing.T) {
	topic, err := service.NewInterpolatedString("arn:aws:sns:us-east-1:123456789012:MyTopic")
	require.NoError(t, err)
	subj, err := service.NewInterpolatedString("")
	require.NoError(t, err)
	conf := snsoConfig{
		TopicArn: topic,
		Timeout:  1 * time.Second,
		Subject:  subj,
	}
	mockSNS := &mockSNSClient{}
	w, err := newSNSWriter(conf, service.MockResources(), mockSNS)
	require.NoError(t, err)

	msg := service.NewMessage([]byte("hello"))
	err = w.Write(context.Background(), msg)
	assert.NoError(t, err)
	assert.Nil(t, mockSNS.lastInput.Subject, "Subject should be nil when empty string is provided")
}
