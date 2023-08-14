package gcp

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestPubSubOutput(t *testing.T) {
	ctx := context.Background()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test_${! content().string().split("_").index(0) }
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(true, nil).Once()
	fooTopic.On("Stop").Return().Once()

	barTopic := &mockTopic{}
	barTopic.On("Exists").Return(true, nil).Once()
	barTopic.On("Stop").Return().Once()

	client.On("Topic", "test_foo").Return(fooTopic).Once()
	client.On("Topic", "test_bar").Return(barTopic).Once()

	fooMsgA := service.NewMessage([]byte("foo_a"))
	fooResA := &mockPublishResult{}
	fooResA.On("Get").Return("foo_a", nil).Once()
	fooTopic.On("Publish", "foo_a", mock.Anything).Return(fooResA).Once()

	fooMsgB := service.NewMessage([]byte("foo_b"))
	fooResB := &mockPublishResult{}
	fooResB.On("Get").Return("foo_b", nil).Once()
	fooTopic.On("Publish", "foo_b", mock.Anything).Return(fooResB).Once()

	barMsg := service.NewMessage([]byte("bar"))
	barRes := &mockPublishResult{}
	barRes.On("Get").Return("bar", nil).Once()
	barTopic.On("Publish", "bar", mock.Anything).Return(barRes).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(
			t,
			client,
			fooTopic, barTopic,
			fooResA, fooResB, barRes,
		)
	})

	err = out.Connect(ctx)
	require.NoError(t, err, "connect failed")

	err = out.WriteBatch(ctx, service.MessageBatch{fooMsgA, fooMsgB, barMsg})
	require.NoError(t, err, "publish failed")
}

func TestPubSubOutput_MessageAttr(t *testing.T) {
	ctx := context.Background()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test
    ordering_key: '${! content().string() }_${! count(content().string()) }'
    metadata:
      exclude_prefixes:
        - drop_
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(true, nil).Once()
	fooTopic.On("EnableOrdering").Return().Once()
	fooTopic.On("Stop").Return().Once()

	fooMsgA := &mockPublishResult{}
	fooMsgA.On("Get").Return("foo", nil).Once()
	fooTopic.On("Publish", "foo", mock.AnythingOfType("*pubsub.Message")).Return(fooMsgA).Once()

	client.On("Topic", "test").Return(fooTopic).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(
			t,
			client,
			fooTopic,
			fooMsgA,
		)
	})

	err = out.Connect(ctx)
	require.NoError(t, err, "connect failed")

	msg := service.NewMessage([]byte("foo"))
	msg.MetaSet("keep_a", "good stuff")
	msg.MetaSet("drop_b", "oh well")

	err = out.WriteBatch(ctx, service.MessageBatch{msg})
	require.NoError(t, err, "publish failed")

	require.Len(t, fooTopic.Calls, 3)
	require.Equal(t, fooTopic.Calls[2].Method, "Publish")
	require.Len(t, fooTopic.Calls[2].Arguments, 2)
	psmsg := fooTopic.Calls[2].Arguments[1].(*pubsub.Message)
	require.Equal(t, map[string]string{"keep_a": "good stuff"}, psmsg.Attributes)
	require.Equal(t, "foo_1", psmsg.OrderingKey)
}

func TestPubSubOutput_MissingTopic(t *testing.T) {
	ctx := context.Background()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: 'test_${! content().string() }'
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(false, nil).Once()

	barTopic := &mockTopic{}
	barTopic.On("Exists").Return(false, errors.New("simulated error")).Once()

	client.On("Topic", "test_foo").Return(fooTopic).Once()
	client.On("Topic", "test_bar").Return(barTopic).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(t, client, fooTopic, barTopic)
	})

	var bErr *service.BatchError
	errs := []error{}

	batch := service.MessageBatch{service.NewMessage([]byte("foo"))}
	index := batch.Index()

	err = out.WriteBatch(ctx, batch)
	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, `topic 'test_foo' does not exist`)
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "topic 'test_foo' does not exist")

	bErr = nil
	errs = []error{}

	batch = service.MessageBatch{service.NewMessage([]byte("bar"))}
	index = batch.Index()

	err = out.WriteBatch(ctx, batch)
	require.ErrorAsf(t, err, &bErr, "expected a batch error but got: %T: %v", bErr, bErr)
	require.ErrorContains(t, bErr, "failed to validate topic 'test_bar': simulated error")
	bErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err)
		}
		return true
	})
	require.Len(t, errs, 1, "expected one error in batch error")
	require.ErrorContains(t, errs[0], "failed to validate topic 'test_bar': simulated error")
}

func TestPubSubOutput_PublishErrors(t *testing.T) {
	ctx := context.Background()

	conf, err := newPubSubOutputConfig().ParseYAML(`
    project: sample-project
    topic: test_${! content().string().split("_").index(0) }
    `,
		nil,
	)
	require.NoError(t, err, "bad output config")

	client := &mockPubSubClient{}

	fooTopic := &mockTopic{}
	fooTopic.On("Exists").Return(true, nil).Once()
	fooTopic.On("Stop").Return().Once()

	barTopic := &mockTopic{}
	barTopic.On("Exists").Return(true, nil).Once()
	barTopic.On("Stop").Return().Once()

	client.On("Topic", "test_foo").Return(fooTopic).Once()
	client.On("Topic", "test_bar").Return(barTopic).Once()

	fooMsgA := service.NewMessage([]byte("foo_a"))
	fooResA := &mockPublishResult{}
	fooResA.On("Get").Return("", errors.New("simulated foo error")).Once()
	fooTopic.On("Publish", "foo_a", mock.Anything).Return(fooResA).Once()

	fooMsgB := service.NewMessage([]byte("foo_b"))
	fooResB := &mockPublishResult{}
	fooResB.On("Get").Return("foo_b", nil).Once()
	fooTopic.On("Publish", "foo_b", mock.Anything).Return(fooResB).Once()

	barMsg := service.NewMessage([]byte("bar"))
	barRes := &mockPublishResult{}
	barRes.On("Get").Return("", errors.New("simulated bar error")).Once()
	barTopic.On("Publish", "bar", mock.Anything).Return(barRes).Once()

	out, err := newPubSubOutput(conf)
	require.NoError(t, err, "failed to create output")
	out.client = client
	t.Cleanup(func() {
		err = out.Close(ctx)
		require.NoError(t, err, "closing output failed")

		mock.AssertExpectationsForObjects(
			t,
			client,
			fooTopic, barTopic,
			fooResA, fooResB, barRes,
		)
	})

	err = out.Connect(ctx)
	require.NoError(t, err, "connect failed")

	batch := service.MessageBatch{fooMsgA, fooMsgB, barMsg}
	index := batch.Index()

	err = out.WriteBatch(ctx, batch)
	require.Error(t, err, "did not get expected publish error")

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr, "error is not a batch error")
	require.Equal(t, 2, batchErr.IndexedErrors(), "did not receive expected number of batch errors")

	var errs []string
	batchErr.WalkMessagesIndexedBy(index, func(i int, m *service.Message, err error) bool {
		if err != nil {
			errs = append(errs, err.Error())
		}
		return true
	})
	require.ElementsMatch(t, []string{"simulated foo error", "simulated bar error"}, errs)
}

func TestGoogleCredsJsonClientOptions_Success(t *testing.T) {
	encCredJSON := "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiaWRvbm90ZXhpc3Rwcm9qZWN0IiwKICAicHJpdmF0ZV9rZXlfaWQiOiAibng4MDNpbzJ1dDVneGFoM3B4NnRmeTkwd3ltOWZsYzIybzR0aG50eSIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVwhIW5vdGFyZWFscHJpdmF0ZWtleSEhIW56c3Bzd2FzdjZidGE3YW5uZDJhN2loaG00bGthYjViZHVqNWlmYmdmdTh4cnp3YTZtZHA4MHE0MmdhMGZyNWltYzloMHExMm1majA2a2J1MDRjMndpNmd5cXc2bGVnNWE0cmZuaHlpcXZjZzM2aGx1MHpxeHNxZWZpYTZxOXNxMDIyOGRtZzZtdnBsbnpzcDltMnBqdW95N250cXRkcnhoc215d3I4ZXhzN3hydGp1MWV5YTNya2V1dzRva2Q0YjI0aW9pdXZwN3ByaTg4aDJveWhzMzBuaDF6bDBxZ2x5bGEzN2xsYzJ5emx2ODg1MmRlMnV3eWM5M20wcWlpN3Vod2dxdXJ6NHl3djVnenhxaDh6aTV6Z2pwOW52cXU3eDUxcHZjc3lxc3BhNWtkeTY0Z3hndmwwYTN4bDVjZ3IyNDJ2a3VzbXduZHo4b3Rua2poZjI3aTlrdGFiMG5rdnp0eTBwYXNyNmo3Y2FlMWY0bWdicmwwNXJ4a2FjbTYwMHN4eWgzOTl2enBkMTc1cWdzdjBkMXM3cHJ0djc2OHRoa2V1Y3hxdnJvcGViZjYzMGdjZzg2OGFsMTJmazZseHdrOHB0cndkbm95aHJnMXM5ZDlyazRrZW9iY3A4a3pwMzUyZXc2eTF4Z2ttZmliemxlZm0wMWlydG8ydTB1M2xkY2sxd3FveTB1emtxdzA4MGZuZmVqMmUzNzg2d3BjYmVsYTNvZjZlaHp4Y2g4MGl3aGVwNDJjejhpamZzeDR6ZHBwa2p6NHhwN3dmenU0cjNkNWlucjN0MW9xOWJjNnYxdjBqMmhueHFiMzAwOXQ1MHowbGtrdjA5Y3duMzFvdHg0NWMxcG90OTYwdWRkZTQ1M2M2cTA5YWkwcGtzbHVnb2N3OXR4MXR6Z3VoMnZhZjM5cmRtZGo4bndoYjJkMnM1anlwdjc0eWZrdDJoNTU0NnRkYnBydzN5MnF4Mmd1enBzd3IxeWw1ZHRpaHo1amlsdzBvaTd0NDc2cWhranB3dTR1ZnR5NnYyc29mYmZ2M3d4ZmpnODZjaXZjNmdxNGUwYWFvc3BvcXAyd2g4cGRoaXFmZGR0bWZnMmd0Z3RyNDhicGdwbjF0ZzFzeDlmYmFuM3VrZW1nczJjY2wwcnNqOTFqdDkyY2s5MGdxMm1sbnV2c3JyOXhiZHlieXM4azcyZGdranF4d3B3czJtazZ6OTJxMXNjY3N2d2NmbHFxanU0MTJndGg0OWZidjd0b21obTg2ZzR0YWJkdGpiOWYwYWV2dGgwenRkY3ByNWZlbjd1ODhydzYycmRsc25mNTY5Nm0yYzdsdjR2XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAidGVzdG1lQGlkb25vdGV4aXN0cHJvamVjdC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICI5NzM1NzE1MzIyNDUwNjY5MzM3OCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvdGVzdG1lJTQwaWRvbm90ZXhpc3Rwcm9qZWN0LmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9"
	var opt []option.ClientOption
	opt, err := getClientOptionWithCredential(encCredJSON, opt)
	require.NoError(t, err)
	require.Lenf(t, opt, 1, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	expectedValue, _ := base64.StdEncoding.DecodeString(encCredJSON)
	require.EqualValues(t, option.WithCredentialsJSON(expectedValue), actualCredsJSON, "GCP Credentials Json not set as expected.")

}

func TestGoogleCredsJsonClientOptions_InvalidBase64EncodingCred(t *testing.T) {
	encCredJSON := "ewogICJ0eXBlIjogI"
	var opt []option.ClientOption
	opt, err := getClientOptionWithCredential(encCredJSON, opt)
	require.ErrorContains(t, err, "illegal base64 data")
	require.Lenf(t, opt, 0, "Unexpected number of Client Options")
}
