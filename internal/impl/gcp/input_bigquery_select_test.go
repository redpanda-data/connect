package gcp

import (
	"context"
	"encoding/base64"
	"errors"
	"google.golang.org/api/option"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var testBQInputYAML = `
project: job-project
table: bigquery-public-data.samples.shakespeare
columns:
  - word
  - sum(word_count) as total_count
where: length(word) >= ?
suffix: |
  GROUP BY word
  ORDER BY total_count DESC
  LIMIT 10
args_mapping: |
  root = [ 3 ]
`

// credentials_json_encoded value is base64 encoded valid json with dummy data
var testBQInputYAMLWithCredsJSON = `
project: job-project
table: bigquery-public-data.samples.shakespeare
columns:
  - word
  - sum(word_count) as total_count
where: length(word) >= ?
suffix: |
  GROUP BY word
  ORDER BY total_count DESC
  LIMIT 10
args_mapping: |
  root = [ 3 ]
credentials_json_encoded: ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiaWRvbm90ZXhpc3Rwcm9qZWN0IiwKICAicHJpdmF0ZV9rZXlfaWQiOiAibng4MDNpbzJ1dDVneGFoM3B4NnRmeTkwd3ltOWZsYzIybzR0aG50eSIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVwhIW5vdGFyZWFscHJpdmF0ZWtleSEhIW56c3Bzd2FzdjZidGE3YW5uZDJhN2loaG00bGthYjViZHVqNWlmYmdmdTh4cnp3YTZtZHA4MHE0MmdhMGZyNWltYzloMHExMm1majA2a2J1MDRjMndpNmd5cXc2bGVnNWE0cmZuaHlpcXZjZzM2aGx1MHpxeHNxZWZpYTZxOXNxMDIyOGRtZzZtdnBsbnpzcDltMnBqdW95N250cXRkcnhoc215d3I4ZXhzN3hydGp1MWV5YTNya2V1dzRva2Q0YjI0aW9pdXZwN3ByaTg4aDJveWhzMzBuaDF6bDBxZ2x5bGEzN2xsYzJ5emx2ODg1MmRlMnV3eWM5M20wcWlpN3Vod2dxdXJ6NHl3djVnenhxaDh6aTV6Z2pwOW52cXU3eDUxcHZjc3lxc3BhNWtkeTY0Z3hndmwwYTN4bDVjZ3IyNDJ2a3VzbXduZHo4b3Rua2poZjI3aTlrdGFiMG5rdnp0eTBwYXNyNmo3Y2FlMWY0bWdicmwwNXJ4a2FjbTYwMHN4eWgzOTl2enBkMTc1cWdzdjBkMXM3cHJ0djc2OHRoa2V1Y3hxdnJvcGViZjYzMGdjZzg2OGFsMTJmazZseHdrOHB0cndkbm95aHJnMXM5ZDlyazRrZW9iY3A4a3pwMzUyZXc2eTF4Z2ttZmliemxlZm0wMWlydG8ydTB1M2xkY2sxd3FveTB1emtxdzA4MGZuZmVqMmUzNzg2d3BjYmVsYTNvZjZlaHp4Y2g4MGl3aGVwNDJjejhpamZzeDR6ZHBwa2p6NHhwN3dmenU0cjNkNWlucjN0MW9xOWJjNnYxdjBqMmhueHFiMzAwOXQ1MHowbGtrdjA5Y3duMzFvdHg0NWMxcG90OTYwdWRkZTQ1M2M2cTA5YWkwcGtzbHVnb2N3OXR4MXR6Z3VoMnZhZjM5cmRtZGo4bndoYjJkMnM1anlwdjc0eWZrdDJoNTU0NnRkYnBydzN5MnF4Mmd1enBzd3IxeWw1ZHRpaHo1amlsdzBvaTd0NDc2cWhranB3dTR1ZnR5NnYyc29mYmZ2M3d4ZmpnODZjaXZjNmdxNGUwYWFvc3BvcXAyd2g4cGRoaXFmZGR0bWZnMmd0Z3RyNDhicGdwbjF0ZzFzeDlmYmFuM3VrZW1nczJjY2wwcnNqOTFqdDkyY2s5MGdxMm1sbnV2c3JyOXhiZHlieXM4azcyZGdranF4d3B3czJtazZ6OTJxMXNjY3N2d2NmbHFxanU0MTJndGg0OWZidjd0b21obTg2ZzR0YWJkdGpiOWYwYWV2dGgwenRkY3ByNWZlbjd1ODhydzYycmRsc25mNTY5Nm0yYzdsdjR2XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAidGVzdG1lQGlkb25vdGV4aXN0cHJvamVjdC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICI5NzM1NzE1MzIyNDUwNjY5MzM3OCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvdGVzdG1lJTQwaWRvbm90ZXhpc3Rwcm9qZWN0LmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9
`

// credentials_json_encoded value is invalid base64 encoded dummy data
var testBQInputYAMLWithInvalidBase64EncCredsJSON = `
project: job-project
table: bigquery-public-data.samples.shakespeare
columns:
  - word
  - sum(word_count) as total_count
where: length(word) >= ?
suffix: |
  GROUP BY word
  ORDER BY total_count DESC
  LIMIT 10
args_mapping: |
  root = [ 3 ]
credentials_json_encoded: ewogICJ0eXBlIjo
`

func TestGCPBigQuerySelectInput(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	inp.client = mockClient

	iter := &mockBQIterator{
		rows: []string{
			`{"total_count":25568,"word":"the"}`,
			`{"total_count":19649,"word":"and"}`,
			`{"total_count":12527,"word":"you"}`,
			`{"total_count":8561,"word":"that"}`,
			`{"total_count":8395,"word":"not"}`,
			`{"total_count":7780,"word":"And"}`,
			`{"total_count":7224,"word":"with"}`,
			`{"total_count":6811,"word":"his"}`,
			`{"total_count":6244,"word":"your"}`,
			`{"total_count":6154,"word":"for"}`,
		},
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	err = inp.Connect(context.Background())
	require.NoError(t, err)

	i := 0
	for {
		msg, ack, err := inp.Read(context.Background())
		if i >= len(iter.rows) {
			require.ErrorIs(t, err, service.ErrEndOfInput)
			break
		}

		require.NoError(t, err)
		require.NoError(t, ack(context.Background(), nil))

		bs, err := msg.AsBytes()
		require.NoError(t, err)

		require.Equal(t, iter.rows[i], string(bs))

		i++
	}

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectInput_NotConnected(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	msg, ack, err := inp.Read(context.Background())
	require.ErrorIs(t, err, service.ErrNotConnected)
	require.Nil(t, msg)
	require.Nil(t, ack)
}

func TestGCPBigQuerySelectInput_IteratorError(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	inp.client = mockClient

	testErr := errors.New("simulated error")
	iter := &mockBQIterator{
		rows: []string{`{"total_count":25568,"word":"the"}`},
		err:  testErr,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	err = inp.Connect(context.Background())
	require.NoError(t, err)

	msg, ack, err := inp.Read(context.Background())
	require.ErrorIs(t, err, testErr)
	require.Nil(t, msg)
	require.Nil(t, ack)
}

func TestGCPBigQuerySelectInput_Connect(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(&mockBQIterator{}, nil)
	inp.client = mockClient

	err = inp.Connect(context.Background())
	require.NoError(t, err)

	err = inp.Close(context.Background())
	require.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectInput_ConnectError(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	testErr := errors.New("test error")
	mockClient := &mockBQClient{}
	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(nil, testErr)
	inp.client = mockClient

	err = inp.Connect(context.Background())
	require.ErrorIs(t, err, testErr)

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectInputClientOptions(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAMLWithCredsJSON, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	var opt []option.ClientOption
	opt, err = getClientOptionWithCredential(inp.config.credentialsJSON, opt)
	require.NoError(t, err)
	require.Lenf(t, opt, 1, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	expectedValue, _ := base64.StdEncoding.DecodeString(inp.config.credentialsJSON)
	require.EqualValues(t, option.WithCredentialsJSON(expectedValue), actualCredsJSON, "GCP Credentials Json not set as expected.")
}

func TestGCPBigQuerySelectInputClientOptions_Error(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAMLWithInvalidBase64EncCredsJSON, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	var opt []option.ClientOption
	opt, err = getClientOptionWithCredential(inp.config.credentialsJSON, opt)
	require.ErrorContains(t, err, "illegal base64 data")
	require.Lenf(t, opt, 0, "Unexpected number of Client Options")
}
