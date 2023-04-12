package gcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/public/service"
)

var testBQProcessorYAML = `
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
  root = [ this.term ]
`

// credentials_json value is base64 encoded valid json with dummy data
var testBQProcessorYAMLWithCredsJSON = `
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
  root = [ this.term ]
credentials_json: ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiaWRvbm90ZXhpc3Rwcm9qZWN0IiwKICAicHJpdmF0ZV9rZXlfaWQiOiAibng4MDNpbzJ1dDVneGFoM3B4NnRmeTkwd3ltOWZsYzIybzR0aG50eSIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVwhIW5vdGFyZWFscHJpdmF0ZWtleSEhIW56c3Bzd2FzdjZidGE3YW5uZDJhN2loaG00bGthYjViZHVqNWlmYmdmdTh4cnp3YTZtZHA4MHE0MmdhMGZyNWltYzloMHExMm1majA2a2J1MDRjMndpNmd5cXc2bGVnNWE0cmZuaHlpcXZjZzM2aGx1MHpxeHNxZWZpYTZxOXNxMDIyOGRtZzZtdnBsbnpzcDltMnBqdW95N250cXRkcnhoc215d3I4ZXhzN3hydGp1MWV5YTNya2V1dzRva2Q0YjI0aW9pdXZwN3ByaTg4aDJveWhzMzBuaDF6bDBxZ2x5bGEzN2xsYzJ5emx2ODg1MmRlMnV3eWM5M20wcWlpN3Vod2dxdXJ6NHl3djVnenhxaDh6aTV6Z2pwOW52cXU3eDUxcHZjc3lxc3BhNWtkeTY0Z3hndmwwYTN4bDVjZ3IyNDJ2a3VzbXduZHo4b3Rua2poZjI3aTlrdGFiMG5rdnp0eTBwYXNyNmo3Y2FlMWY0bWdicmwwNXJ4a2FjbTYwMHN4eWgzOTl2enBkMTc1cWdzdjBkMXM3cHJ0djc2OHRoa2V1Y3hxdnJvcGViZjYzMGdjZzg2OGFsMTJmazZseHdrOHB0cndkbm95aHJnMXM5ZDlyazRrZW9iY3A4a3pwMzUyZXc2eTF4Z2ttZmliemxlZm0wMWlydG8ydTB1M2xkY2sxd3FveTB1emtxdzA4MGZuZmVqMmUzNzg2d3BjYmVsYTNvZjZlaHp4Y2g4MGl3aGVwNDJjejhpamZzeDR6ZHBwa2p6NHhwN3dmenU0cjNkNWlucjN0MW9xOWJjNnYxdjBqMmhueHFiMzAwOXQ1MHowbGtrdjA5Y3duMzFvdHg0NWMxcG90OTYwdWRkZTQ1M2M2cTA5YWkwcGtzbHVnb2N3OXR4MXR6Z3VoMnZhZjM5cmRtZGo4bndoYjJkMnM1anlwdjc0eWZrdDJoNTU0NnRkYnBydzN5MnF4Mmd1enBzd3IxeWw1ZHRpaHo1amlsdzBvaTd0NDc2cWhranB3dTR1ZnR5NnYyc29mYmZ2M3d4ZmpnODZjaXZjNmdxNGUwYWFvc3BvcXAyd2g4cGRoaXFmZGR0bWZnMmd0Z3RyNDhicGdwbjF0ZzFzeDlmYmFuM3VrZW1nczJjY2wwcnNqOTFqdDkyY2s5MGdxMm1sbnV2c3JyOXhiZHlieXM4azcyZGdranF4d3B3czJtazZ6OTJxMXNjY3N2d2NmbHFxanU0MTJndGg0OWZidjd0b21obTg2ZzR0YWJkdGpiOWYwYWV2dGgwenRkY3ByNWZlbjd1ODhydzYycmRsc25mNTY5Nm0yYzdsdjR2XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAidGVzdG1lQGlkb25vdGV4aXN0cHJvamVjdC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICI5NzM1NzE1MzIyNDUwNjY5MzM3OCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvdGVzdG1lJTQwaWRvbm90ZXhpc3Rwcm9qZWN0LmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9
`

// credentials_json value is invalid base64 encoded dummy data
var testBQProcessorYAMLWithCredsJSONError = `
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
  root = [ this.term ]
credentials_json: ewogICJ0eXBlIj
`

func TestGCPBigQuerySelectProcessor(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAML, nil)
	require.NoError(t, err)

	proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{option.WithoutAuthentication()},
	})
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	proc.client = mockClient

	expected := []map[string]any{
		{"total_count": 25568, "word": "the"},
		{"total_count": 19649, "word": "and"},
	}

	expectedMsg, err := json.Marshal(expected)
	require.NoError(t, err)

	var rows []string
	for _, v := range expected {
		row, err := json.Marshal(v)
		require.NoError(t, err)

		rows = append(rows, string(row))
	}

	iter := &mockBQIterator{
		rows: rows,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	inbatch := service.MessageBatch{
		service.NewMessage([]byte(`{"term": "test1"}`)),
		service.NewMessage([]byte(`{"term": "test2"}`)),
	}

	batches, err := proc.ProcessBatch(context.Background(), inbatch)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	// Assert that we generated the right parameters for each BQ query
	mockClient.AssertNumberOfCalls(t, "RunQuery", 2)
	call1 := mockClient.Calls[0]
	args1 := call1.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args1, []string{"test1"})
	call2 := mockClient.Calls[1]
	args2 := call2.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args2, []string{"test2"})

	outbatch := batches[0]
	require.Len(t, outbatch, 2)

	msg1, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(expectedMsg), string(msg1))

	msg2, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(expectedMsg), string(msg2))

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectProcessor_IteratorError(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAML, nil)
	require.NoError(t, err)

	proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{option.WithoutAuthentication()},
	})
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	proc.client = mockClient

	testErr := errors.New("simulated err")
	iter := &mockBQIterator{
		rows:   []string{`{"total_count": 25568, "word": "the"}`},
		err:    testErr,
		errIdx: 1,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	inmsg := []byte(`{"term": "test1"}`)
	inbatch := service.MessageBatch{
		service.NewMessage(inmsg),
	}

	batches, err := proc.ProcessBatch(context.Background(), inbatch)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	// Assert that we generated the right parameters for each BQ query
	mockClient.AssertNumberOfCalls(t, "RunQuery", 1)
	call1 := mockClient.Calls[0]
	args1 := call1.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args1, []string{"test1"})

	outbatch := batches[0]
	require.Len(t, outbatch, 1)

	msg1, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(inmsg), string(msg1))

	msgErr := outbatch[0].GetError()
	require.Contains(t, msgErr.Error(), testErr.Error())

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectProcessorWithCredsJSON1(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAMLWithCredsJSON, nil)
	require.NoError(t, err)

	conf, err := bigQuerySelectProcessorConfigFromParsed(parsed)
	require.NoError(t, err)

	b := &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{option.WithoutAuthentication()},
	}
	err = getClientOptionsProcessorBQSelect(conf, b)

	require.NoError(t, err)
	require.Lenf(t, b.clientOptions, 2, "Unexpected number of Client Options")

	actualCredsJSON := b.clientOptions[1]
	expectedValue, _ := base64.StdEncoding.DecodeString(conf.credentialsJSON)
	require.EqualValues(t, option.WithCredentialsJSON(expectedValue), actualCredsJSON, "GCP Credentials Json not set as expected.")
}

func TestGCPBigQuerySelectProcessorWithOnlyCredsJSONOption(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAMLWithCredsJSON, nil)
	require.NoError(t, err)

	conf, err := bigQuerySelectProcessorConfigFromParsed(parsed)
	require.NoError(t, err)

	b := &bigQueryProcessorOptions{}
	err = getClientOptionsProcessorBQSelect(conf, b)

	require.NoError(t, err)
	require.Lenf(t, b.clientOptions, 1, "Unexpected number of Client Options")

	actualCredsJSON := b.clientOptions[0]
	expectedValue, _ := base64.StdEncoding.DecodeString(conf.credentialsJSON)
	require.EqualValues(t, option.WithCredentialsJSON(expectedValue), actualCredsJSON, "GCP Credentials Json not set as expected.")
}

func TestGCPBigQuerySelectProcessorWithCredsJSONError(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAMLWithCredsJSONError, nil)
	require.NoError(t, err)

	conf, err := bigQuerySelectProcessorConfigFromParsed(parsed)
	require.NoError(t, err)

	b := &bigQueryProcessorOptions{}
	err = getClientOptionsProcessorBQSelect(conf, b)

	require.ErrorContains(t, err, "illegal base64 data")
	require.Lenf(t, b.clientOptions, 0, "Unexpected number of Client Options")
}
