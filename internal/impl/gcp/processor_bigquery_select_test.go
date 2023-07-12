package gcp

import (
	"context"
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
credentials_json: |
  {
  "type": "service_account",
  "project_id": "idonotexistproject",
  "private_key_id": "nx803io2ut5gxah3px6tfy90wym9flc22o4thnty",
  "private_key": "-----BEGIN PRIVATE KEY-----\!!notarealprivatekey!!!nzspswasv6bta7annd2a7ihhm4lkab5bduj5ifbgfu8xrzwa6mdp80q42ga0fr5imc9h0q12mfj06kbu04c2wi6gyqw6leg5a4rfnhyiqvcg36hlu0zqxsqefia6q9sq0228dmg6mvplnzsp9m2pjuoy7ntqtdrxhsmywr8exs7xrtju1eya3rkeuw4okd4b24ioiuvp7pri88h2oyhs30nh1zl0qglyla37llc2yzlv8852de2uwyc93m0qii7uhwgqurz4ywv5gzxqh8zi5zgjp9nvqu7x51pvcsyqspa5kdy64gxgvl0a3xl5cgr242vkusmwndz8otnkjhf27i9ktab0nkvzty0pasr6j7cae1f4mgbrl05rxkacm600sxyh399vzpd175qgsv0d1s7prtv768thkeucxqvropebf630gcg868al12fk6lxwk8ptrwdnoyhrg1s9d9rk4keobcp8kzp352ew6y1xgkmfibzlefm01irto2u0u3ldck1wqoy0uzkqw080fnfej2e3786wpcbela3of6ehzxch80iwhep42cz8ijfsx4zdppkjz4xp7wfzu4r3d5inr3t1oq9bc6v1v0j2hnxqb3009t50z0lkkv09cwn31otx45c1pot960udde453c6q09ai0pkslugocw9tx1tzguh2vaf39rdmdj8nwhb2d2s5jypv74yfkt2h5546tdbprw3y2qx2guzpswr1yl5dtihz5jilw0oi7t476qhkjpwu4ufty6v2sofbfv3wxfjg86civc6gq4e0aaospoqp2wh8pdhiqfddtmfg2gtgtr48bpgpn1tg1sx9fban3ukemgs2ccl0rsj91jt92ck90gq2mlnuvsrr9xbdybys8k72dgkjqxwpws2mk6z92q1sccsvwcflqqju412gth49fbv7tomhm86g4tabdtjb9f0aevth0ztdcpr5fen7u88rw62rdlsnf5696m2c7lv4v\n-----END PRIVATE KEY-----\n",
  "client_email": "testme@idonotexistproject.iam.gserviceaccount.com",
  "client_id": "97357153224506693378",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40idonotexistproject.iam.gserviceaccount.com"
  }
`

// credentials_json value is invalid base64 encoded dummy data
//var testBQProcessorYAMLWithCredsJSONError = `
//project: job-project
//table: bigquery-public-data.samples.shakespeare
//columns:
//  - word
//  - sum(word_count) as total_count
//where: length(word) >= ?
//suffix: |
//  GROUP BY word
//  ORDER BY total_count DESC
//  LIMIT 10
//args_mapping: |
//  root = [ this.term ]
//credentials_json: ewogICJ0eXBlIj
//`

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
	expectedValue := option.WithCredentialsJSON([]byte(cleanCredsJson(conf.credentialsJSON)))
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials Json not set as expected.")
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
	expectedValue := option.WithCredentialsJSON([]byte(cleanCredsJson(conf.credentialsJSON)))
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials Json not set as expected.")
}

//func TestGCPBigQuerySelectProcessorWithCredsJSONError(t *testing.T) {
//	spec := newBigQuerySelectProcessorConfig()
//
//	parsed, err := spec.ParseYAML(testBQProcessorYAMLWithCredsJSONError, nil)
//	require.NoError(t, err)
//
//	conf, err := bigQuerySelectProcessorConfigFromParsed(parsed)
//	require.NoError(t, err)
//
//	b := &bigQueryProcessorOptions{}
//	err = getClientOptionsProcessorBQSelect(conf, b)
//
//	require.ErrorContains(t, err, "illegal base64 data")
//	require.Lenf(t, b.clientOptions, 0, "Unexpected number of Client Options")
//}
