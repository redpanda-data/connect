package gcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/public/service"
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

// credentials_json value is base64 encoded valid json with dummy data
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
//var testBQInputYAMLWithInvalidJSON = `
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
//  root = [ 3 ]
//credentials_json: |
//  {
//  "type": "service_account",
//  "project_id": "idonotexistproject",
//  "private_key_id": "nx803io2ut5gxah3px6tfy90wym9flc22o4thnty",
//  "private_key": "-----BEGIN PRIVATE KEY-----\!!notarealprivatekey!!!nzspswasv6bta7annd2a7ihhm4lkab5bduj5ifbgfu8xrzwa6mdp80q42ga0fr5imc9h0q12mfj06kbu04c2wi6gyqw6leg5a4rfnhyiqvcg36hlu0zqxsqefia6q9sq0228dmg6mvplnzsp9m2pjuoy7ntqtdrxhsmywr8exs7xrtju1eya3rkeuw4okd4b24ioiuvp7pri88h2oyhs30nh1zl0qglyla37llc2yzlv8852de2uwyc93m0qii7uhwgqurz4ywv5gzxqh8zi5zgjp9nvqu7x51pvcsyqspa5kdy64gxgvl0a3xl5cgr242vkusmwndz8otnkjhf27i9ktab0nkvzty0pasr6j7cae1f4mgbrl05rxkacm600sxyh399vzpd175qgsv0d1s7prtv768thkeucxqvropebf630gcg868al12fk6lxwk8ptrwdnoyhrg1s9d9rk4keobcp8kzp352ew6y1xgkmfibzlefm01irto2u0u3ldck1wqoy0uzkqw080fnfej2e3786wpcbela3of6ehzxch80iwhep42cz8ijfsx4zdppkjz4xp7wfzu4r3d5inr3t1oq9bc6v1v0j2hnxqb3009t50z0lkkv09cwn31otx45c1pot960udde453c6q09ai0pkslugocw9tx1tzguh2vaf39rdmdj8nwhb2d2s5jypv74yfkt2h5546tdbprw3y2qx2guzpswr1yl5dtihz5jilw0oi7t476qhkjpwu4ufty6v2sofbfv3wxfjg86civc6gq4e0aaospoqp2wh8pdhiqfddtmfg2gtgtr48bpgpn1tg1sx9fban3ukemgs2ccl0rsj91jt92ck90gq2mlnuvsrr9xbdybys8k72dgkjqxwpws2mk6z92q1sccsvwcflqqju412gth49fbv7tomhm86g4tabdtjb9f0aevth0ztdcpr5fen7u88rw62rdlsnf5696m2c7lv4v\n-----END PRIVATE KEY-----\n",
//  "client_email": "testme@idonotexistproject.iam.gserviceaccount.com",
//  "client_id": "97357153224506693378",
//  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
//  "token_uri": "https://oauth2.googleapis.com/token",
//`

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

	opt, err := getClientOptionsBQSelect(inp)
	require.NoError(t, err)
	require.Lenf(t, opt, 1, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	expectedValue := option.WithCredentialsJSON([]byte(cleanCredsJson(inp.config.credentialsJSON)))
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials Json not set as expected.")
}

//func TestGCPBigQuerySelectInputClientOptions_Error(t *testing.T) {
//	spec := newBigQuerySelectInputConfig()
//
//	parsed, err := spec.ParseYAML(testBQInputYAMLWithInvalidJSON, nil)
//	require.NoError(t, err)
//
//	inp, err := newBigQuerySelectInput(parsed, nil)
//	require.NoError(t, err)
//
//	opt, err := getClientOptionsBQSelect(inp)
//	require.ErrorContains(t, err, "illegal base64 data")
//	require.Lenf(t, opt, 0, "Unexpected number of Client Options")
//}
