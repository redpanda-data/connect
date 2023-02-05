package snowflake

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	dummyUUID = "12345678-90ab-cdef-1234-567890abcdef"
)

type MockDB struct {
	Queries      []string
	QueriesCount int
}

func (db *MockDB) ExecContext(ctx context.Context, query string, _ ...any) (sql.Result, error) {
	db.Queries = append(db.Queries, query)
	db.QueriesCount++

	return nil, nil
}

func (db *MockDB) Close() error { return nil }

func (db *MockDB) hasQuery(query string) bool {
	for _, q := range db.Queries {
		if q == query {
			return true
		}
	}

	return false
}

type MockUUIDGenerator struct{}

func (MockUUIDGenerator) NewV4() (uuid.UUID, error) {
	return uuid.Must(uuid.FromString(dummyUUID)), nil
}

type MockHTTPClient struct {
	SnowpipeHost string
	Queries      []string
	QueriesCount int
	Payloads     []string
	JWTs         []string
}

func (c *MockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.URL.Host = c.SnowpipeHost
	req.URL.Scheme = "http"

	query := req.URL.Path
	query += "?" + req.URL.RawQuery
	c.Queries = append(c.Queries, query)
	c.QueriesCount++

	// Read request body and recreate it
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	req.Body.Close()
	req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	c.Payloads = append(c.Payloads, strings.TrimSpace(string(bodyBytes)))

	c.JWTs = append(c.JWTs, req.Header.Get("Authorization"))

	return http.DefaultClient.Do(req)
}

func (c *MockHTTPClient) hasQuery(query string) bool {
	for _, q := range c.Queries {
		if q == query {
			return true
		}
	}

	return false
}

func (c *MockHTTPClient) hasPayload(payload string) bool {
	for _, p := range c.Payloads {
		if p == payload {
			return true
		}
	}

	return false
}

func TestSnowflakeOutput(t *testing.T) {
	type testCase struct {
		name                      string
		privateKeyPath            string
		privateKeyPassphrase      string
		stage                     string
		fileName                  string
		fileExtension             string
		requestID                 string
		snowpipe                  string
		compression               string
		snowflakeHTTPResponseCode int
		snowflakeResponseCode     string
		wantPUTQuery              string
		wantPUTQueriesCount       int
		wantSnowpipeQuery         string
		wantSnowpipeQueriesCount  int
		wantSnowpipePayload       string
		wantSnowpipeJWT           string
		errConfigContains         string
		errContains               string
	}
	getSnowflakeWriter := func(t *testing.T, tc testCase) (*snowflakeWriter, error) {
		t.Helper()

		outputConfig := `
account: benthos
region: east-us-2
cloud: azure
user: foobar
private_key_file: ` + tc.privateKeyPath + `
private_key_pass: ` + tc.privateKeyPassphrase + `
role: test_role
database: test_db
warehouse: test_warehouse
schema: test_schema
path: foo/bar/baz
stage: '` + tc.stage + `'
file_name: '` + tc.fileName + `'
file_extension: '` + tc.fileExtension + `'
upload_parallel_threads: 42
compression: ` + tc.compression + `
request_id: '` + tc.requestID + `'
snowpipe: '` + tc.snowpipe + `'
`

		spec := snowflakePutOutputConfig()
		env := service.NewEnvironment()
		conf, err := spec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		return newSnowflakeWriterFromConfig(conf, service.MockResources())
	}

	tests := []testCase{
		{
			name:           "executes snowflake query with plaintext SSH key",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			compression:    "NONE",
			wantPUTQuery:   "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:                 "executes snowflake query with encrypted SSH key",
			privateKeyPath:       "resources/ssh_keys/snowflake_rsa_key.p8",
			privateKeyPassphrase: "test123",
			stage:                "@test_stage",
			compression:          "NONE",
			wantPUTQuery:         "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:              "fails to read missing SSH key",
			privateKeyPath:    "resources/ssh_keys/missing_key.pem",
			stage:             "@test_stage",
			compression:       "NONE",
			errConfigContains: "failed to read private key resources/ssh_keys/missing_key.pem: open resources/ssh_keys/missing_key.pem: no such file or directory",
		},
		{
			name:              "fails to read encrypted SSH key without passphrase",
			privateKeyPath:    "resources/ssh_keys/snowflake_rsa_key.p8",
			stage:             "@test_stage",
			compression:       "NONE",
			errConfigContains: "failed to read private key: private key requires a passphrase, but private_key_passphrase was not supplied",
		},
		{
			name:           "executes snowflake query without compression",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			compression:    "NONE",
			wantPUTQuery:   "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:           "executes snowflake query with automatic compression",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			compression:    "AUTO",
			wantPUTQuery:   "PUT file://foo/bar/baz/" + dummyUUID + ".gz @test_stage/foo/bar/baz AUTO_COMPRESS = TRUE SOURCE_COMPRESSION = AUTO_DETECT PARALLEL=42",
		},
		{
			name:           "executes snowflake query with gzip compression",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			compression:    "GZIP",
			wantPUTQuery:   "PUT file://foo/bar/baz/" + dummyUUID + ".gz @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = GZIP PARALLEL=42",
		},
		{
			name:           "executes snowflake query with DEFLATE compression",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			compression:    "DEFLATE",
			wantPUTQuery:   "PUT file://foo/bar/baz/" + dummyUUID + ".deflate @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = DEFLATE PARALLEL=42",
		},
		{
			name:           "executes snowflake query with RAW_DEFLATE compression",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			compression:    "RAW_DEFLATE",
			wantPUTQuery:   "PUT file://foo/bar/baz/" + dummyUUID + ".raw_deflate @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = RAW_DEFLATE PARALLEL=42",
		},
		{
			name:           "handles file name and file extension interpolation",
			privateKeyPath: "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:          "@test_stage",
			fileName:       `${! "deadbeef" }`,
			fileExtension:  `${! "parquet" }`,
			compression:    "NONE",
			wantPUTQuery:   "PUT file://foo/bar/baz/deadbeef.parquet @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:                      "executes snowflake query and calls Snowpipe",
			privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:                     "@test_stage",
			snowpipe:                  "test_pipe",
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "SUCCESS",
			wantPUTQuery:              "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			wantPUTQueriesCount:       1,
			wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe/insertFiles?requestId=" + dummyUUID,
			wantSnowpipeQueriesCount:  1,
			wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/` + dummyUUID + `.json"}]}`,
			wantSnowpipeJWT:           "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi02MjEzNTU5Njc0MCwiaWF0IjotNjIxMzU1OTY4MDAsImlzcyI6IkJFTlRIT1MuRk9PQkFSLlNIQTI1Njprc3dSSG9uZmU0QllXQWtReUlBUDVzY2w5OUxRQ0U2S1Irc0J4VEVoenBFPSIsInN1YiI6IkJFTlRIT1MuRk9PQkFSIn0.ABldbfDem53G-EDMoQaY7VVA2RXPryvXFcY0Hqogu_-qjT3qcJEY1aM1B9SqATkeFDNiagOXPl218dUc-Hes4WTbWnoXq8EUlMLjbg3_9qrlp6p-6SzUbX88lpkuYPXD3UiDBhLXsQso5ciufev2IFX5oCt-Oxg9GbI4uIveey_k8dv3S2a942RQbB6ffCj3Stca31oz2F_IPaF2xDmwVsBig_C9NoHToQFVAfVbPIV1hMDIc7zutuLqXQWZPfT6K0PPc15ZMutQQ0tEYCboDanx3tXe9ub_gLfyGaHwuDUXBk3EN3UkZ8rmgasCk_VnFZ_Xk6tnaZfdIrGKRZ5dsA",
		},
		{
			name:                      "gets error code from Snowpipe",
			privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:                     "@test_stage",
			snowpipe:                  "test_pipe",
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "FAILURE",
			errContains:               "received unexpected Snowpipe response code: FAILURE",
		},
		{
			name:                      "gets http error from Snowpipe",
			privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:                     "@test_stage",
			snowpipe:                  "test_pipe",
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusTeapot,
			errContains:               "received unexpected Snowpipe response status: 418",
		},
		{
			name:                "handles stage interpolation and runs a query for each sub-batch",
			privateKeyPath:      "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:               `@test_stage_${! json("id") }`,
			compression:         "NONE",
			wantPUTQueriesCount: 2,
			wantPUTQuery:        "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage_bar/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
		},
		{
			name:                      "handles Snowpipe interpolation and runs a query for each sub-batch",
			privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:                     "@test_stage",
			snowpipe:                  `test_pipe_${! json("id") }`,
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "SUCCESS",
			wantPUTQuery:              "PUT file://foo/bar/baz/" + dummyUUID + ".json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			wantPUTQueriesCount:       2,
			wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe_bar/insertFiles?requestId=" + dummyUUID,
			wantSnowpipeQueriesCount:  2,
			wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/` + dummyUUID + `.json"}]}`,
			wantSnowpipeJWT:           "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi02MjEzNTU5Njc0MCwiaWF0IjotNjIxMzU1OTY4MDAsImlzcyI6IkJFTlRIT1MuRk9PQkFSLlNIQTI1Njprc3dSSG9uZmU0QllXQWtReUlBUDVzY2w5OUxRQ0U2S1Irc0J4VEVoenBFPSIsInN1YiI6IkJFTlRIT1MuRk9PQkFSIn0.ABldbfDem53G-EDMoQaY7VVA2RXPryvXFcY0Hqogu_-qjT3qcJEY1aM1B9SqATkeFDNiagOXPl218dUc-Hes4WTbWnoXq8EUlMLjbg3_9qrlp6p-6SzUbX88lpkuYPXD3UiDBhLXsQso5ciufev2IFX5oCt-Oxg9GbI4uIveey_k8dv3S2a942RQbB6ffCj3Stca31oz2F_IPaF2xDmwVsBig_C9NoHToQFVAfVbPIV1hMDIc7zutuLqXQWZPfT6K0PPc15ZMutQQ0tEYCboDanx3tXe9ub_gLfyGaHwuDUXBk3EN3UkZ8rmgasCk_VnFZ_Xk6tnaZfdIrGKRZ5dsA",
		},
		{
			name:                      "handles request_id interpolation and runs a query and makes a single Snowpipe call for the entire batch",
			privateKeyPath:            "resources/ssh_keys/snowflake_rsa_key.pem",
			stage:                     `@test_stage`,
			snowpipe:                  `test_pipe`,
			requestID:                 `${! "deadbeef" }`,
			compression:               "NONE",
			snowflakeHTTPResponseCode: http.StatusOK,
			snowflakeResponseCode:     "SUCCESS",
			wantPUTQuery:              "PUT file://foo/bar/baz/deadbeef.json @test_stage/foo/bar/baz AUTO_COMPRESS = FALSE SOURCE_COMPRESSION = NONE PARALLEL=42",
			wantPUTQueriesCount:       1,
			wantSnowpipeQuery:         "/v1/data/pipes/test_db.test_schema.test_pipe/insertFiles?requestId=deadbeef",
			wantSnowpipeQueriesCount:  1,
			wantSnowpipePayload:       `{"files":[{"path":"foo/bar/baz/deadbeef.json"}]}`,
			wantSnowpipeJWT:           "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOi02MjEzNTU5Njc0MCwiaWF0IjotNjIxMzU1OTY4MDAsImlzcyI6IkJFTlRIT1MuRk9PQkFSLlNIQTI1Njprc3dSSG9uZmU0QllXQWtReUlBUDVzY2w5OUxRQ0U2S1Irc0J4VEVoenBFPSIsInN1YiI6IkJFTlRIT1MuRk9PQkFSIn0.ABldbfDem53G-EDMoQaY7VVA2RXPryvXFcY0Hqogu_-qjT3qcJEY1aM1B9SqATkeFDNiagOXPl218dUc-Hes4WTbWnoXq8EUlMLjbg3_9qrlp6p-6SzUbX88lpkuYPXD3UiDBhLXsQso5ciufev2IFX5oCt-Oxg9GbI4uIveey_k8dv3S2a942RQbB6ffCj3Stca31oz2F_IPaF2xDmwVsBig_C9NoHToQFVAfVbPIV1hMDIc7zutuLqXQWZPfT6K0PPc15ZMutQQ0tEYCboDanx3tXe9ub_gLfyGaHwuDUXBk3EN3UkZ8rmgasCk_VnFZ_Xk6tnaZfdIrGKRZ5dsA",
		},
		// TODO:
		// - Snowflake PUT query payload tests
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := getSnowflakeWriter(t, test)
			if test.errConfigContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errConfigContains)
				return
			}

			s.uuidGenerator = MockUUIDGenerator{}

			snowpipeTestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.snowflakeHTTPResponseCode)
				_, _ = w.Write([]byte(`{"ResponseCode": "` + test.snowflakeResponseCode + `"}`))
			}))
			t.Cleanup(snowpipeTestServer.Close)

			mockHTTPClient := MockHTTPClient{
				SnowpipeHost: snowpipeTestServer.Listener.Addr().String(),
			}
			s.httpClient = &mockHTTPClient

			mockDB := MockDB{}
			s.db = &mockDB

			s.nowFn = func() time.Time { return time.Time{} }

			err = s.WriteBatch(context.Background(), service.MessageBatch{
				service.NewMessage([]byte(`{"id":"foo","content":"foo stuff"}`)),
				service.NewMessage([]byte(`{"id":"bar","content":"bar stuff"}`)),
			})
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.errContains)
				return
			}

			if test.wantPUTQueriesCount > 0 {
				assert.Equal(t, test.wantPUTQueriesCount, mockDB.QueriesCount)
			}
			if test.wantPUTQuery != "" {
				assert.True(t, mockDB.hasQuery(test.wantPUTQuery))
			}
			if test.wantSnowpipeQueriesCount > 0 {
				assert.Equal(t, test.wantSnowpipeQueriesCount, mockHTTPClient.QueriesCount)
				assert.Len(t, mockHTTPClient.JWTs, test.wantSnowpipeQueriesCount)
				for _, jwt := range mockHTTPClient.JWTs {
					assert.Equal(t, test.wantSnowpipeJWT, jwt)
				}
			}
			if test.wantSnowpipeQuery != "" {
				assert.True(t, mockHTTPClient.hasQuery(test.wantSnowpipeQuery))
			}
			if test.wantSnowpipePayload != "" {
				assert.True(t, mockHTTPClient.hasPayload(test.wantSnowpipePayload))
			}
		})
	}
}
