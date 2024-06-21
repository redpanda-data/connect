package snowflake

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang-jwt/jwt/v4"
	"github.com/snowflakedb/gosnowflake"
	"github.com/youmark/pkcs8"
	"golang.org/x/crypto/ssh"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	defaultJWTTimeout = 60 * time.Second
)

// CompressionType represents the compression used for the payloads sent to Snowflake.
type CompressionType string

const (
	// CompressionTypeNone No compression.
	CompressionTypeNone CompressionType = "NONE"
	// CompressionTypeAuto Automatic compression (gzip).
	CompressionTypeAuto CompressionType = "AUTO"
	// CompressionTypeGzip Gzip compression.
	CompressionTypeGzip CompressionType = "GZIP"
	// CompressionTypeDeflate Deflate compression using zlib algorithm (with zlib header, RFC1950).
	CompressionTypeDeflate CompressionType = "DEFLATE"
	// CompressionTypeRawDeflate Deflate compression using flate algorithm (without header, RFC1951).
	CompressionTypeRawDeflate CompressionType = "RAW_DEFLATE"
	// CompressionTypeZstandard compression using Zstandard algorithm.
	CompressionTypeZstandard CompressionType = "ZSTD"
)

func snowflakePutOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.0.0").
		Summary("Sends messages to Snowflake stages and, optionally, calls Snowpipe to load this data into one or more tables.").
		Description(output.Description(true, true, `
In order to use a different stage and / or Snowpipe for each message, you can use function interpolations as described
[here](/docs/configuration/interpolation#bloblang-queries). When using batching, messages are grouped by the calculated
stage and Snowpipe and are streamed to individual files in their corresponding stage and, optionally, a Snowpipe
`+"`insertFiles`"+` REST API call will be made for each individual file.

### Credentials

Two authentication mechanisms are supported:
- User/password
- Key Pair Authentication

#### User/password

This is a basic authentication mechanism which allows you to PUT data into a stage. However, it is not compatible with
Snowpipe.

#### Key Pair Authentication

This authentication mechanism allows Snowpipe functionality, but it does require configuring an SSH Private Key
beforehand. Please consult the [documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth.html#configuring-key-pair-authentication)
for details on how to set it up and assign the Public Key to your user.

Note that the Snowflake documentation [used to suggest](https://twitter.com/felipehoffa/status/1560811785606684672)
using this command:

`+"```shell"+`
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
`+"```"+`

to generate an encrypted SSH private key. However, in this case, it uses an encryption algorithm called
`+"`pbeWithMD5AndDES-CBC`"+`, which is part of the PKCS#5 v1.5 and is considered insecure. Due to this, Benthos does not
support it and, if you wish to use password-protected keys directly, you must use PKCS#5 v2.0 to encrypt them by using
the following command (as the current Snowflake docs suggest):

`+"```shell"+`
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -out rsa_key.p8
`+"```"+`

If you have an existing key encrypted with PKCS#5 v1.5, you can re-encrypt it with PKCS#5 v2.0 using this command:

`+"```shell"+`
openssl pkcs8 -in rsa_key_original.p8 -topk8 -v2 des3 -out rsa_key.p8
`+"```"+`

Please consult [this](https://linux.die.net/man/1/pkcs8) pkcs8 command documentation for details on PKCS#5 algorithms.

### Batching

It's common to want to upload messages to Snowflake as batched archives. The easiest way to do this is to batch your
messages at the output level and join the batch of messages with an
`+"[`archive`](/docs/components/processors/archive)"+` and/or `+"[`compress`](/docs/components/processors/compress)"+`
processor.

For the optimal batch size, please consult the Snowflake [documentation](https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare.html).

### Snowpipe

Given a table called `+"`BENTHOS_TBL`"+` with one column of type `+"`variant`"+`:

`+"```sql"+`
CREATE OR REPLACE TABLE BENTHOS_DB.PUBLIC.BENTHOS_TBL(RECORD variant)
`+"```"+`

and the following `+"`BENTHOS_PIPE`"+` Snowpipe:

`+"```sql"+`
CREATE OR REPLACE PIPE BENTHOS_DB.PUBLIC.BENTHOS_PIPE AUTO_INGEST = FALSE AS COPY INTO BENTHOS_DB.PUBLIC.BENTHOS_TBL FROM (SELECT * FROM @%BENTHOS_TBL) FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO)
`+"```"+`

you can configure Benthos to use the implicit table stage `+"`@%BENTHOS_TBL`"+` as the `+"`stage`"+` and
`+"`BENTHOS_PIPE`"+` as the `+"`snowpipe`"+`. In this case, you must set `+"`compression`"+` to `+"`AUTO`"+` and, if
using message batching, you'll need to configure an [`+"`archive`"+`](/docs/components/processors/archive) processor
with the `+"`concatenate`"+` format. Since the `+"`compression`"+` is set to `+"`AUTO`"+`, the
[gosnowflake](https://github.com/snowflakedb/gosnowflake) client library will compress the messages automatically so you
don't need to add a `+"[`compress`](/docs/components/processors/compress)"+` processor for message batches.

If you add `+"`STRIP_OUTER_ARRAY = TRUE`"+` in your Snowpipe `+"`FILE_FORMAT`"+`
definition, then you must use `+"`json_array`"+` instead of `+"`concatenate`"+` as the archive processor format.

Note: Only Snowpipes with `+"`FILE_FORMAT`"+` `+"`TYPE`"+` `+"`JSON`"+` are currently supported.

### Snowpipe Troubleshooting

Snowpipe [provides](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis.html) the `+"`insertReport`"+`
and `+"`loadHistoryScan`"+` REST API endpoints which can be used to get information about recent Snowpipe calls. In
order to query them, you'll first need to generate a valid JWT token for your Snowflake account. There are two methods
for doing so:
- Using the `+"`snowsql`"+` [utility](https://docs.snowflake.com/en/user-guide/snowsql.html):

`+"```shell"+`
snowsql --private-key-path rsa_key.p8 --generate-jwt -a <account> -u <user>
`+"```"+`

- Using the Python `+"`sql-api-generate-jwt`"+` [utility](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating.html#generating-a-jwt-in-python):

`+"```shell"+`
python3 sql-api-generate-jwt.py --private_key_file_path=rsa_key.p8 --account=<account> --user=<user>
`+"```"+`

Once you successfully generate a JWT token and store it into the `+"`JWT_TOKEN`"+` environment variable, then you can,
for example, query the `+"`insertReport`"+` endpoint using `+"`curl`"+`:

`+"```shell"+`
curl -H "Authorization: Bearer ${JWT_TOKEN}" "https://<account>.snowflakecomputing.com/v1/data/pipes/<database>.<schema>.<snowpipe>/insertReport"
`+"```"+`

If you need to pass in a valid `+"`requestId`"+` to any of these Snowpipe REST API endpoints, you can set a
[uuid_v4()](https://www.benthos.dev/docs/guides/bloblang/functions#uuid_v4) string in a metadata field called
`+"`request_id`"+`, log it via the [`+"`log`"+`](https://www.benthos.dev/docs/components/processors/log) processor and
then configure `+"`request_id: ${ @request_id }`"+` ). Alternatively, you can enable debug logging as described
[here](/docs/components/logger/about) and Benthos will print the Request IDs that it sends to Snowpipe.

### General Troubleshooting

The underlying [`+"`gosnowflake`"+` driver](https://github.com/snowflakedb/gosnowflake) requires write access to
the default directory to use for temporary files. Please consult the [`+"`os.TempDir`"+`](https://pkg.go.dev/os#TempDir)
docs for details on how to change this directory via environment variables.

A silent failure can occur due to [this issue](https://github.com/snowflakedb/gosnowflake/issues/701), where the
underlying [`+"`gosnowflake`"+` driver](https://github.com/snowflakedb/gosnowflake) doesn't return an error and doesn't
log a failure if it can't figure out the current username. One way to trigger this behaviour is by running Benthos in a
Docker container with a non-existent user ID (such as `+"`--user 1000:1000`"+`).
`)).
		Field(service.NewStringField("account").Description(`Account name, which is the same as the Account Identifier
as described [here](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used).
However, when using an [Account Locator](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier),
the Account Identifier is formatted as `+"`<account_locator>.<region_id>.<cloud>`"+` and this field needs to be
populated using the `+"`<account_locator>`"+` part.
`)).
		Field(service.NewStringField("region").Description(`Optional region field which needs to be populated when using
an [Account Locator](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier)
and it must be set to the `+"`<region_id>`"+` part of the Account Identifier
(`+"`<account_locator>.<region_id>.<cloud>`"+`).
`).Example("us-west-2").Optional()).
		Field(service.NewStringField("cloud").Description(`Optional cloud platform field which needs to be populated
when using an [Account Locator](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier)
and it must be set to the `+"`<cloud>`"+` part of the Account Identifier
(`+"`<account_locator>.<region_id>.<cloud>`"+`).
`).Example("aws").Example("gcp").Example("azure").Optional()).
		Field(service.NewStringField("user").Description("Username.")).
		Field(service.NewStringField("password").Description("An optional password.").Optional().Secret()).
		Field(service.NewStringField("private_key_file").Description("The path to a file containing the private SSH key.").Optional()).
		Field(service.NewStringField("private_key_pass").Description("An optional private SSH key passphrase.").Optional().Secret()).
		Field(service.NewStringField("role").Description("Role.")).
		Field(service.NewStringField("database").Description("Database.")).
		Field(service.NewStringField("warehouse").Description("Warehouse.")).
		Field(service.NewStringField("schema").Description("Schema.")).
		Field(service.NewInterpolatedStringField("stage").Description(`Stage name. Use either one of the
[supported](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage.html) stage types.`)).
		Field(service.NewInterpolatedStringField("path").Description("Stage path.").Default("")).
		Field(service.NewInterpolatedStringField("file_name").Description("Stage file name. Will be equal to the Request ID if not set or empty.").Optional().Default("").Version("v4.12.0")).
		Field(service.NewInterpolatedStringField("file_extension").Description("Stage file extension. Will be derived from the configured `compression` if not set or empty.").Optional().Default("").Example("csv").Example("parquet").Version("v4.12.0")).
		Field(service.NewIntField("upload_parallel_threads").Description("Specifies the number of threads to use for uploading files.").Advanced().Default(4).LintRule(`root = if this < 1 || this > 99 { [ "upload_parallel_threads must be between 1 and 99" ] }`)).
		Field(service.NewStringAnnotatedEnumField("compression", map[string]string{
			string(CompressionTypeNone):       "No compression is applied and messages must contain plain-text JSON. Default `file_extension`: `json`.",
			string(CompressionTypeAuto):       "Compression (gzip) is applied automatically by the output and messages must contain plain-text JSON. Default `file_extension`: `gz`.",
			string(CompressionTypeGzip):       "Messages must be pre-compressed using the gzip algorithm. Default `file_extension`: `gz`.",
			string(CompressionTypeDeflate):    "Messages must be pre-compressed using the zlib algorithm (with zlib header, RFC1950). Default `file_extension`: `deflate`.",
			string(CompressionTypeRawDeflate): "Messages must be pre-compressed using the flate algorithm (without header, RFC1951). Default `file_extension`: `raw_deflate`.",
			string(CompressionTypeZstandard):  "Messages must be pre-compressed using the Zstandard algorithm. Default `file_extension`: `zst`.",
		}).Description("Compression type.").Default(string(CompressionTypeAuto))).
		Field(service.NewInterpolatedStringField("request_id").Description("Request ID. Will be assigned a random UUID (v4) string if not set or empty.").Optional().Default("").Version("v4.12.0")).
		Field(service.NewInterpolatedStringField("snowpipe").Description(`An optional Snowpipe name. Use the `+"`<snowpipe>`"+` part from `+"`<database>.<schema>.<snowpipe>`"+`.`).Optional()).
		Field(service.NewBoolField("client_session_keep_alive").Description("Enable Snowflake keepalive mechanism to prevent the client session from expiring after 4 hours (error 390114).").Advanced().Default(false)).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewIntField("max_in_flight").Description("The maximum number of parallel message batches to have in flight at any given time.").Default(1)).
		LintRule(`root = match {
  this.exists("password") && this.password != "" && this.exists("private_key_file") && this.private_key_file != "" => [ "both `+"`password`"+` and `+"`private_key_file`"+` can't be set simultaneously" ],
  this.exists("snowpipe") && this.snowpipe != "" && (!this.exists("private_key_file") || this.private_key_file == "") => [ "`+"`private_key_file`"+` is required when setting `+"`snowpipe`"+`" ],
}`).
		Example("Kafka / realtime brokers", "Upload message batches from realtime brokers such as Kafka persisting the batch partition and offsets in the stage path and filename similarly to the [Kafka Connector scheme](https://docs.snowflake.com/en/user-guide/kafka-connector-ts.html#step-1-view-the-copy-history-for-the-table) and call Snowpipe to load them into a table. When batching is configured at the input level, it is done per-partition.", `
input:
  kafka:
    addresses:
      - localhost:9092
    topics:
      - foo
    consumer_group: benthos
    batching:
      count: 10
      period: 3s
      processors:
        - mapping: |
            meta kafka_start_offset = meta("kafka_offset").from(0)
            meta kafka_end_offset = meta("kafka_offset").from(-1)
            meta batch_timestamp = if batch_index() == 0 { now() }
        - mapping: |
            meta batch_timestamp = if batch_index() != 0 { meta("batch_timestamp").from(0) }

output:
  snowflake_put:
    account: benthos
    user: test@benthos.dev
    private_key_file: path_to_ssh_key.pem
    role: ACCOUNTADMIN
    database: BENTHOS_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%BENTHOS_TBL"
    path: benthos/BENTHOS_TBL/${! @kafka_partition }
    file_name: ${! @kafka_start_offset }_${! @kafka_end_offset }_${! meta("batch_timestamp") }
    upload_parallel_threads: 4
    compression: NONE
    snowpipe: BENTHOS_PIPE
`).
		Example("No compression", "Upload concatenated messages into a `.json` file to a table stage without calling Snowpipe.", `
output:
  snowflake_put:
    account: benthos
    user: test@benthos.dev
    private_key_file: path_to_ssh_key.pem
    role: ACCOUNTADMIN
    database: BENTHOS_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%BENTHOS_TBL"
    path: benthos
    upload_parallel_threads: 4
    compression: NONE
    batching:
      count: 10
      period: 3s
      processors:
        - archive:
            format: concatenate
`).
		Example("Parquet format with snappy compression", "Upload concatenated messages into a `.parquet` file to a table stage without calling Snowpipe.", `
output:
  snowflake_put:
    account: benthos
    user: test@benthos.dev
    private_key_file: path_to_ssh_key.pem
    role: ACCOUNTADMIN
    database: BENTHOS_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%BENTHOS_TBL"
    path: benthos
    file_extension: parquet
    upload_parallel_threads: 4
    compression: NONE
    batching:
      count: 10
      period: 3s
      processors:
        - parquet_encode:
            schema:
              - name: ID
                type: INT64
              - name: CONTENT
                type: BYTE_ARRAY
            default_compression: snappy
`).
		Example("Automatic compression", "Upload concatenated messages compressed automatically into a `.gz` archive file to a table stage without calling Snowpipe.", `
output:
  snowflake_put:
    account: benthos
    user: test@benthos.dev
    private_key_file: path_to_ssh_key.pem
    role: ACCOUNTADMIN
    database: BENTHOS_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%BENTHOS_TBL"
    path: benthos
    upload_parallel_threads: 4
    compression: AUTO
    batching:
      count: 10
      period: 3s
      processors:
        - archive:
            format: concatenate
`).
		Example("DEFLATE compression", "Upload concatenated messages compressed into a `.deflate` archive file to a table stage and call Snowpipe to load them into a table.", `
output:
  snowflake_put:
    account: benthos
    user: test@benthos.dev
    private_key_file: path_to_ssh_key.pem
    role: ACCOUNTADMIN
    database: BENTHOS_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%BENTHOS_TBL"
    path: benthos
    upload_parallel_threads: 4
    compression: DEFLATE
    snowpipe: BENTHOS_PIPE
    batching:
      count: 10
      period: 3s
      processors:
        - archive:
            format: concatenate
        - mapping: |
            root = content().compress("zlib")
`).
		Example("RAW_DEFLATE compression", "Upload concatenated messages compressed into a `.raw_deflate` archive file to a table stage and call Snowpipe to load them into a table.", `
output:
  snowflake_put:
    account: benthos
    user: test@benthos.dev
    private_key_file: path_to_ssh_key.pem
    role: ACCOUNTADMIN
    database: BENTHOS_DB
    warehouse: COMPUTE_WH
    schema: PUBLIC
    stage: "@%BENTHOS_TBL"
    path: benthos
    upload_parallel_threads: 4
    compression: RAW_DEFLATE
    snowpipe: BENTHOS_PIPE
    batching:
      count: 10
      period: 3s
      processors:
        - archive:
            format: concatenate
        - mapping: |
            root = content().compress("flate")
`)
}

func init() {
	err := service.RegisterBatchOutput("snowflake_put", snowflakePutOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newSnowflakeWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// getPrivateKey reads and parses the private key
// Inspired from https://github.com/chanzuckerberg/terraform-provider-snowflake/blob/c07d5820bea7ac3d8a5037b0486c405fdf58420e/pkg/provider/provider.go#L367
func getPrivateKey(f ifs.FS, path, passphrase string) (*rsa.PrivateKey, error) {
	privateKeyBytes, err := ifs.ReadFile(f, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read private key %s: %s", path, err)
	}
	if len(privateKeyBytes) == 0 {
		return nil, errors.New("private key is empty")
	}

	privateKeyBlock, _ := pem.Decode(privateKeyBytes)
	if privateKeyBlock == nil {
		return nil, errors.New("could not parse private key, key is not in PEM format")
	}

	if privateKeyBlock.Type == "ENCRYPTED PRIVATE KEY" {
		if passphrase == "" {
			return nil, errors.New("private key requires a passphrase, but private_key_passphrase was not supplied")
		}

		// Only keys encrypted with pbes2 http://oid-info.com/get/1.2.840.113549.1.5.13 are supported.
		// pbeWithMD5AndDES-CBC http://oid-info.com/get/1.2.840.113549.1.5.3 is not supported.
		privateKey, err := pkcs8.ParsePKCS8PrivateKeyRSA(privateKeyBlock.Bytes, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt encrypted private key (only ciphers aes-128-cbc, aes-128-gcm, aes-192-cbc, aes-192-gcm, aes-256-cbc, aes-256-gcm, and des-ede3-cbc are supported): %s", err)
		}

		return privateKey, nil
	}

	privateKey, err := ssh.ParseRawPrivateKey(privateKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("could not parse private key: %s", err)
	}

	rsaPrivateKey, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key must be of type RSA but got %T instead: ", privateKey)
	}
	return rsaPrivateKey, nil
}

// calculatePublicKeyFingerprint computes the value of the `RSA_PUBLIC_KEY_FP` for the current user based on the
// configured private key
// Inspired from https://stackoverflow.com/questions/63598044/snowpipe-rest-api-returning-always-invalid-jwt-token
func calculatePublicKeyFingerprint(privateKey *rsa.PrivateKey) (string, error) {
	pubKey := privateKey.Public()
	pubDER, err := x509.MarshalPKIXPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("failed to marshal public key: %s", err)
	}

	hash := sha256.Sum256(pubDER)
	return "SHA256:" + base64.StdEncoding.EncodeToString(hash[:]), nil
}

type dbI interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Close() error
}

type uuidGenI interface {
	NewV4() (uuid.UUID, error)
}

type httpClientI interface {
	Do(req *http.Request) (*http.Response, error)
}

type snowflakeWriter struct {
	logger *service.Logger

	account       string
	user          string
	database      string
	schema        string
	stage         *service.InterpolatedString
	path          *service.InterpolatedString
	fileName      *service.InterpolatedString
	fileExtension *service.InterpolatedString
	requestID     *service.InterpolatedString
	snowpipe      *service.InterpolatedString

	accountIdentifier         string
	putQueryFormat            string
	defaultStageFileExtension string
	privateKey                *rsa.PrivateKey
	publicKeyFingerprint      string
	dsn                       string

	connMut       sync.Mutex
	uuidGenerator uuidGenI
	httpClient    httpClientI
	nowFn         func() time.Time
	db            dbI
}

func newSnowflakeWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*snowflakeWriter, error) {
	s := snowflakeWriter{
		logger:        mgr.Logger(),
		uuidGenerator: uuid.NewGen(),
		httpClient:    http.DefaultClient,
		nowFn:         time.Now,
	}

	var err error

	if s.account, err = conf.FieldString("account"); err != nil {
		return nil, fmt.Errorf("failed to parse account: %s", err)
	}

	s.accountIdentifier = s.account

	if conf.Contains("region") {
		var region string
		if region, err = conf.FieldString("region"); err != nil {
			return nil, fmt.Errorf("failed to parse region: %s", err)
		}
		s.accountIdentifier += "." + region
	}

	if conf.Contains("cloud") {
		var cloud string
		if cloud, err = conf.FieldString("cloud"); err != nil {
			return nil, fmt.Errorf("failed to parse cloud: %s", err)
		}
		s.accountIdentifier += "." + cloud
	}

	if s.user, err = conf.FieldString("user"); err != nil {
		return nil, fmt.Errorf("failed to parse user: %s", err)
	}

	var password string
	if conf.Contains("password") {
		if password, err = conf.FieldString("password"); err != nil {
			return nil, fmt.Errorf("failed to parse password: %s", err)
		}
	}

	var role string
	if role, err = conf.FieldString("role"); err != nil {
		return nil, fmt.Errorf("failed to parse role: %s", err)
	}

	if s.database, err = conf.FieldString("database"); err != nil {
		return nil, fmt.Errorf("failed to parse database: %s", err)
	}

	var warehouse string
	if warehouse, err = conf.FieldString("warehouse"); err != nil {
		return nil, fmt.Errorf("failed to parse warehouse: %s", err)
	}

	if s.schema, err = conf.FieldString("schema"); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %s", err)
	}

	if s.stage, err = conf.FieldInterpolatedString("stage"); err != nil {
		return nil, fmt.Errorf("failed to parse stage: %s", err)
	}

	if s.path, err = conf.FieldInterpolatedString("path"); err != nil {
		return nil, fmt.Errorf("failed to parse path: %s", err)
	}

	if s.fileName, err = conf.FieldInterpolatedString("file_name"); err != nil {
		return nil, fmt.Errorf("failed to parse file_name: %s", err)
	}

	if s.fileExtension, err = conf.FieldInterpolatedString("file_extension"); err != nil {
		return nil, fmt.Errorf("failed to parse file_extension: %s", err)
	}

	var uploadParallelThreads int
	if uploadParallelThreads, err = conf.FieldInt("upload_parallel_threads"); err != nil {
		return nil, fmt.Errorf("failed to parse stage: %s", err)
	}

	compressionStr, err := conf.FieldString("compression")
	if err != nil {
		return nil, fmt.Errorf("failed to parse compression: %s", err)
	}

	compression := CompressionType(compressionStr)
	var autoCompress, sourceCompression string
	// Should match file extensions in https://github.com/snowflakedb/gosnowflake/blob/2648a83699492c0613a888e66298157fc1e45bf5/file_compression_type.go
	switch compression {
	case CompressionTypeNone:
		s.defaultStageFileExtension = "json"
		autoCompress = "FALSE"
		sourceCompression = "NONE"
	case CompressionTypeAuto:
		s.defaultStageFileExtension = "gz"
		autoCompress = "TRUE"
		sourceCompression = "AUTO_DETECT"
	case CompressionTypeGzip:
		s.defaultStageFileExtension = "gz"
		autoCompress = "FALSE"
		sourceCompression = "GZIP"
	case CompressionTypeDeflate:
		s.defaultStageFileExtension = "deflate"
		autoCompress = "FALSE"
		sourceCompression = string(compression)
	case CompressionTypeRawDeflate:
		s.defaultStageFileExtension = "raw_deflate"
		autoCompress = "FALSE"
		sourceCompression = string(compression)
	case CompressionTypeZstandard:
		s.defaultStageFileExtension = "zst"
		autoCompress = "FALSE"
		sourceCompression = string(compression)
	default:
		return nil, fmt.Errorf("unrecognised compression type: %s", compression)
	}

	// File path and stage are populated dynamically via interpolation
	s.putQueryFormat = fmt.Sprintf("PUT file://%%s %%s AUTO_COMPRESS = %s SOURCE_COMPRESSION = %s PARALLEL=%d", autoCompress, sourceCompression, uploadParallelThreads)

	if s.requestID, err = conf.FieldInterpolatedString("request_id"); err != nil {
		return nil, fmt.Errorf("failed to parse request_id: %s", err)
	}

	if conf.Contains("snowpipe") {
		if s.snowpipe, err = conf.FieldInterpolatedString("snowpipe"); err != nil {
			return nil, fmt.Errorf("failed to parse snowpipe: %s", err)
		}
	}

	authenticator := gosnowflake.AuthTypeJwt
	if password == "" {
		var privateKeyFile string
		if privateKeyFile, err = conf.FieldString("private_key_file"); err != nil {
			return nil, fmt.Errorf("failed to parse private_key_file: %s", err)
		}

		var privateKeyPass string
		if conf.Contains("private_key_pass") {
			if privateKeyPass, err = conf.FieldString("private_key_pass"); err != nil {
				return nil, fmt.Errorf("failed to parse private_key_pass: %s", err)
			}
		}

		if s.privateKey, err = getPrivateKey(mgr.FS(), privateKeyFile, privateKeyPass); err != nil {
			return nil, fmt.Errorf("failed to read private key: %s", err)
		}

		if s.publicKeyFingerprint, err = calculatePublicKeyFingerprint(s.privateKey); err != nil {
			return nil, fmt.Errorf("failed to calculate public key fingerprint: %s", err)
		}
	} else {
		authenticator = gosnowflake.AuthTypeSnowflake
	}

	var params map[string]*string
	if clientSessionKeepAlive, err := conf.FieldBool("client_session_keep_alive"); err != nil {
		return nil, fmt.Errorf("failed to parse client_session_keep_alive: %s", err)
	} else if clientSessionKeepAlive {
		params = make(map[string]*string)
		value := "true"
		// This parameter must be set to prevent the auth token from expiring after 4 hours.
		// Details here: https://github.com/snowflakedb/gosnowflake/issues/556
		params["client_session_keep_alive"] = &value
	}

	if s.dsn, err = gosnowflake.DSN(&gosnowflake.Config{
		Account: s.accountIdentifier,
		// Region: The driver extracts the region automatically from the account and I think it doesn't have to be set here
		Password:      password,
		Authenticator: authenticator,
		User:          s.user,
		Role:          role,
		Database:      s.database,
		Warehouse:     warehouse,
		Schema:        s.schema,
		PrivateKey:    s.privateKey,
		Params:        params,
	}); err != nil {
		return nil, fmt.Errorf("failed to construct DSN: %s", err)
	}

	return &s, nil
}

//------------------------------------------------------------------------------

func (s *snowflakeWriter) Connect(ctx context.Context) error {
	var err error
	s.db, err = sql.Open("snowflake", s.dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to snowflake: %s", err)
	}

	return nil
}

// createJWT creates a new Snowpipe JWT token
// Inspired from https://stackoverflow.com/questions/63598044/snowpipe-rest-api-returning-always-invalid-jwt-token
func (s *snowflakeWriter) createJWT() (string, error) {
	// Need to use the account without the region segment as described in https://stackoverflow.com/questions/65811588/snowflake-jdbc-driver-throws-net-snowflake-client-jdbc-snowflakesqlexception-jw
	qualifiedUsername := strings.ToUpper(s.account + "." + s.user)
	now := s.nowFn().UTC()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": qualifiedUsername + "." + s.publicKeyFingerprint,
		"sub": qualifiedUsername,
		"iat": now.Unix(),
		"exp": now.Add(defaultJWTTimeout).Unix(),
	})

	return token.SignedString(s.privateKey)
}

func (s *snowflakeWriter) getSnowpipeInsertURL(snowpipe, requestID string) string {
	query := url.Values{"requestId": []string{requestID}}
	u := url.URL{
		Scheme:   "https",
		Host:     s.accountIdentifier + ".snowflakecomputing.com",
		Path:     path.Join("/v1/data/pipes", fmt.Sprintf("%s.%s.%s", s.database, s.schema, snowpipe), "insertFiles"),
		RawQuery: query.Encode(),
	}
	return u.String()
}

func (s *snowflakeWriter) callSnowpipe(ctx context.Context, snowpipe, requestID, filePath string) error {
	jwtToken, err := s.createJWT()
	if err != nil {
		return fmt.Errorf("failed to create Snowpipe JWT token: %s", err)
	}

	type File struct {
		Path string `json:"path"`
	}
	reqPayload := struct {
		Files []File `json:"files"`
	}{
		Files: []File{
			{
				Path: filePath,
			},
		},
	}

	buf := bytes.Buffer{}
	if err := json.NewEncoder(&buf).Encode(reqPayload); err != nil {
		return fmt.Errorf("failed to marshal request body JSON: %s", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.getSnowpipeInsertURL(snowpipe, requestID), &buf)
	if err != nil {
		return fmt.Errorf("failed to create Snowpipe HTTP request: %s", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute Snowpipe HTTP request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received unexpected Snowpipe response status: %d", resp.StatusCode)
	}

	var respPayload struct {
		ResponseCode string
	}
	if err = json.NewDecoder(resp.Body).Decode(&respPayload); err != nil {
		return fmt.Errorf("failed to decode Snowpipe HTTP response: %s", err)
	}
	if respPayload.ResponseCode != "SUCCESS" {
		return fmt.Errorf("received unexpected Snowpipe response code: %s", respPayload.ResponseCode)
	}

	return nil
}

func (s *snowflakeWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	s.connMut.Lock()
	defer s.connMut.Unlock()
	if s.db == nil {
		return service.ErrNotConnected
	}

	type file struct {
		stage         string
		stagePath     string
		fileName      string
		fileExtension string
		requestID     string
		snowpipe      string
	}

	// Concatenate messages into sub-batches based on matching interpolated fields.
	// TODO: Maybe add a check to ensure that the interpolated snowpipe is consistent across each sub-batch.
	files := map[file][]byte{}
	for _, msg := range batch {
		var (
			f   file
			err error
		)

		if f.stage, err = s.stage.TryString(msg); err != nil {
			return fmt.Errorf("failed to get stage: %s", err)
		} else if f.stage == "" {
			return fmt.Errorf("stage cannot be empty: %s", err)
		}

		if f.stagePath, err = s.path.TryString(msg); err != nil {
			return fmt.Errorf("failed to get stage path: %s", err)
		}

		if f.requestID, err = s.requestID.TryString(msg); err != nil {
			return fmt.Errorf("failed to get request ID: %s", err)
		}

		if f.fileName, err = s.fileName.TryString(msg); err != nil {
			return fmt.Errorf("failed to get file: %s", err)
		}

		if f.fileExtension, err = s.fileExtension.TryString(msg); err != nil {
			return fmt.Errorf("failed to get file extension: %s", err)
		} else if f.fileExtension == "" {
			f.fileExtension = s.defaultStageFileExtension
		}

		if s.snowpipe != nil {
			if f.snowpipe, err = s.snowpipe.TryString(msg); err != nil {
				return fmt.Errorf("failed to get snowpipe: %s", err)
			}
		}

		msgBytes, err := msg.AsBytes()
		if err != nil {
			return fmt.Errorf("failed to get message bytes: %s", err)
		}

		files[f] = append(files[f], msgBytes...)
	}

	// Stage each file in Snowflake and, optionally, call Snowpipe
	for f, fBytes := range files {
		requestID := f.requestID
		if requestID == "" {
			uuid, err := s.uuidGenerator.NewV4()
			if err != nil {
				return fmt.Errorf("failed to generate requestID: %s", err)
			}

			requestID = uuid.String()
		}

		fileName := f.fileName
		if fileName == "" {
			fileName = requestID
		}

		filePath := path.Join(f.stagePath, fileName+"."+f.fileExtension)

		_, err := s.db.ExecContext(gosnowflake.WithFileStream(
			gosnowflake.WithFileTransferOptions(ctx, &gosnowflake.SnowflakeFileTransferOptions{RaisePutGetError: true}),
			bytes.NewReader(fBytes)), fmt.Sprintf(s.putQueryFormat, filePath, path.Join(f.stage, f.stagePath)))
		if err != nil {
			return fmt.Errorf("failed to run query: %s", err)
		}

		if f.snowpipe != "" {
			s.logger.Debugf("Calling Snowpipe with requestId=%s", requestID)

			if err := s.callSnowpipe(ctx, f.snowpipe, requestID, filePath); err != nil {
				return fmt.Errorf("failed to call Snowpipe: %s", err)
			}
		}
	}

	return nil
}

func (s *snowflakeWriter) Close(ctx context.Context) error {
	s.connMut.Lock()
	defer s.connMut.Unlock()

	return s.db.Close()
}
