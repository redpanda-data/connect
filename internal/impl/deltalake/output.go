package deltalake

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/nillock"
	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
)

const (
	dlFieldTableURL = "table_url"
	dlFieldBatching = "batching"
)

func outputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.53.0").
		Summary("Ingests data into Delta Lake tables such as those managed by Databricks").
		Description(`
    Ingests parquet data into Delta Lake tables, supporting both Databricks-managed and open-source Spark deployments.
`).
		Fields(
			service.NewStringField(dlFieldTableURL).
				Description("The URL to table to ingest data into. Infers the storage backend to use from the scheme in the given table URL."),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput("delta_lake", outputConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newDeltaLakeWriterFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

func newDeltaLakeWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*deltaLakeWriter, error) {
	d := &deltaLakeWriter{logger: mgr.Logger()}

	var err error
	if d.tableURL, err = conf.FieldString(dlFieldTableURL); err != nil {
		return nil, err
	}

	u, err := url.Parse(d.tableURL)
	if err != nil {
		return nil, err
	}
	d.bucket = u.Host
	d.prefix = strings.TrimPrefix(u.Path, "/")

	return d, nil
}

type deltaLakeWriter struct {
	logger      *service.Logger
	tableURL    string
	bucket      string
	prefix      string
	table       *delta.Table
	objectStore storage.ObjectStore
	s3Client    *s3.Client
}

func (d *deltaLakeWriter) Connect(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	objectStorePath := storage.NewPath(d.tableURL)
	objectStore, err := s3store.New(s3Client, objectStorePath)
	if err != nil {
		return fmt.Errorf("creating object store: %w", err)
	}

	stateStore := newLogStateStore(s3Client, d.bucket, d.prefix)
	locker := nillock.New()
	table, err := delta.OpenTable(objectStore, locker, stateStore)
	if err != nil {
		return fmt.Errorf("opening table: %w", err)
	}

	d.table = table
	d.objectStore = objectStore
	return nil
}

func (d *deltaLakeWriter) Write(ctx context.Context, msg *service.Message) error {
	id := "2b0046bb-f9d1-46b1-9476-8eeaa6c27ce5"
	// id := uuid.New().String()
	fileName := fmt.Sprintf("part-%s.snappy.parquet", id)

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("converting message to bytes: %w", err)
	}

	err = d.objectStore.Put(storage.NewPath(fileName), msgBytes)
	if err != nil {
		return fmt.Errorf("uploading object: %w", err)
	}

	add, _, err := delta.NewAdd(d.objectStore, storage.NewPath(fileName), make(map[string]string))
	if err != nil {
		return fmt.Errorf("creating new add operation: %w", err)
	}

	transaction := d.table.CreateTransaction(delta.NewTransactionOptions())
	transaction.AddAction(add)
	operation := delta.Write{Mode: delta.Append}
	// TODO: metadata
	transaction.SetOperation(operation)
	version, err := transaction.Commit()
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	fmt.Printf("version %d\n", version)
	return nil
}

func (d *deltaLakeWriter) Close(ctx context.Context) error {
	return nil
}

func newLogStateStore(s3Client *s3.Client, bucket, prefix string) *logStateStore {
	return &logStateStore{
		s3Client: s3Client,
		bucket:   bucket,
		prefix:   prefix,
	}
}

type logStateStore struct {
	s3Client *s3.Client
	bucket   string
	prefix   string
}

var logEntryRegex = regexp.MustCompile(`^\d{20}\.json$`)

func (l *logStateStore) Get() (state.CommitState, error) {
	ctx := context.Background()
	paginator := s3.NewListObjectsV2Paginator(l.s3Client, &s3.ListObjectsV2Input{
		Bucket: &l.bucket,
		Prefix: &l.prefix,
	})

	var newestKey string

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return state.CommitState{}, fmt.Errorf("error getting page: %w", err)
		}

		for _, obj := range page.Contents {
			// empty string always compares less than a non-empty string
			if logEntryRegex.MatchString(*obj.Key) && *obj.Key > newestKey {
				newestKey = *obj.Key
			}
		}
	}

	if newestKey == "" {
		return state.CommitState{}, errors.New("no delta log files found")
	}

	version, err := strconv.ParseInt(newestKey[:20], 10, 64)
	if err != nil {
		return state.CommitState{}, fmt.Errorf("converting delta log entry filename to integer: %q: %w", newestKey, err)
	}

	return state.CommitState{Version: version}, nil
}

func (l *logStateStore) Put(state.CommitState) error {
	// nothing to do
	return nil
}
