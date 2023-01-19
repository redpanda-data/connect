package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

type bigQuerySelectInputConfig struct {
	project       string
	queryParts    *bqQueryParts
	argsMapping   *bloblang.Executor
	queryPriority bigquery.QueryPriority
	jobLabels     map[string]string
}

func bigQuerySelectInputConfigFromParsed(inConf *service.ParsedConfig) (conf bigQuerySelectInputConfig, err error) {
	queryParts := &bqQueryParts{}
	conf.queryParts = queryParts

	if conf.project, err = inConf.FieldString("project"); err != nil {
		return
	}

	if inConf.Contains("args_mapping") {
		if conf.argsMapping, err = inConf.FieldBloblang("args_mapping"); err != nil {
			return
		}
	}

	if conf.jobLabels, err = inConf.FieldStringMap("job_labels"); err != nil {
		return
	}

	if queryParts.table, err = inConf.FieldString("table"); err != nil {
		return
	}

	if queryParts.columns, err = inConf.FieldStringList("columns"); err != nil {
		return
	}

	if inConf.Contains("where") {
		if queryParts.where, err = inConf.FieldString("where"); err != nil {
			return
		}
	}

	if inConf.Contains("prefix") {
		queryParts.prefix, err = inConf.FieldString("prefix")
		if err != nil {
			return
		}
	}

	if inConf.Contains("suffix") {
		queryParts.suffix, err = inConf.FieldString("suffix")
		if err != nil {
			return
		}
	}

	if conf.queryPriority, err = parseQueryPriority(inConf, "priority"); err != nil {
		return
	}

	return
}

func newBigQuerySelectInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.63.0").
		Categories("Services", "GCP").
		Summary("Executes a `SELECT` query against BigQuery and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted, this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
		Field(service.NewStringField("project").Description("GCP project where the query job will execute.")).
		Field(service.NewStringField("table").Description("Fully-qualified BigQuery table name to query.").Example("bigquery-public-data.samples.shakespeare")).
		Field(service.NewStringListField("columns").Description("A list of columns to query.")).
		Field(service.NewStringField("where").
			Description("An optional where clause to add. Placeholder arguments are populated with the `args_mapping` field. Placeholders should always be question marks (`?`).").
			Example("type = ? and created_at > ?").
			Example("user_id = ?").
			Optional(),
		).
		Field(service.NewStringMapField("job_labels").Description("A list of labels to add to the query job.").Default(map[string]string{})).
		Field(service.NewStringField("priority").Description("The priority with which to schedule the query.").Default("")).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `where`.").
			Example(`root = [ "article", now().ts_format("2006-01-02") ]`).
			Optional()).
		Field(service.NewStringField("prefix").
			Description("An optional prefix to prepend to the select query (before SELECT).").
			Optional()).
		Field(service.NewStringField("suffix").
			Description("An optional suffix to append to the select query.").
			Optional()).
		Example("Word counts",
			`
Here we query the public corpus of Shakespeare's works to generate a stream of the top 10 words that are 3 or more characters long:`,
			`
input:
  gcp_bigquery_select:
    project: sample-project
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
`,
		)
}

type bigQuerySelectInput struct {
	logger *service.Logger
	config *bigQuerySelectInputConfig

	client bqClient

	shutdownSig *shutdown.Signaller

	// Represents a row iterator that returns query results
	// The indirection provided by the `bigqueryIterator` interface allows test
	// code to conveniently create mock iterators
	iterator bigqueryIterator
}

func newBigQuerySelectInput(inConf *service.ParsedConfig, logger *service.Logger) (*bigQuerySelectInput, error) {
	conf, err := bigQuerySelectInputConfigFromParsed(inConf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &bigQuerySelectInput{
		logger:      logger,
		config:      &conf,
		shutdownSig: shutdown.NewSignaller(),
	}, nil
}

func (inp *bigQuerySelectInput) Connect(ctx context.Context) error {
	jobctx, _ := inp.shutdownSig.CloseAtLeisureCtx(context.Background())

	if inp.client == nil {
		client, err := bigquery.NewClient(jobctx, inp.config.project)
		if err != nil {
			return fmt.Errorf("failed to create bigquery client: %w", err)
		}
		inp.client = wrapBQClient(client, inp.logger)
	}

	var args []any
	argsMapping := inp.config.argsMapping

	if argsMapping != nil {
		rawArgs, err := inp.config.argsMapping.Query(nil)
		if err != nil {
			return err
		}

		checkedArgs, ok := rawArgs.([]any)
		if !ok {
			return fmt.Errorf("mapping returned non-array result: %T", rawArgs)
		}

		args = checkedArgs
	}

	iter, err := inp.client.RunQuery(jobctx, &bqQueryBuilderOptions{
		queryParts:    inp.config.queryParts,
		jobLabels:     inp.config.jobLabels,
		queryPriority: inp.config.queryPriority,
		args:          args,
	})
	if err != nil {
		return err
	}

	inp.iterator = iter

	return nil
}

func (inp *bigQuerySelectInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if inp.iterator == nil {
		return nil, nil, fmt.Errorf("query result iterator is not set: %w", service.ErrNotConnected)
	}

	var row map[string]bigquery.Value
	err := inp.iterator.Next(&row)
	if errors.Is(err, iterator.Done) {
		return nil, nil, service.ErrEndOfInput
	}
	if err != nil {
		return nil, nil, err
	}

	bs, err := json.Marshal(row)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal row to json: %w", err)
	}

	msg := service.NewMessage(bs)

	return msg, func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

func (inp *bigQuerySelectInput) Close(ctx context.Context) error {
	inp.shutdownSig.CloseNow()

	if inp.client != nil {
		return inp.client.Close()
	}

	return nil
}

func init() {
	err := service.RegisterInput(
		"gcp_bigquery_select", newBigQuerySelectInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newBigQuerySelectInput(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(i), nil
		})
	if err != nil {
		panic(err)
	}
}
