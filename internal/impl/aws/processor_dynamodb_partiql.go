package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	conf := service.NewConfigSpec().
		Summary("Executes a PartiQL expression against a DynamoDB table for each message.").
		Description("Both writes or reads are supported, when the query is a read the contents of the message will be replaced with the result. This processor is more efficient when messages are pre-batched as the whole batch will be executed in a single call.").
		Categories("Integration").
		Version("3.48.0").
		Field(service.NewStringField("query").Description("A PartiQL query to execute for each message.")).
		Field(service.NewBoolField("unsafe_dynamic_query").Description("Whether to enable dynamic queries that support interpolation functions.").Advanced().Default(false)).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that, for each message, creates a list of arguments to use with the query.").Default("")).
		Example(
			"Insert",
			`The following example inserts rows into the table footable with the columns foo, bar and baz populated with values extracted from messages:`,
			`
pipeline:
  processors:
    - aws_dynamodb_partiql:
        query: "INSERT INTO footable VALUE {'foo':'?','bar':'?','baz':'?'}"
        args_mapping: |
          root = [
            { "S": this.foo },
            { "S": meta("kafka_topic") },
            { "S": this.document.content },
          ]
`,
		)

	for _, f := range config.SessionFields() {
		conf = conf.Field(f)
	}

	err := service.RegisterBatchProcessor(
		"aws_dynamodb_partiql", conf,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			sess, err := GetSession(context.TODO(), conf)
			if err != nil {
				return nil, err
			}
			client := dynamodb.NewFromConfig(sess)
			query, err := conf.FieldString("query")
			if err != nil {
				return nil, err
			}
			args, err := conf.FieldBloblang("args_mapping")
			if err != nil {
				return nil, err
			}
			allowDynQuery, err := conf.FieldBool("unsafe_dynamic_query")
			if err != nil {
				return nil, err
			}
			var dynQuery *service.InterpolatedString
			if allowDynQuery {
				mgr.Logger().Warn("using unsafe_dynamic_query leaves you vulnerable to SQL injection attacks")
				if dynQuery, err = service.NewInterpolatedString(query); err != nil {
					return nil, fmt.Errorf("failed to parse query: %v", err)
				}
			}
			return newDynamoDBPartiQL(mgr.Logger(), client, query, dynQuery, args), nil
		})
	if err != nil {
		panic(err)
	}
}

type dynamoDBPartiQL struct {
	logger *service.Logger
	client dynamoDBAPI

	query    string
	dynQuery *service.InterpolatedString
	args     *bloblang.Executor
}

func newDynamoDBPartiQL(
	logger *service.Logger,
	client dynamoDBAPI,
	query string,
	dynQuery *service.InterpolatedString,
	args *bloblang.Executor,
) *dynamoDBPartiQL {
	return &dynamoDBPartiQL{
		logger:   logger,
		client:   client,
		query:    query,
		dynQuery: dynQuery,
		args:     args,
	}
}

func (d *dynamoDBPartiQL) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	stmts := []types.BatchStatementRequest{}
	for i := range batch {
		req := types.BatchStatementRequest{}
		req.Statement = &d.query
		if d.dynQuery != nil {
			query, err := batch.TryInterpolatedString(i, d.dynQuery)
			if err != nil {
				return nil, fmt.Errorf("query interpolation error: %w", err)
			}
			req.Statement = &query
		}

		argMsg, err := batch.BloblangQuery(i, d.args)
		if err != nil {
			return nil, fmt.Errorf("error evaluating arg mapping at index %d: %v", i, err)
		}

		argStructured, err := argMsg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("error evaluating arg mapping as structured at index %d: %v", i, err)
		}

		argsSlice, ok := argStructured.([]any)
		if !ok {
			return nil, fmt.Errorf("arg mapping resulted in non-array value at index %d: %T", i, argStructured)
		}

		for i, a := range argsSlice {
			tmp, err := objFormToAttributeValue(a)
			if err != nil {
				return nil, fmt.Errorf("arg mapping index %d failed to map to an attribute value: %v", i, err)
			}
			req.Parameters = append(req.Parameters, tmp)
		}

		stmts = append(stmts, req)
	}

	batchResult, err := d.client.BatchExecuteStatement(ctx, &dynamodb.BatchExecuteStatementInput{
		Statements: stmts,
	})
	if err != nil {
		return nil, err
	}

	for i, res := range batchResult.Responses {
		if res.Error != nil {
			code := fmt.Sprintf(" (%v)", res.Error.Code)
			batch[i].SetError(fmt.Errorf("failed to process statement%v: %v", code, *res.Error.Message))
			continue
		}
		if res.Item != nil {
			resMap := map[string]any{}
			for k, v := range res.Item {
				resMap[k] = attributeValueToObjForm(v)
			}
			batch[i].SetStructuredMut(resMap)
		}
	}

	return []service.MessageBatch{batch}, nil
}

func (d *dynamoDBPartiQL) Close(ctx context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func attributeValueToObjForm(v types.AttributeValue) map[string]any {
	switch t := v.(type) {
	case *types.AttributeValueMemberB:
		return map[string]any{
			"B": t.Value,
		}
	case *types.AttributeValueMemberBOOL:
		return map[string]any{
			"BOOL": t.Value,
		}
	case *types.AttributeValueMemberBS:
		lAny := make([]any, len(t.Value))
		for i, v := range t.Value {
			lAny[i] = v
		}
		return map[string]any{
			"BS": lAny,
		}
	case *types.AttributeValueMemberL:
		lAny := make([]any, len(t.Value))
		for i, v := range t.Value {
			lAny[i] = attributeValueToObjForm(v)
		}
		return map[string]any{
			"L": lAny,
		}
	case *types.AttributeValueMemberM:
		mAny := make(map[string]any, len(t.Value))
		for k, v := range t.Value {
			mAny[k] = attributeValueToObjForm(v)
		}
		return map[string]any{
			"M": mAny,
		}
	case *types.AttributeValueMemberN:
		return map[string]any{
			"N": t.Value,
		}
	case *types.AttributeValueMemberNS:
		lAny := make([]any, len(t.Value))
		for i, v := range t.Value {
			lAny[i] = v
		}
		return map[string]any{
			"NS": lAny,
		}
	case *types.AttributeValueMemberNULL:
		return map[string]any{
			"NULL": t.Value,
		}
	case *types.AttributeValueMemberS:
		return map[string]any{
			"S": t.Value,
		}
	case *types.AttributeValueMemberSS:
		lAny := make([]any, len(t.Value))
		for i, v := range t.Value {
			lAny[i] = v
		}
		return map[string]any{
			"SS": lAny,
		}
	}
	return map[string]any{
		"NULL": true,
	}
}

func objFormToAttributeValue(v any) (types.AttributeValue, error) {
	obj, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected object value, got %T", v)
	}

	if v, ok := obj["B"].([]byte); ok {
		return &types.AttributeValueMemberB{
			Value: v,
		}, nil
	}
	if v, ok := obj["B"].(string); ok {
		return &types.AttributeValueMemberB{
			Value: []byte(v),
		}, nil
	}
	if v, ok := obj["BOOL"].(bool); ok {
		return &types.AttributeValueMemberBOOL{
			Value: v,
		}, nil
	}
	if v, ok := obj["BS"].([]any); ok {
		var a [][]byte
		for _, vs := range v {
			switch t := vs.(type) {
			case string:
				a = append(a, []byte(t))
			case []byte:
				a = append(a, t)
			}
		}
		return &types.AttributeValueMemberBS{
			Value: a,
		}, nil
	}
	if v, ok := obj["L"].([]any); ok {
		var a []types.AttributeValue
		for i, vl := range v {
			tmp, err := objFormToAttributeValue(vl)
			if err != nil {
				return nil, fmt.Errorf("%v: %w", i, err)
			}
			a = append(a, tmp)
		}
		return &types.AttributeValueMemberL{
			Value: a,
		}, nil
	}
	if v, ok := obj["M"].(map[string]any); ok {
		a := map[string]types.AttributeValue{}
		for k, vl := range v {
			tmp, err := objFormToAttributeValue(vl)
			if err != nil {
				return nil, fmt.Errorf("%v: %w", k, err)
			}
			a[k] = tmp
		}
		return &types.AttributeValueMemberM{
			Value: a,
		}, nil
	}
	if v, exists := obj["N"]; exists {
		switch t := v.(type) {
		case string:
			return &types.AttributeValueMemberN{
				Value: t,
			}, nil
		default:
			return &types.AttributeValueMemberN{
				Value: fmt.Sprintf("%v", t),
			}, nil
		}
	}
	if v, ok := obj["NS"].([]any); ok {
		var a []string
		for _, e := range v {
			switch t := e.(type) {
			case string:
				a = append(a, t)
			default:
				a = append(a, fmt.Sprintf("%v", t))
			}
		}
		return &types.AttributeValueMemberNS{
			Value: a,
		}, nil
	}
	if v, ok := obj["NULL"].(bool); ok {
		return &types.AttributeValueMemberNULL{
			Value: v,
		}, nil
	}
	if v, ok := obj["S"].(string); ok {
		return &types.AttributeValueMemberS{
			Value: v,
		}, nil
	}
	if v, ok := obj["SS"].([]any); ok {
		var a []string
		for _, e := range v {
			s, _ := e.(string)
			a = append(a, s)
		}
		return &types.AttributeValueMemberSS{
			Value: a,
		}, nil
	}
	return nil, errors.New("expected object to contain attribute key")
}
