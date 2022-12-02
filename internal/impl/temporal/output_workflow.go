package temporal

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	tmprl "go.temporal.io/sdk/temporal"
)

func newWorkflowOutputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Categories("Services").
		Summary("Executes a Temporal workflow with messages passed as inputs.")

	for _, f := range clientOptionsFields() {
		spec = spec.Field(f)
	}

	for _, f := range executeWorkflowConfigFields() {
		spec = spec.Field(f)
	}

	retryField := service.NewObjectField("retry_policy", retryPolicyConfigFields()...).
		Description("Optional retry policy for workflow.").
		Optional()

	spec = spec.
		Field(retryField).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of workflow execution requests that can be made in parallel.").Default(64))

	return spec
}

type workflowOutput struct {
	logger *service.Logger
	client client.Client

	clientOptions *client.Options
	execConfig    *executeConfig

	retry *tmprl.RetryPolicy
}

func newWorkflowOutputFromConfig(
	conf *service.ParsedConfig,
	manager *service.Resources,
) (*workflowOutput, error) {
	output := &workflowOutput{}
	var err error

	if output.clientOptions, err = newClientOptionsFromParsed(conf); err != nil {
		return nil, err
	}

	if output.execConfig, err = newExecuteWorkflowConfigFromParsed(conf); err != nil {
		return nil, err
	}

	if conf.Contains("retry_policy") {
		retryConf := conf.Namespace("retry_policy")

		if output.retry, err = newRetryPolicyFromParsed(retryConf); err != nil {
			return nil, err
		}
	}

	logger := manager.Logger()
	output.logger = logger
	output.clientOptions.Logger = &wrappedLogger{logger: logger}

	return output, nil
}

func (output *workflowOutput) Connect(ctx context.Context) error {
	var err error
	output.client, err = client.Dial(*output.clientOptions)

	return err
}

func (output *workflowOutput) Close(ctx context.Context) error {
	output.client.Close()

	return nil
}

func (output *workflowOutput) Write(ctx context.Context, msg *service.Message) error {
	var err error

	options, err := output.buildOptions(msg)
	if err != nil {
		return err
	}

	args, err := output.buildArgs(msg)
	if err != nil {
		return err
	}

	cfg := output.execConfig
	workflowName := cfg.name.String(msg)

	we, err := output.client.ExecuteWorkflow(ctx, *options, workflowName, args...)
	if err != nil {
		return err
	}

	output.logger.With(
		"event", "workflow.started",
		"id", we.GetID(),
		"run_id", we.GetRunID(),
	).Debug("workflow started")

	var result any
	switch cfg.awaitResult {
	case awaitCurrentRun:
		err = we.GetWithOptions(ctx, &result, client.WorkflowRunGetOptions{DisableFollowingRuns: true})
	case awaitFinalRun:
		err = we.Get(ctx, &result)
	}

	outcome := "failure"
	if err == nil {
		outcome = "success"
	}

	output.logger.With(
		"event", "workflow.finished",
		"outcome", outcome,
		"id", we.GetID(),
		"run_id", we.GetRunID(),
	).Debug("workflow finished")

	return err
}

func (output *workflowOutput) buildOptions(msg *service.Message) (*client.StartWorkflowOptions, error) {
	cfg := output.execConfig

	options := client.StartWorkflowOptions{
		ID:                       cfg.id.String(msg),
		TaskQueue:                cfg.taskQueue.String(msg),
		WorkflowExecutionTimeout: cfg.executionTimeout,
		WorkflowRunTimeout:       cfg.runTimeout,
		WorkflowTaskTimeout:      cfg.taskTimeout,
		RetryPolicy:              output.retry,
	}

	switch cfg.idReusePolicy {
	case idReuseAllowDuplicate:
		options.WorkflowIDReusePolicy = enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	case idReuseAllowDuplicateFailedOnly:
		options.WorkflowIDReusePolicy = enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	case idReuseRejectDuplicate:
		options.WorkflowIDReusePolicy = enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	case idReuseTerminateIfRunning:
		options.WorkflowIDReusePolicy = enums.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	}

	if cfg.memo != nil {
		memoRaw, err := msg.BloblangQuery(cfg.memo)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate memo mapping: %w", err)
		}

		imemo, err := memoRaw.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("failed to parse memo option: %w", err)
		}

		if memo, ok := imemo.(map[string]any); ok {
			options.Memo = memo
		} else {
			return nil, fmt.Errorf("memo mapping returned non-object result: %T", imemo)
		}
	}

	if cfg.searchAttributes != nil {
		attrRaw, err := msg.BloblangQuery(cfg.searchAttributes)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate search attributes mapping: %w", err)
		}

		iSearchAttr, err := attrRaw.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("failed to parse search attributes: %w", err)
		}

		if searchAttr, ok := iSearchAttr.(map[string]any); ok {
			options.SearchAttributes = searchAttr
		} else {
			return nil, fmt.Errorf("search attributes mapping returned non-object result: %T", iSearchAttr)
		}
	}

	return &options, nil
}

func (output *workflowOutput) buildArgs(msg *service.Message) ([]any, error) {
	var err error

	mapping := output.execConfig.argsMapping

	var structured any
	if mapping == nil {
		structured, err = msg.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}
	} else {
		rawArgs, err := msg.BloblangQuery(mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate args mapping: %w", err)
		}

		structured, err = rawArgs.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("failed to parse args: %w", err)
		}
	}

	var args []any
	switch v := structured.(type) {
	case []any:
		args = v
	default:
		args = []any{v}
	}

	return args, nil
}

func init() {
	err := service.RegisterOutput(
		"temporal_workflow", newWorkflowOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			out, err := newWorkflowOutputFromConfig(conf, mgr)
			if err != nil {
				return nil, 0, err
			}

			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}

			return out, maxInFlight, nil
		})

	if err != nil {
		panic(err)
	}
}
