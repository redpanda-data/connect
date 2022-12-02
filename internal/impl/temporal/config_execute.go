package temporal

import (
	"time"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	idReuseAllowDuplicate           = "allow_duplicate"
	idReuseAllowDuplicateFailedOnly = "allow_duplicate_failed_only"
	idReuseRejectDuplicate          = "reject_duplicate"
	idReuseTerminateIfRunning       = "terminate_if_running"
)

const (
	awaitSkip       = "skip"
	awaitCurrentRun = "current_run"
	awaitFinalRun   = "final_run"
)

func executeWorkflowConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewInterpolatedStringField("name").
			Description("The name of the workflow to execute."),
		service.NewInterpolatedStringField("task_queue").
			Description("The queue where the workflow will be scheduled."),
		service.NewBloblangField("args_mapping").
			Description("A mapping that is used to get the arguments for the workflow execution.").
			Optional(),
		service.NewStringAnnotatedEnumField("await_result", map[string]string{
			awaitSkip:       "Do not wait for any workflow runs and return immediately after successfully starting a workflow execution.",
			awaitCurrentRun: "Only wait for current workflow execution run to complete. Any subsequent runs, due to continue-as-new or retries, are ignored.",
			awaitFinalRun:   "Wait for the latest workflow execution run to complete. The output will block until retries/failures are resolved or continue-as-new runs return.",
		}).
			Description("Determines if the output will wait for a result from the workflow execution.").
			Default(awaitFinalRun),
		service.NewInterpolatedStringField("id").
			Description("The business identifier of the workflow execution.").
			Optional(),
		service.NewStringEnumField("id_reuse_policy", idReuseAllowDuplicate, idReuseAllowDuplicateFailedOnly, idReuseRejectDuplicate, idReuseTerminateIfRunning).
			Description("Whether server allow reuse of workflow ID, can be useful for dedupe logic if set to RejectDuplicate.").
			Optional(),
		service.NewBloblangField("memo").
			Description("Optional non-indexed info that will be shown in list workflow.").
			Optional(),
		service.NewBloblangField("search_attributes").
			Description("Optional indexed info that can be used in query of List/Scan/Count workflow APIs.").
			Optional(),
		service.NewDurationField("execution_timeout").
			Description("The timeout for duration of workflow execution. It includes retries and continue as new. Use WorkflowRunTimeout to limit execution time of a single workflow run.").
			Optional(),
		service.NewDurationField("run_timeout").
			Description("The timeout for duration of a single workflow run.").
			Optional(),
		service.NewDurationField("task_timeout").
			Description("The timeout for processing workflow task from the time the worker pulled this task. If a workflow task is lost, it is retried after this timeout.").
			Optional(),
	}
}

type executeConfig struct {
	name             *service.InterpolatedString
	id               *service.InterpolatedString
	idReusePolicy    string
	taskQueue        *service.InterpolatedString
	argsMapping      *bloblang.Executor
	memo             *bloblang.Executor
	searchAttributes *bloblang.Executor
	awaitResult      string

	executionTimeout time.Duration
	runTimeout       time.Duration
	taskTimeout      time.Duration
}

func newExecuteWorkflowConfigFromParsed(conf *service.ParsedConfig) (*executeConfig, error) {
	var err error
	config := &executeConfig{}

	if config.taskQueue, err = conf.FieldInterpolatedString("task_queue"); err != nil {
		return nil, err
	}

	if config.name, err = conf.FieldInterpolatedString("name"); err != nil {
		return nil, err
	}

	if config.awaitResult, err = conf.FieldString("await_result"); err != nil {
		return nil, err
	}

	if conf.Contains("args_mapping") {
		if config.argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("id") {
		if config.id, err = conf.FieldInterpolatedString("id"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("id_reuse_policy") {
		if config.idReusePolicy, err = conf.FieldString("id_reuse_policy"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("memo") {
		if config.memo, err = conf.FieldBloblang("memo"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("search_attributes") {
		if config.searchAttributes, err = conf.FieldBloblang("search_attributes"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("execution_timeout") {
		if config.executionTimeout, err = conf.FieldDuration("execution_timeout"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("run_timeout") {
		if config.runTimeout, err = conf.FieldDuration("run_timeout"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("task_timeout") {
		if config.taskTimeout, err = conf.FieldDuration("task_timeout"); err != nil {
			return nil, err
		}
	}

	return config, nil
}
