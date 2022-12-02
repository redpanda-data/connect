package temporal

import (
	"fmt"
	"math"

	"github.com/benthosdev/benthos/v4/public/service"
	tmprl "go.temporal.io/sdk/temporal"
)

func retryPolicyConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewDurationField("initial_interval").
			Description("Backoff interval for the first retry.If BackoffCoefficient is 1.0 then it is used for all retries. If not set or set to 0, a default interval of 1s will be used.").
			Optional(),
		service.NewFloatField("backoff_coefficient").
			Description("Coefficient used to calculate the next retry backoff interval.").
			Optional(),
		service.NewDurationField("maximum_interval").
			Description("Maximum backoff interval between retries. Exponential backoff leads to interval increase. This value is the cap of the interval. Default is 100x of initial interval.").
			Optional(),
		service.NewIntField("maximum_attempts").
			Description("Maximum number of attempts. When exceeded the retries stop even if not expired yet. If not set or set to 0, it means unlimited, and rely on activity ScheduleToCloseTimeout to stop.").
			Optional(),
		service.NewStringListField("non_retryable_error_types").
			Description("Non-Retriable errors. Temporal server will stop retry if error type matches this list. Note that cancellation is not a failure, so it won't be retried and only StartToClose or Heartbeat timeouts are retryable.").
			Optional(),
	}
}

func newRetryPolicyFromParsed(conf *service.ParsedConfig) (*tmprl.RetryPolicy, error) {
	var err error
	policy := &tmprl.RetryPolicy{}

	if conf.Contains("initial_interval") {
		if policy.InitialInterval, err = conf.FieldDuration("initial_interval"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("backoff_coefficient") {
		if policy.BackoffCoefficient, err = conf.FieldFloat("backoff_coefficient"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("maximum_interval") {
		if policy.MaximumInterval, err = conf.FieldDuration("maximum_interval"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("non_retryable_error_types") {
		if policy.NonRetryableErrorTypes, err = conf.FieldStringList("non_retryable_error_types"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("maximum_attempts") {
		maxAttempts, err := conf.FieldInt("maximum_attempts")
		if err != nil {
			return nil, err
		}
		if maxAttempts > math.MaxInt32 || maxAttempts < math.MinInt32 {
			return nil, fmt.Errorf("retry_policy.maximum_attempts is not a valid 32-bit integer")
		}
		policy.MaximumAttempts = int32(maxAttempts)
	}

	return policy, nil
}
