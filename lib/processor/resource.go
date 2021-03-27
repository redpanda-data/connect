package processor

import (
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeResource] = TypeSpec{
		constructor: NewResource,
		Categories: []Category{
			CategoryUtility,
		},
		Summary: `
Resource is a processor type that runs a processor resource identified by its label.`,
		Description: `
This processor allows you to reference the same configured processor resource in multiple places, and can also tidy up large nested configs. For example, the config:

` + "```yaml" + `
pipeline:
  processors:
    - bloblang: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()
` + "```" + `

Is equivalent to:

` + "``` yaml" + `
pipeline:
  processors:
    - resource: foo_proc

processor_resources:
  - label: foo_proc
    bloblang: |
      root.message = this
      root.meta.link_count = this.links.length()
      root.user.age = this.user.age.number()
` + "```" + `

You can find out more about resources [in this document.](/docs/configuration/resources)`,
	}
}

//------------------------------------------------------------------------------

type procProvider interface {
	GetProcessor(name string) (types.Processor, error)
}

//------------------------------------------------------------------------------

// Resource is a processor that returns the result of a processor resource.
type Resource struct {
	mgr  procProvider
	name string
	log  log.Modular

	mCount       metrics.StatCounter
	mErr         metrics.StatCounter
	mErrNotFound metrics.StatCounter
}

// NewResource returns a resource processor.
func NewResource(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	// TODO: V4 Remove this
	procProvider, ok := mgr.(procProvider)
	if !ok {
		return nil, errors.New("manager does not support processor resources")
	}

	if _, err := procProvider.GetProcessor(conf.Resource); err != nil {
		return nil, fmt.Errorf("failed to obtain processor resource '%v': %v", conf.Resource, err)
	}
	return &Resource{
		mgr:  procProvider,
		name: conf.Resource,
		log:  log,

		mCount:       stats.GetCounter("count"),
		mErrNotFound: stats.GetCounter("error_not_found"),
		mErr:         stats.GetCounter("error"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (r *Resource) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	proc, err := r.mgr.GetProcessor(r.name)
	r.mCount.Incr(1)
	if err != nil {
		r.log.Debugf("Failed to obtain processor resource '%v': %v", r.name, err)
		r.mErrNotFound.Incr(1)
		r.mErr.Incr(1)
		return nil, response.NewError(err)
	}
	return proc.ProcessMessage(msg)
}

// CloseAsync shuts down the processor and stops processing requests.
func (r *Resource) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (r *Resource) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
