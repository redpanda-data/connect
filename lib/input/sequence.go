package input

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/OneOfOne/xxhash"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSequence] = TypeSpec{
		constructor: fromSimpleConstructor(NewSequence),
		Summary: `
Reads messages from a sequence of child inputs, starting with the first and once
that input gracefully terminates starts consuming from the next, and so on.`,
		Description: `
This input is useful for consuming from inputs that have an explicit end but
must not be consumed in parallel.`,
		Examples: []docs.AnnotatedExample{
			{
				Title:   "End of Stream Message",
				Summary: "A common use case for sequence might be to generate a message at the end of our main input. With the following config once the records within `./dataset.csv` are exhausted our final payload `{\"status\":\"finished\"}` will be routed through the pipeline.",
				Config: `
input:
  sequence:
    inputs:
      - csv:
          paths: [ ./dataset.csv ]
      - bloblang:
          count: 1
          mapping: 'root = {"status":"finished"}'
`,
			},
			{
				Title: "Joining Fragmented CSV Files",
				Summary: `Benthos can be used to join data from fragmented datasets in memory by specifying a common identifier field and a number of sharded iterations. For example, given two CSV files, the first called "main.csv", which contains rows of user data:

` + "```csv" + `
uuid,name,age
AAA,Melanie,34
BBB,Emma,28
CCC,Geri,45
` + "```" + `

And the second called "hobbies.csv" that, for each user, contains zero or more rows of hobbies:

` + "```csv" + `
uuid,hobby
CCC,pokemon go
AAA,rowing
AAA,golf
` + "```" + `

We can parse and join this data into a single dataset:

` + "```json" + `
{"uuid":"AAA","name":"Melanie","age":34,"hobbies":["rowing","golf"]}
{"uuid":"BBB","name":"Emma","age":28}
{"uuid":"CCC","name":"Geri","age":45,"hobbies":["pokemon go"]}
` + "```" + `

With the following config:`,
				Config: `
input:
  sequence:
    sharded_join:
      iterations: 10
      id_field: uuid
    inputs:
      - csv:
          paths:
            - ./hobbies.csv
            - ./main.csv
`,
			},
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			inputsSanit := make([]interface{}, 0, len(conf.Sequence.Inputs))
			for _, in := range conf.Sequence.Inputs {
				sanit, err := SanitiseConfig(in)
				if err != nil {
					return nil, err
				}
				inputsSanit = append(inputsSanit, sanit)
			}

			shardedJoinBytes, err := yaml.Marshal(conf.Sequence.ShardedJoin)
			if err != nil {
				return nil, err
			}

			shardedJoinMap := map[string]interface{}{}
			if err = yaml.Unmarshal(shardedJoinBytes, &shardedJoinMap); err != nil {
				return nil, err
			}

			return map[string]interface{}{
				"sharded_join": shardedJoinMap,
				"inputs":       inputsSanit,
			}, nil
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced(
				"sharded_join",
				`EXPERIMENTAL: Provides a way to perform sharded joins of structured data resulting from the input sequence. This is a
way to merge the structured fields of fragmented datasets within memory even when the overall size of the data surpasses the memory available on the machine.

When configured the sequence of inputs will be consumed multiple times according to the number of iterations, and each iteration will process an entirely different set of messages by sharding them by the ID field.

Each message must be structured (JSON or otherwise processed into a structured form) and the fields will be aggregated with those of other
messages sharing the ID. At the end of each iteration the joined messages are flushed downstream before the next iteration begins, hence keeping memory usage limited.`,
			).WithChildren(
				docs.FieldCommon("iterations", "The total number of iterations (shards), increasing this number will increase the overall time taken to process the data, but reduces the memory used in the process. A rough estimate for how large this should be is the total size of the data being consumed divided by the amount of available memory, multiplied by a factor of ten in order to provide a safe margin."),
				docs.FieldCommon("id_field", "A common identifier field used to join messages from fragmented datasets. Messages that are not structured or are missing this field will be dropped."),
			),
			docs.FieldCommon("inputs", "An array of inputs to read from sequentially."),
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

// SequenceShardedJoinConfig describes an optional mechanism for performing
// sharded joins of structured data resulting from the input sequence. This is a
// way to merge the structured fields of fragmented datasets within memory even
// when the overall size of the data surpasses the memory available on the
// machine.
//
// When configured the sequence of inputs will be consumed multiple times
// according to the number of iterations, and each iteration will process an
// entirely different set of messages by sharding them by the ID field.
//
// Each message must be structured (JSON or otherwise processed into a
// structured form) and the fields will be aggregated with those of other
// messages sharing the ID. At the end of each iteration the joined messages are
// flushed downstream before the next iteration begins.
type SequenceShardedJoinConfig struct {
	Iterations int    `json:"iterations" yaml:"iterations"`
	IDField    string `json:"id_field" yaml:"id_field"`
}

// NewSequenceShardedJoinConfig creates a new sequence sharding configuration
// with default values.
func NewSequenceShardedJoinConfig() SequenceShardedJoinConfig {
	return SequenceShardedJoinConfig{
		Iterations: 0,
		IDField:    "",
	}
}

func (s SequenceShardedJoinConfig) validate() (*messageJoiner, error) {
	if s.Iterations == 0 {
		return nil, nil
	}
	if s.Iterations < 0 {
		return nil, fmt.Errorf("invalid number of iterations: %v", s.Iterations)
	}
	if len(s.IDField) == 0 {
		return nil, errors.New("the id field must not be empty")
	}
	return &messageJoiner{
		totalIterations: s.Iterations,
		idField:         s.IDField,
		messages:        map[string]*joinedMessage{},
	}, nil
}

// SequenceConfig contains configuration values for the Sequence input type.
type SequenceConfig struct {
	ShardedJoin SequenceShardedJoinConfig `json:"sharded_join" yaml:"sharded_join"`
	Inputs      []Config                  `json:"inputs" yaml:"inputs"`
}

// NewSequenceConfig creates a new SequenceConfig with default values.
func NewSequenceConfig() SequenceConfig {
	return SequenceConfig{
		ShardedJoin: NewSequenceShardedJoinConfig(),
		Inputs:      []Config{},
	}
}

//------------------------------------------------------------------------------

type joinedMessage struct {
	metadata types.Metadata
	fields   *gabs.Container
}

type messageJoiner struct {
	currentIteration int
	totalIterations  int
	idField          string
	messages         map[string]*joinedMessage
}

func (m *messageJoiner) Add(msg types.Message) {
	if m.messages == nil {
		m.messages = map[string]*joinedMessage{}
	}

	msg.Iter(func(i int, p types.Part) error {
		var incomingObj map[string]interface{}
		if jData, err := p.JSON(); err == nil {
			incomingObj, _ = jData.(map[string]interface{})
		}
		if incomingObj == nil {
			// Messages that aren't structured objects are dropped.
			// TODO: Propagate errors?
			return nil
		}

		id, _ := incomingObj[m.idField].(string)
		if len(id) == 0 {
			// TODO: Propagate errors?
			return nil
		}

		// Drop all messages that aren't within our current shard.
		if int(xxhash.ChecksumString64(id)%uint64(m.totalIterations)) != m.currentIteration {
			return nil
		}

		jObj := m.messages[id]
		if jObj == nil {
			jObj = &joinedMessage{
				fields:   gabs.Wrap(incomingObj),
				metadata: p.Metadata().Copy(),
			}
			m.messages[id] = jObj
			return nil
		}

		gIncoming := gabs.Wrap(incomingObj)
		_ = gIncoming.Delete(m.idField)
		_ = jObj.fields.Merge(gIncoming)

		p.Metadata().Iter(func(k, v string) error {
			jObj.metadata.Set(k, v)
			return nil
		})
		return nil
	})
}

func (m *messageJoiner) Empty(fn func(msg types.Message)) bool {
	for k, v := range m.messages {
		part := message.NewPart(nil)
		part.SetJSON(v.fields)
		part.SetMetadata(v.metadata)
		msg := message.New(nil)
		msg.Append(part)
		fn(msg)
		delete(m.messages, k)
	}
	m.currentIteration++
	return m.currentIteration >= m.totalIterations
}

// Sequence is an input type that reads from a sequence of inputs, starting with
// the first, and when it ends gracefully it moves onto the next, and so on.
type Sequence struct {
	conf SequenceConfig

	targetMut sync.Mutex
	target    Type
	remaining []sequenceTarget
	spent     []sequenceTarget

	joiner *messageJoiner

	wrapperMgr   types.Manager
	wrapperLog   log.Modular
	wrapperStats metrics.Type

	stats metrics.Type
	log   log.Modular

	transactions chan types.Transaction

	ctx        context.Context
	closeFn    func()
	closedChan chan struct{}
}

type sequenceTarget struct {
	index  int
	config Config
}

// NewSequence creates a new Sequence input type.
func NewSequence(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if len(conf.Sequence.Inputs) == 0 {
		return nil, errors.New("requires at least one child input")
	}

	targets := make([]sequenceTarget, 0, len(conf.Sequence.Inputs))
	for i, c := range conf.Sequence.Inputs {
		targets = append(targets, sequenceTarget{
			index:  i,
			config: c,
		})
	}

	rdr := &Sequence{
		conf: conf.Sequence,

		remaining: targets,

		wrapperLog:   log,
		wrapperStats: stats,
		wrapperMgr:   mgr,

		log:          log.NewModule(".sequence"),
		stats:        metrics.Namespaced(stats, "sequence"),
		transactions: make(chan types.Transaction),
		closedChan:   make(chan struct{}),
	}
	rdr.ctx, rdr.closeFn = context.WithCancel(context.Background())

	var err error
	if rdr.joiner, err = rdr.conf.ShardedJoin.validate(); err != nil {
		return nil, fmt.Errorf("invalid sharded join config: %w", err)
	}

	if target, err := rdr.createNextTarget(); err != nil {
		return nil, err
	} else if target == nil {
		return nil, errors.New("failed to initialize first input")
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *Sequence) getTarget() Type {
	r.targetMut.Lock()
	target := r.target
	r.targetMut.Unlock()
	return target
}

func (r *Sequence) createNextTarget() (Type, error) {
	var target Type
	var err error

	r.targetMut.Lock()
	r.target = nil
	if len(r.remaining) > 0 {
		if target, err = New(
			r.remaining[0].config,
			r.wrapperMgr,
			r.wrapperLog,
			r.wrapperStats,
		); err == nil {
			r.spent = append(r.spent, r.remaining[0])
			r.remaining = r.remaining[1:]
		} else {
			err = fmt.Errorf("failed to initialize input index %v: %w", r.remaining[0].index, err)
		}
	}
	if target != nil {
		r.target = target
	}
	r.targetMut.Unlock()

	return target, err
}

func (r *Sequence) resetTargets() {
	r.targetMut.Lock()
	r.remaining = r.spent
	r.spent = nil
	r.targetMut.Unlock()
}

func (r *Sequence) dispatchJoinedMessage(wg *sync.WaitGroup, msg types.Message) {
	resChan := make(chan types.Response)
	tran := types.NewTransaction(msg, resChan)
	select {
	case r.transactions <- tran:
	case <-r.ctx.Done():
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case res := <-resChan:
				if res.Error() == nil {
					return
				}
				r.log.Errorf("Failed to send joined message: %v\n", res.Error())
			case <-r.ctx.Done():
				return
			}
			select {
			case <-time.After(time.Second):
			case <-r.ctx.Done():
				return
			}
			select {
			case r.transactions <- tran:
			case <-r.ctx.Done():
				return
			}
		}
	}()
}

func (r *Sequence) loop() {
	defer func() {
		if t := r.getTarget(); t != nil {
			t.CloseAsync()
			err := t.WaitForClose(time.Second)
			for ; err != nil; err = t.WaitForClose(time.Second) {
			}
		}
		close(r.transactions)
		close(r.closedChan)
	}()

	target := r.getTarget()

runLoop:
	for {
		if target == nil {
			var err error
			if target, err = r.createNextTarget(); err != nil {
				r.log.Errorf("Unable to start next sequence: %v\n", err)
				select {
				case <-time.After(time.Second):
				case <-r.ctx.Done():
					return
				}
				continue runLoop
			}
		}
		if target == nil {
			if r.joiner != nil {
				var wg sync.WaitGroup
				lastIteration := r.joiner.Empty(func(msg types.Message) {
					r.dispatchJoinedMessage(&wg, msg)
				})
				wg.Wait()
				if lastIteration {
					r.log.Infoln("Finished sharded iterations and exhausted all sequence inputs, shutting down.")
					return
				}
				r.resetTargets()
				continue runLoop
			} else {
				r.log.Infoln("Exhausted all sequence inputs, shutting down.")
				return
			}
		}

		var tran types.Transaction
		var open bool
		select {
		case tran, open = <-target.TransactionChan():
			if !open {
				target.CloseAsync() // For good measure.
				target = nil
				continue runLoop
			}
		case <-r.ctx.Done():
			return
		}

		if r.joiner != nil {
			r.joiner.Add(tran.Payload.DeepCopy())
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-r.ctx.Done():
				return
			}
		} else {
			select {
			case r.transactions <- tran:
			case <-r.ctx.Done():
				return
			}
		}
	}
}

// TransactionChan returns a transactions channel for consuming messages from
// this input type.
func (r *Sequence) TransactionChan() <-chan types.Transaction {
	return r.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (r *Sequence) Connected() bool {
	if t := r.getTarget(); t != nil {
		return t.Connected()
	}
	return false
}

// CloseAsync shuts down the Sequence input and stops processing requests.
func (r *Sequence) CloseAsync() {
	r.closeFn()
}

// WaitForClose blocks until the Sequence input has closed down.
func (r *Sequence) WaitForClose(timeout time.Duration) error {
	select {
	case <-r.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
