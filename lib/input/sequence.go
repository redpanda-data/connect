package input

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/OneOfOne/xxhash"
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
      - generate:
          count: 1
          mapping: 'root = {"status":"finished"}'
`,
			},
			{
				Title: "Joining Data (Simple)",
				Summary: `Benthos can be used to join unordered data from fragmented datasets in memory by specifying a common identifier field and a number of sharded iterations. For example, given two CSV files, the first called "main.csv", which contains rows of user data:

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
      type: full-outter
      id_path: uuid
      merge_strategy: array
    inputs:
      - csv:
          paths:
            - ./hobbies.csv
            - ./main.csv
`,
			},
			{
				Title: "Joining Data (Advanced)",
				Summary: `In this example we are able to join unordered and fragmented data from a combination of CSV files and newline-delimited JSON documents by specifying multiple sequence inputs with their own processors for extracting the structured data.

The first file "main.csv" contains straight forward CSV data:

` + "```csv" + `
uuid,name,age
AAA,Melanie,34
BBB,Emma,28
CCC,Geri,45
` + "```" + `

And the second file called "hobbies.ndjson" contains JSON documents, one per line, that associate an identifer with an array of hobbies. However, these data objects are in a nested format:

` + "```json" + `
{"document":{"uuid":"CCC","hobbies":[{"type":"pokemon go"}]}}
{"document":{"uuid":"AAA","hobbies":[{"type":"rowing"},{"type":"golf"}]}}
` + "```" + `

And so we will want to map these into a flattened structure before the join, and then we will end up with a single dataset that looks like this:

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
      type: full-outter
      id_path: uuid
      iterations: 10
      merge_strategy: array
    inputs:
      - csv:
          paths: [ ./main.csv ]
      - file:
          codec: lines
          paths: [ ./hobbies.ndjson ]
        processors:
          - bloblang: |
              root.uuid = this.document.uuid
              root.hobbies = this.document.hobbies.map_each(this.type)
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced(
				"sharded_join",
				`EXPERIMENTAL: Provides a way to perform outter joins of arbitrarily structured and unordered data resulting from the input sequence, even when the overall size of the data surpasses the memory available on the machine.

When configured the sequence of inputs will be consumed one or more times according to the number of iterations, and when more than one iteration is specified each iteration will process an entirely different set of messages by sharding them by the ID field. Increasing the number of iterations reduces the memory consumption at the cost of needing to fully parse the data each time.

Each message must be structured (JSON or otherwise processed into a structured form) and the fields will be aggregated with those of other messages sharing the ID. At the end of each iteration the joined messages are flushed downstream before the next iteration begins, hence keeping memory usage limited.`,
			).WithChildren(
				docs.FieldCommon("type", "The type of join to perform. A `full-outter` ensures that all identifiers seen in any of the input sequences are sent, and is performed by consuming all input sequences before flushing the joined results. An `outter` join consumes all input sequences but only writes data joined from the last input in the sequence, similar to a left or right outter join. With an `outter` join if an identifier appears multiple times within the final sequence input it will be flushed each time it appears.").HasOptions("none", "full-outter", "outter"),
				docs.FieldCommon("id_path", "A [dot path](/docs/configuration/field_paths) that points to a common field within messages of each fragmented data set and can be used to join them. Messages that are not structured or are missing this field will be dropped. This field must be set in order to enable joins."),
				docs.FieldCommon("iterations", "The total number of iterations (shards), increasing this number will increase the overall time taken to process the data, but reduces the memory used in the process. The real memory usage required is significantly higher than the real size of the data and therefore the number of iterations should be at least an order of magnitude higher than the available memory divided by the overall size of the dataset."),
				docs.FieldCommon(
					"merge_strategy",
					"The chosen strategy to use when a data join would otherwise result in a collision of field values. The strategy `array` means non-array colliding values are placed into an array and colliding arrays are merged. The strategy `replace` replaces old values with new values. The strategy `keep` keeps the old value.",
				).HasOptions("array", "replace", "keep"),
			).AtVersion("3.40.0"),
			docs.FieldCommon("inputs", "An array of inputs to read from sequentially.").Array().HasType(docs.FieldTypeInput),
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
	Type          string `json:"type" yaml:"type"`
	IDPath        string `json:"id_path" yaml:"id_path"`
	Iterations    int    `json:"iterations" yaml:"iterations"`
	MergeStrategy string `json:"merge_strategy" yaml:"merge_strategy"`
}

// NewSequenceShardedJoinConfig creates a new sequence sharding configuration
// with default values.
func NewSequenceShardedJoinConfig() SequenceShardedJoinConfig {
	return SequenceShardedJoinConfig{
		Type:          "none",
		IDPath:        "",
		Iterations:    1,
		MergeStrategy: "array",
	}
}

func (s SequenceShardedJoinConfig) validate() (*messageJoiner, error) {
	var flushOnLast bool
	switch s.Type {
	case "none":
		return nil, nil
	case "full-outter":
		flushOnLast = false
	case "outter":
		flushOnLast = true
	default:
		return nil, fmt.Errorf("join type '%v' was not recognized", s.Type)
	}
	if s.IDPath == "" {
		return nil, errors.New("the id path must not be empty")
	}
	if s.Iterations <= 0 {
		return nil, fmt.Errorf("invalid number of iterations: %v", s.Iterations)
	}
	collisionFn, err := getMessageJoinerCollisionFn(s.MergeStrategy)
	if err != nil {
		return nil, err
	}
	return &messageJoiner{
		totalIterations: s.Iterations,
		idPath:          s.IDPath,
		messages:        map[string]*joinedMessage{},
		collisionFn:     collisionFn,
		flushOnLast:     flushOnLast,
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
	metadata map[string]string
	fields   *gabs.Container
}

func (j *joinedMessage) ToMsg() *message.Batch {
	part := message.NewPart(nil)
	part.SetJSON(j.fields)
	for k, v := range j.metadata {
		part.MetaSet(k, v)
	}
	msg := message.QuickBatch(nil)
	msg.Append(part)
	return msg
}

type messageJoinerCollisionFn func(dest, source interface{}) interface{}

func getMessageJoinerCollisionFn(name string) (messageJoinerCollisionFn, error) {
	switch name {
	case "array":
		return func(dest, source interface{}) interface{} {
			destArr, destIsArray := dest.([]interface{})
			sourceArr, sourceIsArray := source.([]interface{})
			if destIsArray {
				if sourceIsArray {
					return append(destArr, sourceArr...)
				}
				return append(destArr, source)
			}
			if sourceIsArray {
				return append(append([]interface{}{}, dest), sourceArr...)
			}
			return []interface{}{dest, source}
		}, nil
	case "replace":
		return func(dest, source interface{}) interface{} {
			return source
		}, nil
	case "keep":
		return func(dest, source interface{}) interface{} {
			return dest
		}, nil
	}
	return nil, fmt.Errorf("merge strategy '%v' was not recognised", name)
}

type messageJoiner struct {
	currentIteration int
	totalIterations  int
	idPath           string
	messages         map[string]*joinedMessage
	collisionFn      messageJoinerCollisionFn
	flushOnLast      bool
}

func (m *messageJoiner) Add(msg *message.Batch, lastInSequence bool, fn func(msg *message.Batch)) {
	if m.messages == nil {
		m.messages = map[string]*joinedMessage{}
	}

	_ = msg.Iter(func(i int, p *message.Part) error {
		var incomingObj map[string]interface{}
		if jData, err := p.JSON(); err == nil {
			incomingObj, _ = jData.(map[string]interface{})
		}
		if incomingObj == nil {
			// Messages that aren't structured objects are dropped.
			// TODO: Propagate errors?
			return nil
		}

		gIncoming := gabs.Wrap(incomingObj)
		id, _ := gIncoming.Path(m.idPath).Data().(string)
		if id == "" {
			// TODO: Propagate errors?
			return nil
		}

		// Drop all messages that aren't within our current shard.
		if int(xxhash.ChecksumString64(id)%uint64(m.totalIterations)) != m.currentIteration {
			return nil
		}

		meta := map[string]string{}
		_ = p.MetaIter(func(k, v string) error {
			meta[k] = v
			return nil
		})

		jObj := m.messages[id]
		if jObj == nil {
			jObj = &joinedMessage{
				fields:   gIncoming,
				metadata: meta,
			}
			m.messages[id] = jObj

			if m.flushOnLast && lastInSequence {
				fn(jObj.ToMsg().DeepCopy())
			}
			return nil
		}

		_ = gIncoming.Delete(m.idPath)
		_ = jObj.fields.MergeFn(gIncoming, m.collisionFn)

		_ = p.MetaIter(func(k, v string) error {
			jObj.metadata[k] = v
			return nil
		})

		if m.flushOnLast && lastInSequence {
			fn(jObj.ToMsg().DeepCopy())
		}
		return nil
	})
}

func (m *messageJoiner) GetIteration() (int, bool) {
	return m.currentIteration, m.currentIteration == (m.totalIterations - 1)
}

func (m *messageJoiner) Empty(fn func(*message.Batch)) bool {
	for k, v := range m.messages {
		if !m.flushOnLast {
			msg := v.ToMsg()
			fn(msg)
		}
		delete(m.messages, k)
	}
	m.currentIteration++
	return m.currentIteration >= m.totalIterations
}

//------------------------------------------------------------------------------

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

	shutSig *shutdown.Signaller
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

	_, rLog, rStats := interop.LabelChild("sequence", mgr, log, stats)
	rdr := &Sequence{
		conf: conf.Sequence,

		remaining: targets,

		wrapperLog:   log,
		wrapperStats: stats,
		wrapperMgr:   mgr,

		log:          rLog,
		stats:        rStats,
		transactions: make(chan types.Transaction),
		shutSig:      shutdown.NewSignaller(),
	}

	var err error
	if rdr.joiner, err = rdr.conf.ShardedJoin.validate(); err != nil {
		return nil, fmt.Errorf("invalid sharded join config: %w", err)
	}

	if target, _, err := rdr.createNextTarget(); err != nil {
		return nil, err
	} else if target == nil {
		return nil, errors.New("failed to initialize first input")
	}

	go rdr.loop()
	return rdr, nil
}

//------------------------------------------------------------------------------

func (r *Sequence) getTarget() (Type, bool) {
	r.targetMut.Lock()
	target := r.target
	final := len(r.remaining) == 0
	r.targetMut.Unlock()
	return target, final
}

func (r *Sequence) createNextTarget() (Type, bool, error) {
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
		r.log.Debugf("Initialized sequence input %v.", len(r.spent)-1)
		r.target = target
	}
	final := len(r.remaining) == 0
	r.targetMut.Unlock()

	return target, final, err
}

func (r *Sequence) resetTargets() {
	r.targetMut.Lock()
	r.remaining = r.spent
	r.spent = nil
	r.targetMut.Unlock()
}

func (r *Sequence) dispatchJoinedMessage(wg *sync.WaitGroup, msg *message.Batch) {
	resChan := make(chan types.Response)
	tran := types.NewTransaction(msg, resChan)
	select {
	case r.transactions <- tran:
	case <-r.shutSig.CloseNowChan():
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case res := <-resChan:
				if res.AckError() == nil {
					return
				}
				r.log.Errorf("Failed to send joined message: %v\n", res.AckError())
			case <-r.shutSig.CloseNowChan():
				return
			}
			select {
			case <-time.After(time.Second):
			case <-r.shutSig.CloseNowChan():
				return
			}
			select {
			case r.transactions <- tran:
			case <-r.shutSig.CloseNowChan():
				return
			}
		}
	}()
}

func (r *Sequence) loop() {
	var shardJoinWG sync.WaitGroup
	defer func() {
		shardJoinWG.Wait()
		if t, _ := r.getTarget(); t != nil {
			t.CloseAsync()
			go func() {
				select {
				case <-r.shutSig.CloseNowChan():
					_ = t.WaitForClose(0)
				case <-r.shutSig.HasClosedChan():
				}
			}()
			_ = t.WaitForClose(shutdown.MaximumShutdownWait())
		}
		close(r.transactions)
		r.shutSig.ShutdownComplete()
	}()

	target, finalInSequence := r.getTarget()

runLoop:
	for {
		if target == nil {
			var err error
			if target, finalInSequence, err = r.createNextTarget(); err != nil {
				r.log.Errorf("Unable to start next sequence: %v\n", err)
				select {
				case <-time.After(time.Second):
				case <-r.shutSig.CloseAtLeisureChan():
					return
				}
				continue runLoop
			}
		}
		if target == nil {
			if r.joiner != nil {
				iteration, _ := r.joiner.GetIteration()
				r.log.Debugf("Finished sharded iteration %v.", iteration)

				// Wait for pending transactions before adding more.
				shardJoinWG.Wait()

				lastIteration := r.joiner.Empty(func(msg *message.Batch) {
					r.dispatchJoinedMessage(&shardJoinWG, msg)
				})
				shardJoinWG.Wait()
				if lastIteration {
					r.log.Infoln("Finished all sharded iterations and exhausted all sequence inputs, shutting down.")
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
		case <-r.shutSig.CloseAtLeisureChan():
			return
		}

		if r.joiner != nil {
			r.joiner.Add(tran.Payload.DeepCopy(), finalInSequence, func(msg *message.Batch) {
				r.dispatchJoinedMessage(&shardJoinWG, msg)
			})
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-r.shutSig.CloseNowChan():
				return
			}
		} else {
			select {
			case r.transactions <- tran:
			case <-r.shutSig.CloseNowChan():
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
	if t, _ := r.getTarget(); t != nil {
		return t.Connected()
	}
	return false
}

// CloseAsync shuts down the Sequence input and stops processing requests.
func (r *Sequence) CloseAsync() {
	r.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the Sequence input has closed down.
func (r *Sequence) WaitForClose(timeout time.Duration) error {
	go func() {
		if tAfter := timeout - time.Second; tAfter > 0 {
			<-time.After(timeout - time.Second)
		}
		r.shutSig.CloseNow()
	}()
	select {
	case <-r.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
