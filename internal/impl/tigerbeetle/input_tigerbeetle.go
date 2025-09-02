package tigerbeetle

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/Jeffail/shutdown"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/service"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	tb_types "github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

const (
	fieldClusterID        = "cluster_id"
	fieldAddresses        = "addresses"
	fieldProgressCache    = "progress_cache"
	fieldEventCountMax    = "event_count_max"
	fieldIdleInterval     = "idle_interval_ms"
	fieldTimestampInitial = "timestamp_initial"

	idleIntervalDefault = 1000
	eventCountDefault   = 2730
	shutdownTimeout     = 5 * time.Second
)

func configSpec() *service.ConfigSpec {
	jsonSampleObject, err := json.MarshalIndent(JsonChangeEvent{
		Timestamp: "1745328372758695656",
		Type:      "single_phase",
		Ledger:    2,
		Transfer: JsonTransfer{
			ID:          "9082709",
			Amount:      "3794",
			PendingID:   "0",
			UserData128: "79248595801719937611592367840129079151",
			UserData64:  "13615171707598273871",
			UserData32:  3229992513,
			Timeout:     0,
			Code:        20295,
			Flags:       0,
			Timestamp:   "1745328372758695656",
		},
		DebitAccount: JsonAccount{
			ID:             "3750",
			DebitsPending:  "0",
			DebitsPosted:   "8463768",
			CreditsPending: "0",
			CreditsPosted:  "8861179",
			UserData128:    "118966247877720884212341541320399553321",
			UserData64:     "526432537153007844",
			UserData32:     4157247332,
			Code:           1,
			Flags:          0,
			Timestamp:      "1745328270103398016",
		},
		CreditAccount: JsonAccount{
			ID:             "6765",
			DebitsPending:  "0",
			DebitsPosted:   "8669204",
			CreditsPending: "0",
			CreditsPosted:  "8637251",
			UserData128:    "43670023860556310170878798978091998141",
			UserData64:     "12485093662256535374",
			UserData32:     1924162092,
			Code:           1,
			Flags:          0,
			Timestamp:      "1745328270103401031",
		},
	}, "", "  ")
	if err != nil {
		panic("assertion failed: cannot marshal JSON object")
	}

	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("0.0.1").
		Summary("Enables TigerBeetle CDC streaming for RedPanda Connect.").
		Description(`Listens to a TigerBeetle cluster and creates a message for each change.

Each message is a JSON object like:

`+fmt.Sprintf("```json\n%s\n```", string(jsonSampleObject))+`

For more information refer to https://docs.tigerbeetle.com/operating/cdc/

== Metadata

This input adds the following metadata fields to each message:

- event_type: One of "single_phase", "two_phase_pending", "two_phase_posted", "two_phase_voided", or "two_phase_expired".
- ledger: The ledger code.
- transfer_code: The transfer code.
- debit_account_code: The debit account code.
- credit_account_code: The credit account code.
- timestamp: The unique event timestamp with nanosecond resolution.
- timestamp_ms: The event timestamp with millisecond resolution.

== Guarantees

This input guarantees _at-least-once semantics_, and makes a best effort to prevent
duplicate messages. However, during crash recovery, it may replay unacknowledged
messages that could have been already delivered to consumers.

It is the consumer’s responsibility to perform idempotency checks when processing messages.

== Upgrading

The TigerBeetle client version must not be newer than the cluster version, as it will fail
with an error message if so.

Requires TigerBeetle cluster version 0.16.57 or greater.`).
		Fields(
			service.NewStringField(fieldClusterID).
				Description("The TigerBeetle unique 128-bit cluster ID.").
				LintRule(`root = if !this.re_match("^[0-9]+$") {
						[ "field '`+fieldClusterID+`' must be a valid integer" ]
					}`),
			service.NewStringListField(fieldAddresses).
				Description("A list of IP addresses of all the TigerBeetle replicas in the cluster. "+
					"The order of addresses must correspond to the order of replicas.").
				LintRule(`root = if this.length() == 0 {
				 		[ "field '`+fieldAddresses+`' must contain at least one address" ]
					}`),
			service.NewStringField(fieldProgressCache).
				Description("A https://docs.redpanda.com/redpanda-connect/components/caches/about[cache resource^] "+
					"used to track progress by storing the last acknowledged timestamp.\n"+
					"This allows Redpanda Connect to resume from the latest delivered event "+
					"upon restart."),
			service.NewIntField(fieldEventCountMax).
				Description("The maximum number of events fetched from TigerBeetle per batch.\n"+
					"Must be greater than zero.").
				Default(eventCountDefault).
				LintRule(`root = if this <= 0 {
						[ "field '`+fieldEventCountMax+`' must be greater than 0" ]
					}`),
			service.NewIntField(fieldIdleInterval).
				Description("The time interval in milliseconds to wait before querying again when "+
					"the last query returned no events.\n"+
					"Must be greater than zero.").
				Default(idleIntervalDefault).
				LintRule(`root = if this <= 0 {
						[ "field '`+fieldIdleInterval+`' must be greater than 0" ]
					}`),
			service.NewStringField(fieldTimestampInitial).
				Description("The initial timestamp to start extracting events from. "+
					"If not defined, all events since the beginning will be included.\n"+
					"Ignored if a more recent timestamp has already been acknowledged.\n"+
					"This is a TigerBeetle timestamp with nanosecond precision.").
				Default("").
				LintRule(`root = if this.length() > 0 && !this.re_match("^[0-9]+$") {
						[ "field '`+fieldTimestampInitial+`' must be a valid integer" ]
					}`),
			service.NewAutoRetryNacksToggleField(),
		)
}

type tigerbeetleConfig struct {
	clusterID        tb_types.Uint128
	addresses        []string
	eventCountMax    uint32
	idleInterval     time.Duration
	timestampInitial uint64
	progressCache    string
	timestampLastKey string
}

type tigerbeetleInput struct {
	config tigerbeetleConfig

	producerChan chan []tb_types.ChangeEvent
	consumerChan chan batchedMesssage

	stopSignaller *shutdown.Signaller
	logger        *service.Logger
	resources     *service.Resources
}

type batchedMesssage struct {
	batch   []*service.Message
	ackFunc service.AckFunc
}

func init() {
	service.MustRegisterBatchInput("tigerbeetle", configSpec(), newTigerbeetleInput)
}

func (input *tigerbeetleInput) Connect(ctx context.Context) error {
	timestampLast, err := input.getTimestampLast(ctx)
	if err != nil {
		input.logger.Errorf("Could not retrieve the last timestamp from cache: %w", err)
		return err
	}
	// Overriding the timestamp with the configured initial value:
	if input.config.timestampInitial > timestampLast {
		timestampLast = input.config.timestampInitial - 1 // Inclusive range.
	}

	client, err := tb.NewClient(input.config.clusterID, input.config.addresses)
	if err != nil {
		input.logger.Errorf("Could not initialize the TigerBeetle client: %w", err)
		return err
	}

	input.stopSignaller = shutdown.NewSignaller()
	go func() {
		ctx, _ := input.stopSignaller.SoftStopCtx(context.Background())
		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error { return input.produce(ctx, client, timestampLast) })
		wg.Go(func() error { return input.consume(ctx) })

		if err := wg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			input.logger.Errorf("Error during TigerBeetle CDC: %w", err)
		} else {
			input.logger.Info("Successfully shutdown TigerBeetle CDC stream")
		}
		input.stopSignaller.TriggerHasStopped()
	}()
	return nil
}

func (input *tigerbeetleInput) Close(ctx context.Context) error {
	if input.stopSignaller == nil {
		// Never connected.
		return nil
	}
	input.stopSignaller.TriggerSoftStop()
	select {
	case <-ctx.Done():
	case <-time.After(shutdownTimeout):
		input.stopSignaller.TriggerHardStop()
	case <-input.stopSignaller.HasStoppedChan():
	}

	select {
	case <-ctx.Done():
	case <-input.stopSignaller.HasStoppedChan():
	case <-time.After(shutdownTimeout):
		input.logger.Error("Failed to shut down TigerBeetle CDC within the timeout")
	}
	return nil
}

func (input *tigerbeetleInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case batchedMessage := <-input.consumerChan:
		return batchedMessage.batch, batchedMessage.ackFunc, nil
	case <-input.stopSignaller.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}

func newTigerbeetleInput(config *service.ParsedConfig, resources *service.Resources) (s service.BatchInput, err error) {
	var (
		clusterID           string
		addresses           []string
		eventCountMax       int
		idleInterval        int
		timestampInitialStr string
		timestampInitial    uint64 = 0
		progressCache       string
	)

	logger := resources.Logger()

	if clusterID, err = config.FieldString(fieldClusterID); err != nil {
		return nil, err
	}
	clusterID128, success := StringToUint128(clusterID)
	if !success {
		return nil, fmt.Errorf("invalid config: %s='%s'", fieldClusterID, clusterID)
	}

	if addresses, err = config.FieldStringList(fieldAddresses); err != nil {
		return nil, err
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("invalid config: %s is empty", fieldAddresses)
	}

	if progressCache, err = config.FieldString(fieldProgressCache); err != nil {
		return nil, err
	}
	if !config.Resources().HasCache(progressCache) {
		return nil, fmt.Errorf("cache resource '%s' not found", progressCache)
	}

	if eventCountMax, err = config.FieldInt(fieldEventCountMax); err != nil {
		return nil, err
	} else if eventCountMax <= 0 {
		return nil, fmt.Errorf("property '%s' must be greater than zero", fieldEventCountMax)
	}

	if idleInterval, err = config.FieldInt(fieldIdleInterval); err != nil {
		return nil, err
	} else if idleInterval <= 0 {
		return nil, fmt.Errorf("property '%s' must be greater than zero", fieldIdleInterval)
	}

	if timestampInitialStr, err = config.FieldString(fieldTimestampInitial); err != nil {
		return nil, err
	} else if len(timestampInitialStr) != 0 {
		if timestampInitial, err = strconv.ParseUint(timestampInitialStr, 10, 64); err != nil {
			return nil, fmt.Errorf("invalid config: %s='%s'", fieldTimestampInitial, timestampInitialStr)
		}
	}

	input := &tigerbeetleInput{
		config: tigerbeetleConfig{
			clusterID:        clusterID128,
			addresses:        addresses,
			progressCache:    progressCache,
			timestampLastKey: "timestamp_last_" + clusterID,
			eventCountMax:    uint32(eventCountMax),
			idleInterval:     time.Duration(idleInterval) * time.Millisecond,
			timestampInitial: timestampInitial,
		},
		producerChan: make(chan []tb_types.ChangeEvent, 1),
		consumerChan: make(chan batchedMesssage, 1),
		logger:       logger,
		resources:    resources,
	}

	return service.AutoRetryNacksBatchedToggled(config, input)
}

// Extracts events from TigerBeetle.
func (input *tigerbeetleInput) produce(ctx context.Context, client tb.Client, timestampLast uint64) error {
	// Asynchronously closes the client,
	// forcing any inflight request to finish in case of hard stop.
	go func() {
		select {
		case <-input.stopSignaller.HasStoppedChan(): // Graceful shutdown.
		case <-input.stopSignaller.HardStopChan(): // Hard stop.
		}
		client.Close()
	}()

	idleTimer := time.NewTimer(0)
	_ = idleTimer.Stop()

	for {
		input.logger.Debugf("producer: get_change_events: timestamp_min=%d limit=%d",
			timestampLast+1,
			input.config.eventCountMax,
		)

		results, err := client.GetChangeEvents(tb_types.ChangeEventsFilter{
			TimestampMin: timestampLast + 1,
			TimestampMax: 0,
			Limit:        input.config.eventCountMax,
		})
		if err != nil {
			return err
		}

		input.logger.Debugf("producer: get_change_events: %d results", len(results))

		// No events returned from the query,
		// waiting for the timeout to resume the producer.
		if len(results) == 0 {
			// NB: We could go idle if `len(results) < eventCountMax`, since the client
			// likely won’t return new results if queried again immediately.
			// However, we wait for the *consumer* to begin flushing the current results
			// before issuing a new query, avoiding unnecessary idle time for workloads
			// with high frequency but low volume per batch.
			rescheduled := idleTimer.Reset(input.config.idleInterval)
			if rescheduled {
				return errors.New("assertion failed: idle timer was already running")
			}

			input.logger.Debugf("producer: idle: %d ms", input.config.idleInterval.Milliseconds())

			select {
			case <-idleTimer.C:
				continue
			case <-ctx.Done():
				_ = idleTimer.Stop()
				return ctx.Err()
			}
		}

		// Waits until the consumer flushes the results or the job is stopped.
		select {
		case input.producerChan <- results:
			timestampLast = results[len(results)-1].Timestamp
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Flushes the events into the pipeline.
func (input *tigerbeetleInput) consume(ctx context.Context) error {
	// We must keep events ordered,
	// the next batch can only be flushed when the current one has been acknowledged.
	batch := make([]*service.Message, 0, input.config.eventCountMax)
	ackChan := make(chan struct{}, 1)
	for {
		select {
		case results := <-input.producerChan:
			if len(results) == 0 {
				return errors.New("assertion failed: unexpected empty results")
			} else if len(results) > int(input.config.eventCountMax) {
				return errors.New("assertion failed: too many results")
			} else if len(batch) != 0 {
				return errors.New("assertion failed: pending messages to flush")
			}

			for _, result := range results {
				bytes, err := jsonSerialize(result)
				if err != nil {
					input.logger.Errorf("Unable to serialize as JSON: %w", err)
					return err
				}
				message := service.NewMessage(bytes)
				message.MetaSet("timestamp", strconv.FormatUint(result.Timestamp, 10))
				message.MetaSet("timestamp_ms", strconv.FormatUint(result.Timestamp/uint64(time.Millisecond), 10))
				message.MetaSet("event_type", eventTypeString(result.Type))
				message.MetaSet("ledger", strconv.FormatUint(uint64(result.Ledger), 10))
				message.MetaSet("transfer_code", strconv.FormatUint(uint64(result.TransferCode), 10))
				message.MetaSet("debit_account_code", strconv.FormatUint(uint64(result.DebitAccountCode), 10))
				message.MetaSet("credit_account_code", strconv.FormatUint(uint64(result.CreditAccountCode), 10))

				batch = append(batch, message)
			}

			timestampLast := results[len(results)-1].Timestamp
			batchedMessage := batchedMesssage{
				batch: batch,
				ackFunc: func(ctx context.Context, _ error) error {
					if err := input.setTimestampLast(ctx, timestampLast); err != nil {
						return err
					}
					// Signals the batch was acknowledged.
					ackChan <- struct{}{}
					return nil
				},
			}

			input.logger.Debugf("consumer: flush: %d events", len(results))

			// Waits until the batch is flushed and acknowledged or
			// the job was aborted by `TriggerHardStop()`.
			select {
			case input.consumerChan <- batchedMessage:
				select {
				case <-ackChan:
					// Resets the buffer for the next iteration.
					batch = batch[:0]
					input.logger.Debugf("consumer: flush: ack: timestampLast=%d", timestampLast)
					continue
				case <-input.stopSignaller.HardStopChan():
					break
				}
			case <-input.stopSignaller.HardStopChan():
				break
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// JsonChangeEvent represents the structure of a CDC event as serialized to JSON.
type JsonChangeEvent struct {
	Timestamp     string       `json:"timestamp"`
	Type          string       `json:"type"`
	Ledger        uint32       `json:"ledger"`
	Transfer      JsonTransfer `json:"transfer"`
	DebitAccount  JsonAccount  `json:"debit_account"`
	CreditAccount JsonAccount  `json:"credit_account"`
}

// JsonTransfer represents the structure of a CDC transfer event as serialized to JSON.
type JsonTransfer struct {
	ID          string `json:"id"`
	Amount      string `json:"amount"`
	PendingID   string `json:"pending_id"`
	UserData128 string `json:"user_data_128"`
	UserData64  string `json:"user_data_64"`
	UserData32  uint32 `json:"user_data_32"`
	Timeout     uint32 `json:"timeout"`
	Code        uint16 `json:"code"`
	Flags       uint16 `json:"flags"`
	Timestamp   string `json:"timestamp"`
}

// JsonAccount represents the structure of a CDC account event as serialized to JSON.
type JsonAccount struct {
	ID             string `json:"id"`
	DebitsPending  string `json:"debits_pending"`
	DebitsPosted   string `json:"debits_posted"`
	CreditsPending string `json:"credits_pending"`
	CreditsPosted  string `json:"credits_posted"`
	UserData128    string `json:"user_data_128"`
	UserData64     string `json:"user_data_64"`
	UserData32     uint32 `json:"user_data_32"`
	Code           uint16 `json:"code"`
	Flags          uint16 `json:"flags"`
	Timestamp      string `json:"timestamp"`
}

func jsonSerialize(result tb_types.ChangeEvent) ([]byte, error) {
	return json.Marshal(JsonChangeEvent{
		Timestamp: strconv.FormatUint(result.Timestamp, 10),
		Type:      eventTypeString(result.Type),
		Ledger:    result.Ledger,
		Transfer: JsonTransfer{
			ID:          Uint128ToString(result.TransferID),
			Amount:      Uint128ToString(result.TransferAmount),
			PendingID:   Uint128ToString(result.TransferPendingID),
			UserData128: Uint128ToString(result.TransferUserData128),
			UserData64:  strconv.FormatUint(result.TransferUserData64, 10),
			UserData32:  result.TransferUserData32,
			Timeout:     result.TransferTimeout,
			Code:        result.TransferCode,
			Flags:       result.TransferFlags,
			Timestamp:   strconv.FormatUint(result.TransferTimestamp, 10),
		},
		DebitAccount: JsonAccount{
			ID:             Uint128ToString(result.DebitAccountID),
			DebitsPending:  Uint128ToString(result.DebitAccountDebitsPending),
			DebitsPosted:   Uint128ToString(result.DebitAccountDebitsPosted),
			CreditsPending: Uint128ToString(result.DebitAccountCreditsPending),
			CreditsPosted:  Uint128ToString(result.DebitAccountCreditsPosted),
			UserData128:    Uint128ToString(result.DebitAccountUserData128),
			UserData64:     strconv.FormatUint(result.DebitAccountUserData64, 10),
			UserData32:     result.DebitAccountUserData32,
			Code:           result.DebitAccountCode,
			Flags:          result.DebitAccountFlags,
			Timestamp:      strconv.FormatUint(result.DebitAccountTimestamp, 10),
		},
		CreditAccount: JsonAccount{
			ID:             Uint128ToString(result.CreditAccountID),
			DebitsPending:  Uint128ToString(result.CreditAccountDebitsPending),
			DebitsPosted:   Uint128ToString(result.CreditAccountDebitsPosted),
			CreditsPending: Uint128ToString(result.CreditAccountCreditsPending),
			CreditsPosted:  Uint128ToString(result.CreditAccountCreditsPosted),
			UserData128:    Uint128ToString(result.CreditAccountUserData128),
			UserData64:     strconv.FormatUint(result.CreditAccountUserData64, 10),
			UserData32:     result.CreditAccountUserData32,
			Code:           result.CreditAccountCode,
			Flags:          result.CreditAccountFlags,
			Timestamp:      strconv.FormatUint(result.CreditAccountTimestamp, 10),
		},
	})
}

// StringToUint128 parses a base 10 string and returns the corresponding value as a Uint128.
func StringToUint128(str string) (tb_types.Uint128, bool) {
	if len(str) == 0 {
		return tb_types.Uint128{}, false
	}
	bigInt := new(big.Int)
	_, success := bigInt.SetString(str, 10)
	if !success {
		return tb_types.Uint128{}, false
	}
	return tb_types.BigIntToUint128(*bigInt), true
}

// Uint128ToString formats a Uint128 number as a base10 string.
func Uint128ToString(value tb_types.Uint128) string {
	bigInt := value.BigInt()
	return bigInt.Text(10)
}

func eventTypeString(value tb_types.ChangeEventType) string {
	switch value {
	case tb_types.ChangeEventSinglePhase:
		return "single_phase"
	case tb_types.ChangeEventTwoPhasePending:
		return "two_phase_pending"
	case tb_types.ChangeEventTwoPhasePosted:
		return "two_phase_posted"
	case tb_types.ChangeEventTwoPhaseVoided:
		return "two_phase_voided"
	case tb_types.ChangeEventTwoPhaseExpired:
		return "two_phase_expired"
	default:
		panic("unexpected event type")
	}
}

// To make the CDC stateless, a cache is used to store the state:
// During publishing, an entry containing the last timestamp is added into this cache at
// the end of each published batch.
// On restart, the presence of this entry indicates the `timestamp_min` from which to resume
// processing events. Otherwise, processing starts from the beginning.
// The cache `key` is generated to be unique based on the `cluster_id`.

func (input *tigerbeetleInput) getTimestampLast(ctx context.Context) (uint64, error) {
	var (
		cacheVal []byte
		cErr     error
	)
	if err := input.resources.AccessCache(ctx, input.config.progressCache, func(c service.Cache) {
		cacheVal, cErr = c.Get(ctx, input.config.timestampLastKey)
	}); err != nil {
		return 0, fmt.Errorf("unable to access cache for reading: %w", err)
	}

	if errors.Is(cErr, service.ErrKeyNotFound) {
		return 0, nil
	} else if cErr != nil {
		return 0, fmt.Errorf("unable read timestamp last from cache: %w", cErr)
	} else if cacheVal == nil {
		return 0, nil
	} else if len(cacheVal) != 8 {
		return 0, fmt.Errorf("invalid timestamp last from cache: len=%d", len(cacheVal))
	}

	return binary.LittleEndian.Uint64(cacheVal), nil
}

func (input *tigerbeetleInput) setTimestampLast(ctx context.Context, timestamp uint64) error {
	var cErr error
	if err := input.resources.AccessCache(ctx, input.config.progressCache, func(c service.Cache) {
		bytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, timestamp)
		cErr = c.Set(
			ctx,
			input.config.timestampLastKey,
			bytes,
			nil,
		)
	}); err != nil {
		return fmt.Errorf("unable to access cache for writing: %w", err)
	}
	if cErr != nil {
		return fmt.Errorf("unable to persist the last timestamp to cache:: %w", cErr)
	}
	return nil
}
