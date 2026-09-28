// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kinesis

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// shardRecordSource yields batches of records from a single Kinesis shard,
// abstracting over the polling (GetRecords) and enhanced fan-out
// (SubscribeToShard) consumption models.
type shardRecordSource interface {
	// Fetch returns the next batch of records. done is true once the shard is
	// closed and fully consumed. A source that waits for data does so
	// internally (bounded); one that does not returns immediately and relies
	// on the caller to pace retries.
	Fetch(ctx context.Context) (recs []types.Record, done bool, err error)
	// WaitsForData reports whether Fetch waits for data internally, in which
	// case an empty result is a timeout and the caller must not add its own
	// backoff to it.
	WaitsForData() bool
	// Close releases any underlying resources.
	Close()
}

// errPollGateWaiting signals that a Fetch returned early because the
// poll_period gate has not yet elapsed, rather than because the shard
// had no records. The consumer loop retries immediately without arming
// its failure backoff; the gate itself is the pacing mechanism.
var errPollGateWaiting = errors.New("poll gate waiting")

// kinesisPollAPI is the subset of the Kinesis API used by the polling source.
type kinesisPollAPI interface {
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

// pollingRecordSource consumes a shard via GetRecords, owning the shard
// iterator, its refresh on expiry, and the optional poll_period gate.
type pollingRecordSource struct {
	api             kinesisPollAPI
	streamARN       string
	shardID         string
	startFromOldest bool
	pollPeriod      time.Duration
	// maxGateWait bounds how long a single Fetch may sleep inside the
	// poll_period gate before handing control back to the caller, so that a
	// long poll_period cannot starve the consumer loop's commit timer. A value
	// of zero leaves the gate wait uncapped.
	maxGateWait time.Duration
	sequenceFn  func() string
	log         *service.Logger

	iter     string
	lastPoll time.Time
	// lastReadSeq is the sequence number of the last record read from the
	// shard, which may be ahead of the last acknowledged sequence.
	lastReadSeq string
}

func newPollingRecordSource(ctx context.Context, api kinesisPollAPI, streamARN, shardID, startingSequence string, startFromOldest bool, pollPeriod, maxGateWait time.Duration, sequenceFn func() string, log *service.Logger) (*pollingRecordSource, error) {
	p := &pollingRecordSource{
		api:             api,
		streamARN:       streamARN,
		shardID:         shardID,
		startFromOldest: startFromOldest,
		pollPeriod:      pollPeriod,
		maxGateWait:     maxGateWait,
		sequenceFn:      sequenceFn,
		log:             log,
	}
	iter, err := p.getIter(ctx, startingSequence)
	if err != nil {
		return nil, err
	}
	p.iter = iter
	return p, nil
}

func (p *pollingRecordSource) getIter(ctx context.Context, sequence string) (string, error) {
	iterType := types.ShardIteratorTypeTrimHorizon
	if !p.startFromOldest {
		iterType = types.ShardIteratorTypeLatest
	}
	var startingSequence *string
	if sequence != "" {
		iterType = types.ShardIteratorTypeAfterSequenceNumber
		startingSequence = &sequence
	}

	res, err := p.api.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamARN:              &p.streamARN,
		ShardId:                &p.shardID,
		StartingSequenceNumber: startingSequence,
		ShardIteratorType:      iterType,
	})
	if err != nil {
		// A sequence that has aged out of the shard's retention window is
		// rejected outright rather than yielding an empty iterator, and no
		// number of retries will make it resolvable again, so fall through to
		// the TRIM_HORIZON fallback below.
		var invalidArg *types.InvalidArgumentException
		if startingSequence == nil || !errors.As(err, &invalidArg) {
			return "", err
		}
		p.log.Warnf("Stored sequence for shard '%v' was rejected, falling back to the oldest retained record", p.shardID)
	}

	var iter string
	if res != nil && res.ShardIterator != nil {
		iter = *res.ShardIterator
	}
	if iter == "" {
		// If we failed to obtain from a sequence we start from beginning
		iterType = types.ShardIteratorTypeTrimHorizon

		res, err := p.api.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamARN:         &p.streamARN,
			ShardId:           &p.shardID,
			ShardIteratorType: iterType,
		})
		if err != nil {
			return "", err
		}

		if res.ShardIterator != nil {
			iter = *res.ShardIterator
		}
	}
	if iter == "" {
		return "", errors.New("obtaining shard iterator")
	}
	return iter, nil
}

// waitPollGate enforces the poll_period spacing between GetRecords calls. It
// returns nil when the caller may poll now, errPollGateWaiting when it slept
// for maxGateWait and the period has still not elapsed, or the context error.
// lastPoll only advances on a nil return, so a capped wait never shortens the
// spacing between real polls, whilst the caller stays free to service its
// commit timer and flush pending messages between capped waits.
func (p *pollingRecordSource) waitPollGate(ctx context.Context) error {
	if p.pollPeriod > 0 {
		if wait := p.pollPeriod - time.Since(p.lastPoll); wait > 0 {
			sleep, capped := wait, false
			if p.maxGateWait > 0 && p.maxGateWait < wait {
				sleep, capped = p.maxGateWait, true
			}
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return ctx.Err()
			}
			if capped {
				return errPollGateWaiting
			}
		}
	}
	p.lastPoll = time.Now()
	return nil
}

// resumeSequence returns the sequence number to resume from when refreshing
// an expired iterator. It prioritises the last read (possibly in-flight)
// sequence over the last acknowledged one so that a shard with in-flight but
// unacknowledged records neither re-reads them nor, when nothing was ever
// acknowledged, falls back to LATEST and skips the backlog.
func (p *pollingRecordSource) resumeSequence() string {
	if p.lastReadSeq != "" {
		return p.lastReadSeq
	}
	return p.sequenceFn()
}

// Fetch pulls the next batch via GetRecords, pacing calls through
// waitPollGate. The shard iterator is only replaced on success or an internal
// refresh, so a failed call can always be retried with the retained iterator.
func (p *pollingRecordSource) Fetch(ctx context.Context) ([]types.Record, bool, error) {
	if err := p.waitPollGate(ctx); err != nil {
		return nil, false, err
	}

	res, err := p.api.GetRecords(ctx, &kinesis.GetRecordsInput{
		StreamARN:     &p.streamARN,
		Limit:         &awsKinesisDefaultLimit,
		ShardIterator: &p.iter,
	})
	if err != nil {
		var aerr *types.ExpiredIteratorException
		if errors.As(err, &aerr) {
			p.log.Warn("Shard iterator expired, attempting to refresh")
			newIter, ierr := p.getIter(ctx, p.resumeSequence())
			if ierr != nil {
				p.log.Errorf("Failed to refresh shard iterator: %v", ierr)
			} else {
				p.iter = newIter
				// Re-open the poll gate so the fresh iterator is used on the
				// next fetch: iterators expire after five minutes, so making
				// it wait out a poll_period that can exceed that would let it
				// expire again and wedge the shard permanently.
				p.lastPoll = time.Time{}
			}
			// Treated as an empty result so the caller applies its usual
			// empty-result pacing, matching the pre-refactor behaviour.
			return nil, false, nil
		}
		return nil, false, err
	}

	if len(res.Records) > 0 {
		if last := res.Records[len(res.Records)-1].SequenceNumber; last != nil {
			p.lastReadSeq = *last
		}
	}

	nextIter := ""
	if res.NextShardIterator != nil {
		nextIter = *res.NextShardIterator
	}
	p.iter = nextIter
	return res.Records, nextIter == "", nil
}

func (*pollingRecordSource) WaitsForData() bool { return false }

func (*pollingRecordSource) Close() {}
