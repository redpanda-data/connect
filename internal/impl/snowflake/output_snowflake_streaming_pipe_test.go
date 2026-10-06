// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/redpanda-data/benthos/v4/public/service"

	v2 "github.com/redpanda-data/connect/v4/internal/impl/snowflake/streamingv2"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/pool"
)

type fakeChannel struct {
	name         string
	committed    string
	appends      [][][]byte
	startOffsets []string
	endOffsets   []string
	waited       []string

	// appendErr, if set, is returned by AppendRows after it records the
	// attempt (appends/offsets are still appended to first, matching the real
	// Channel: a failed append still reached the server with these rows).
	appendErr error
	// waitErr, if set, is returned by WaitUntilCommitted after it records
	// the token, standing in for a commit-poll failure (non-backpressure
	// status error, decode failure, timeout).
	waitErr error

	// rowsRejected (delta) and rowsErrorCount (lifetime total) are
	// independent so a test can simulate pre-existing errors at open.
	rowsRejected     int64
	rowsErrorCount   int64
	lastErrorMessage string

	// latestCalls counts LatestOffsetToken calls, so a test can assert that
	// checkSyntheticTokenSpace's control-plane lookup happens once per
	// channel per process rather than once per batch.
	latestCalls int
}

func (f *fakeChannel) Name() string { return f.name }

// AppendRows mirrors the real Channel: empty/whitespace-only rows are
// skipped, and an all-empty batch records nothing and reports sent=false.
func (f *fakeChannel) AppendRows(_ context.Context, rows [][]byte, startOffsetToken, endOffsetToken string) (bool, error) {
	anyNonEmpty := false
	for _, r := range rows {
		if len(bytes.TrimSpace(r)) > 0 {
			anyNonEmpty = true
			break
		}
	}
	if !anyNonEmpty {
		return false, nil
	}
	f.appends = append(f.appends, rows)
	f.startOffsets = append(f.startOffsets, startOffsetToken)
	f.endOffsets = append(f.endOffsets, endOffsetToken)
	// A request was made either way from here -- sent reports whether one
	// was attempted, not whether it succeeded.
	return true, f.appendErr
}

func (f *fakeChannel) LatestOffsetToken(context.Context) (string, error) {
	f.latestCalls++
	return f.committed, nil
}

func (f *fakeChannel) WaitUntilCommitted(_ context.Context, offsetToken string, _ time.Duration) error {
	f.waited = append(f.waited, offsetToken)
	if offsetToken == "" {
		// The real wait can never be satisfied by an empty token; fail loudly
		// so a regression to sending one is caught.
		return fmt.Errorf("fakeChannel: WaitUntilCommitted called with an empty offset token, which can never be satisfied")
	}
	return f.waitErr
}

// RowsRejected mirrors the real delta semantics: reported once, then 0.
func (f *fakeChannel) RowsRejected() int64 {
	d := f.rowsRejected
	f.rowsRejected = 0
	return d
}
func (f *fakeChannel) RowsErrorCount() int64    { return f.rowsErrorCount }
func (f *fakeChannel) LastErrorMessage() string { return f.lastErrorMessage }

var _ v2.IngestChannel = (*fakeChannel)(nil)

func msg(s string) *service.Message { return service.NewMessage([]byte(s)) }

// renderChannelName resolves an interpolated channel_name template against
// the first message of a batch, the same way WriteBatch does.
func renderChannelName(expr *service.InterpolatedString, batch service.MessageBatch) (string, error) {
	return batch.TryInterpolatedString(0, expr)
}

// defaultChannelNameExpr builds the constant default channel_name.
func defaultChannelNameExpr(t *testing.T, db, schema, pipe string) *service.InterpolatedString {
	t.Helper()
	expr, err := service.NewInterpolatedString(defaultChannelName(db, schema, pipe))
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	return expr
}

// kafkaOffsetTokenExpr builds "${! @kafka_offset }".
func kafkaOffsetTokenExpr(t *testing.T) *service.InterpolatedString {
	t.Helper()
	expr, err := service.NewInterpolatedString("${! @kafka_offset }")
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	return expr
}

// partitionedBatch builds a single-message batch tagged with kafka_partition,
// for exercising the channel_name default's per-partition routing.
func partitionedBatch(t *testing.T, partition int) service.MessageBatch {
	t.Helper()
	m := msg("row")
	m.MetaSetMut("kafka_partition", partition)
	return service.MessageBatch{m}
}

// offsetBatch builds a batch of messages tagged with kafka_offset, in the
// given order, for exercising preprocessForExactlyOncePipe and WriteBatch.
func offsetBatch(t *testing.T, offsets []int) service.MessageBatch {
	t.Helper()
	batch := make(service.MessageBatch, len(offsets))
	for i, offset := range offsets {
		m := msg("row")
		m.MetaSetMut("kafka_offset", offset)
		batch[i] = m
	}
	return batch
}

// testOutput builds an output around a fake channel: default channel_name,
// offset_token "${! @kafka_offset }" (exactly-once path).
func testOutput(t *testing.T, ch *fakeChannel) *snowpipeIndexedOutputPipe {
	t.Helper()
	o := &snowpipeIndexedOutputPipe{
		db:            "DB",
		schema:        "SC",
		channelName:   defaultChannelNameExpr(t, "DB", "SC", "P"),
		offsetToken:   kafkaOffsetTokenExpr(t),
		commitTimeout: time.Second,
		logger:        service.MockResources().Logger(),
		metrics:       newSnowpipePipeMetrics(service.MockResources().Metrics()),
		openChannelFn: func(_ context.Context, name string) (v2.IngestChannel, error) {
			// WriteBatch releases by channel.Name(), so the fake must carry
			// the name it was opened under.
			ch.name = name
			return ch, nil
		},
	}
	o.channelPool = pool.NewIndexed(func(ctx context.Context, name string) (v2.IngestChannel, error) {
		return o.openChannelFn(ctx, name)
	})
	return o
}

// testOutputNoDedup is testOutput with offset_token unset (at-least-once).
func testOutputNoDedup(t *testing.T, ch *fakeChannel) *snowpipeIndexedOutputPipe {
	t.Helper()
	o := testOutput(t, ch)
	o.offsetToken = nil
	return o
}

// testOutputMultiChannel builds an output whose channel_name resolves from a
// "channel" metadata field, with a distinct fake channel per resolved name.
func testOutputMultiChannel(t *testing.T) (*snowpipeIndexedOutputPipe, map[string]*fakeChannel) {
	t.Helper()
	var mu sync.Mutex
	channels := map[string]*fakeChannel{}

	channelNameExpr, err := service.NewInterpolatedString(`${! meta("channel") }`)
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}

	o := &snowpipeIndexedOutputPipe{
		db:            "DB",
		schema:        "SC",
		channelName:   channelNameExpr,
		offsetToken:   kafkaOffsetTokenExpr(t),
		commitTimeout: time.Second,
		logger:        service.MockResources().Logger(),
		metrics:       newSnowpipePipeMetrics(service.MockResources().Metrics()),
		openChannelFn: func(_ context.Context, name string) (v2.IngestChannel, error) {
			mu.Lock()
			defer mu.Unlock()
			ch, ok := channels[name]
			if !ok {
				ch = &fakeChannel{name: name}
				channels[name] = ch
			}
			return ch, nil
		},
	}
	o.channelPool = pool.NewIndexed(func(ctx context.Context, name string) (v2.IngestChannel, error) {
		return o.openChannelFn(ctx, name)
	})
	return o, channels
}

// testOutputCountingOpens is testOutput plus a counter of channel opens
// (the pool's first open and any reopen-on-failure both go through
// openChannelFn).
func testOutputCountingOpens(t *testing.T, ch *fakeChannel) (*snowpipeIndexedOutputPipe, *int) {
	t.Helper()
	o := testOutput(t, ch)
	count := 0
	inner := o.openChannelFn
	o.openChannelFn = func(ctx context.Context, name string) (v2.IngestChannel, error) {
		count++
		return inner(ctx, name)
	}
	return o, &count
}

// TestChannelNameDefaultsToSingleConstantChannel: the default channel_name is
// the constant db.schema.pipe and needs no message metadata.
func TestChannelNameDefaultsToSingleConstantChannel(t *testing.T) {
	batch := service.MessageBatch{msg("row")}
	name, err := renderChannelName(defaultChannelNameExpr(t, "DB", "SC", "P"), batch)
	if err != nil {
		t.Fatalf("renderChannelName: %v", err)
	}
	if name != "DB.SC.P" {
		t.Errorf("channel name = %q, want DB.SC.P", name)
	}
}

// TestChannelNamePerPartitionInterpolation: the documented per-partition
// channel_name pattern resolves against a @kafka_partition-tagged message.
func TestChannelNamePerPartitionInterpolation(t *testing.T) {
	expr, err := service.NewInterpolatedString("prefix-p${! @kafka_partition }")
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	name, err := renderChannelName(expr, partitionedBatch(t, 3))
	if err != nil {
		t.Fatalf("renderChannelName: %v", err)
	}
	if name != "prefix-p3" {
		t.Errorf("channel name = %q, want prefix-p3", name)
	}
}

func TestExactlyOnceDropsAlreadyCommittedRowsNumerically(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: "9"}
	batch := offsetBatch(t, []int{8, 9, 10, 11})
	filtered, first, last, err := preprocessForExactlyOncePipe(t.Context(), ch, kafkaOffsetTokenExpr(t), batch)
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("expected offsets 10 and 11 to survive, got %d rows", len(filtered))
	}
	if first != "10" || last != "11" {
		t.Errorf("range = %q..%q, want 10..11", first, last)
	}
}

func TestExactlyOnceKeepsEverythingWhenNothingCommitted(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: ""}
	batch := offsetBatch(t, []int{0, 1})
	filtered, _, _, err := preprocessForExactlyOncePipe(t.Context(), ch, kafkaOffsetTokenExpr(t), batch)
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("expected both rows, got %d", len(filtered))
	}
}

func TestExactlyOnceDropsWholeBatchWhenBehind(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: "100"}
	batch := offsetBatch(t, []int{9, 10})
	filtered, _, _, err := preprocessForExactlyOncePipe(t.Context(), ch, kafkaOffsetTokenExpr(t), batch)
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}
	if len(filtered) != 0 {
		t.Errorf("expected the whole batch to be dropped, got %d rows", len(filtered))
	}
}

func TestExactlyOnceFallsBackToLexicographicForNonNumericTokens(t *testing.T) {
	// Non-numeric tokens compare as strings: "0/A" < "0/B".
	ch := &fakeChannel{name: "c", committed: "0/A"}
	m0 := msg("row")
	offsetTok, err := service.NewInterpolatedString("${! @custom_offset }")
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	m0.MetaSetMut("custom_offset", "0/A")
	m1 := msg("row")
	m1.MetaSetMut("custom_offset", "0/B")
	batch := service.MessageBatch{m0, m1}
	filtered, first, last, err := preprocessForExactlyOncePipe(t.Context(), ch, offsetTok, batch)
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("expected only 0/B to survive, got %d rows", len(filtered))
	}
	if first != "0/B" || last != "0/B" {
		t.Errorf("range = %q..%q, want 0/B..0/B", first, last)
	}
}

func TestExactlyOnceFailsLoudOnMissingOffsetMetadata(t *testing.T) {
	// Missing metadata interpolates to the literal "null", which must be
	// refused rather than used as a shared token for every such row.
	ch := &fakeChannel{name: "c", committed: ""}
	batch := service.MessageBatch{msg("row")} // no kafka_offset metadata set
	_, _, _, err := preprocessForExactlyOncePipe(t.Context(), ch, kafkaOffsetTokenExpr(t), batch)
	if err == nil {
		t.Fatal("expected an error when the offset token has no @kafka_offset metadata to resolve")
	}
	if !strings.Contains(err.Error(), ssopFieldOffsetToken) {
		t.Errorf("error %q should mention %s", err.Error(), ssopFieldOffsetToken)
	}
}

func TestExactlyOnceFailsLoudOnEmptyOffsetToken(t *testing.T) {
	// "" collides with LatestOffsetToken's nothing-committed sentinel.
	ch := &fakeChannel{name: "c", committed: ""}
	offsetTok, err := service.NewInterpolatedString("${! @custom_offset }")
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	m := msg("row")
	m.MetaSetMut("custom_offset", "")
	batch := service.MessageBatch{m}
	_, _, _, err = preprocessForExactlyOncePipe(t.Context(), ch, offsetTok, batch)
	if err == nil {
		t.Fatal("expected an error when the offset token interpolates to an empty string")
	}
	if !strings.Contains(err.Error(), ssopFieldOffsetToken) {
		t.Errorf("error %q should mention %s", err.Error(), ssopFieldOffsetToken)
	}
}

func TestWriteBatchAppendsThenWaitsForCommit(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)
	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(ch.appends) != 1 || len(ch.appends[0]) != 2 {
		t.Fatalf("expected one append of two rows, got %+v", ch.appends)
	}
	if ch.startOffsets[0] != "5" {
		t.Errorf("start offset token = %q, want the batch's first offset 5", ch.startOffsets[0])
	}
	if ch.endOffsets[0] != "6" {
		t.Errorf("end offset token = %q, want the batch's last offset 6", ch.endOffsets[0])
	}
	if len(ch.waited) != 1 || ch.waited[0] != "6" {
		t.Errorf("expected a commit wait on the end offset 6, got %v", ch.waited)
	}
}

// TestWriteBatchDetectsOutOfOrderSubmissionAndFailsLoud: a batch whose
// tokens are below what this process already submitted for the channel
// fails via checkSubmissionOrder's in-process watermark (committed is left
// "" so the dedup filter cannot be what catches it).
func TestWriteBatchDetectsOutOfOrderSubmissionAndFailsLoud(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)

	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{20})); err != nil {
		t.Fatalf("WriteBatch (offset 20): %v", err)
	}

	err := o.WriteBatch(t.Context(), offsetBatch(t, []int{10}))
	if err == nil {
		t.Fatal("expected WriteBatch to fail loudly on an out-of-order submission, got nil error")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Errorf("error should name the out-of-order-submission problem, got: %v", err)
	}
	// The later, out-of-order batch must never reach AppendRows at all --
	// this is a rejection, not a drop-after-append.
	if len(ch.appends) != 1 {
		t.Errorf("expected only the first (offset 20) batch to have been appended, got %d appends: %+v", len(ch.appends), ch.appends)
	}
}

// TestWriteBatchAppendsWithEqualStartAndEndOffsetsForSingleRowBatch: a
// one-row batch appends with start == end.
func TestWriteBatchAppendsWithEqualStartAndEndOffsetsForSingleRowBatch(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)
	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{7})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(ch.appends) != 1 || len(ch.appends[0]) != 1 {
		t.Fatalf("expected one append of one row, got %+v", ch.appends)
	}
	if ch.startOffsets[0] != "7" || ch.endOffsets[0] != "7" {
		t.Errorf("start=%q end=%q, want both to equal the single row's offset 7", ch.startOffsets[0], ch.endOffsets[0])
	}
	if len(ch.waited) != 1 || ch.waited[0] != "7" {
		t.Errorf("expected a commit wait on 7, got %v", ch.waited)
	}
}

// chMsg builds a message with "channel" and kafka_offset metadata.
func chMsg(channel, payload string, offset int) *service.Message {
	m := msg(payload)
	m.MetaSetMut("channel", channel)
	m.MetaSetMut("kafka_offset", offset)
	return m
}

// TestGroupMessagesByChannel: groups come out in first-appearance order and
// each preserves its messages' relative order.
func TestGroupMessagesByChannel(t *testing.T) {
	channelExpr, err := service.NewInterpolatedString(`${! meta("channel") }`)
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}

	tests := []struct {
		name  string
		batch service.MessageBatch
		want  []channelGroup
	}{
		{
			name:  "empty batch",
			batch: service.MessageBatch{},
			want:  nil,
		},
		{
			name: "single channel",
			batch: service.MessageBatch{
				chMsg("A", "1", 1), chMsg("A", "2", 2), chMsg("A", "3", 3),
			},
			want: []channelGroup{
				{name: "A", batch: service.MessageBatch{chMsg("A", "1", 1), chMsg("A", "2", 2), chMsg("A", "3", 3)}},
			},
		},
		{
			name: "interleaved channels: first-appearance group order, per-group relative order preserved",
			batch: service.MessageBatch{
				chMsg("A", "a1", 1), chMsg("B", "b1", 1), chMsg("A", "a2", 2), chMsg("A", "a3", 3), chMsg("B", "b2", 2),
			},
			want: []channelGroup{
				{name: "A", batch: service.MessageBatch{chMsg("A", "a1", 1), chMsg("A", "a2", 2), chMsg("A", "a3", 3)}},
				{name: "B", batch: service.MessageBatch{chMsg("B", "b1", 1), chMsg("B", "b2", 2)}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := test.batch.InterpolationExecutor(channelExpr)
			got, err := groupMessagesByChannel(exec, test.batch)
			if err != nil {
				t.Fatalf("groupMessagesByChannel: %v", err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("got %d groups, want %d: %+v", len(got), len(test.want), got)
			}
			for i, wantGroup := range test.want {
				if got[i].name != wantGroup.name {
					t.Errorf("group %d name = %q, want %q", i, got[i].name, wantGroup.name)
				}
				if len(got[i].batch) != len(wantGroup.batch) {
					t.Fatalf("group %d: got %d messages, want %d", i, len(got[i].batch), len(wantGroup.batch))
				}
				for j := range wantGroup.batch {
					gotBytes, err := got[i].batch[j].AsBytes()
					if err != nil {
						t.Fatalf("AsBytes: %v", err)
					}
					wantBytes, err := wantGroup.batch[j].AsBytes()
					if err != nil {
						t.Fatalf("AsBytes: %v", err)
					}
					if string(gotBytes) != string(wantBytes) {
						t.Errorf("group %d message %d = %q, want %q", i, j, gotBytes, wantBytes)
					}
				}
			}
		})
	}
}

// TestWriteBatchGroupsInterleavedMessagesByChannel: an interleaved batch
// (A, B, A, A, B) reaches both channels, each with only its own rows, in
// order.
func TestWriteBatchGroupsInterleavedMessagesByChannel(t *testing.T) {
	o, channels := testOutputMultiChannel(t)

	batch := service.MessageBatch{
		chMsg("A", "a1", 1),
		chMsg("B", "b1", 1),
		chMsg("A", "a2", 2),
		chMsg("A", "a3", 3),
		chMsg("B", "b2", 2),
	}
	if err := o.WriteBatch(t.Context(), batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	chA, ok := channels["A"]
	if !ok {
		t.Fatalf("channel A was never opened; channels seen: %v", channels)
	}
	chB, ok := channels["B"]
	if !ok {
		t.Fatalf("channel B was never opened; channels seen: %v", channels)
	}

	if len(chA.appends) != 1 {
		t.Fatalf("channel A: expected exactly one append call, got %d", len(chA.appends))
	}
	wantA := []string{"a1", "a2", "a3"}
	if len(chA.appends[0]) != len(wantA) {
		t.Fatalf("channel A: got %d rows, want %d: %q", len(chA.appends[0]), len(wantA), chA.appends[0])
	}
	for i, want := range wantA {
		if got := string(chA.appends[0][i]); got != want {
			t.Errorf("channel A row %d = %q, want %q", i, got, want)
		}
	}

	if len(chB.appends) != 1 {
		t.Fatalf("channel B: expected exactly one append call, got %d", len(chB.appends))
	}
	wantB := []string{"b1", "b2"}
	if len(chB.appends[0]) != len(wantB) {
		t.Fatalf("channel B: got %d rows, want %d: %q", len(chB.appends[0]), len(wantB), chB.appends[0])
	}
	for i, want := range wantB {
		if got := string(chB.appends[0][i]); got != want {
			t.Errorf("channel B row %d = %q, want %q", i, got, want)
		}
	}
}

// TestWriteBatchRetryAfterPartialMultiChannelFailureDoesNotReAppend: when
// channel A's group commits and B's fails, redelivering the same batch must
// not re-append A's rows, and B must fail the same way.
func TestWriteBatchRetryAfterPartialMultiChannelFailureDoesNotReAppend(t *testing.T) {
	o, channels := testOutputMultiChannel(t)

	batch := service.MessageBatch{
		chMsg("A", "a1", 2),
		chMsg("B", "b1", 50),
	}

	// Simulate B having already lost a race before this test even starts:
	// some other concurrent submission already got a higher token (100)
	// through checkSubmissionOrder for B.
	if err := o.checkSubmissionOrder("B", "100", "100"); err != nil {
		t.Fatalf("priming B's watermark: %v", err)
	}

	err := o.WriteBatch(t.Context(), batch)
	if err == nil {
		t.Fatal("expected WriteBatch to fail on B's out-of-order group")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Fatalf("expected checkSubmissionOrder's error, got: %v", err)
	}

	chA, ok := channels["A"]
	if !ok {
		t.Fatalf("channel A was never opened; channels seen: %v", channels)
	}
	if len(chA.appends) != 1 {
		t.Fatalf("channel A: expected exactly one append on the first WriteBatch call, got %d", len(chA.appends))
	}
	// A's append committed; advance its token as a status poll would.
	chA.committed = "2"

	// Redeliver the identical batch, as Benthos would after this nack.
	err = o.WriteBatch(t.Context(), batch)
	if err == nil {
		t.Fatal("expected the retry to still fail on B -- nothing about a retry changes why B lost the race")
	}
	if !strings.Contains(err.Error(), "out of order") {
		t.Fatalf("expected checkSubmissionOrder's error again on retry, got: %v", err)
	}
	if len(chA.appends) != 1 {
		t.Errorf("channel A: got %d appends after the retry, want still 1 -- the already-committed group must not be re-sent", len(chA.appends))
	}
}

// TestWriteBatchWithoutOffsetTokenSkipsDedupAndUsesSyntheticCommitToken:
// with offset_token unset, no metadata is required, nothing is filtered, and
// each call waits on a distinct non-empty synthetic token.
func TestWriteBatchWithoutOffsetTokenSkipsDedupAndUsesSyntheticCommitToken(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutputNoDedup(t, ch)

	// Plain messages with zero Kafka (or any other) metadata: offset_token
	// being unset must not require any such metadata to be present.
	if err := o.WriteBatch(t.Context(), service.MessageBatch{msg("row-1"), msg("row-2")}); err != nil {
		t.Fatalf("WriteBatch (first batch): %v", err)
	}
	if err := o.WriteBatch(t.Context(), service.MessageBatch{msg("row-3")}); err != nil {
		t.Fatalf("WriteBatch (second batch): %v", err)
	}

	if len(ch.appends) != 2 {
		t.Fatalf("expected two appends (one per WriteBatch call), got %d: %+v", len(ch.appends), ch.appends)
	}
	if len(ch.appends[0]) != 2 || len(ch.appends[1]) != 1 {
		t.Fatalf("expected no rows filtered out (dedup is skipped when offset_token is unset), got %+v", ch.appends)
	}

	for i, tok := range ch.startOffsets {
		if tok == "" {
			t.Errorf("append %d: start offset token is empty, want a non-empty synthetic token", i)
		}
	}
	if len(ch.waited) != 2 {
		t.Fatalf("expected two commit waits, got %d: %v", len(ch.waited), ch.waited)
	}

	// A reused token could not distinguish this call's commit from an
	// earlier one's.
	if ch.waited[0] == ch.waited[1] {
		t.Errorf("expected two distinct synthetic commit tokens across calls, got the same value %q twice", ch.waited[0])
	}
	// Within a single call, start and end offset tokens must match: there's
	// only one synthetic token per call, used as both bounds.
	if ch.startOffsets[0] != ch.endOffsets[0] {
		t.Errorf("call 1: start=%q end=%q, want equal (single synthetic token used for both)", ch.startOffsets[0], ch.endOffsets[0])
	}
	if ch.startOffsets[1] != ch.endOffsets[1] {
		t.Errorf("call 2: start=%q end=%q, want equal (single synthetic token used for both)", ch.startOffsets[1], ch.endOffsets[1])
	}
}

// TestNoDedupSyntheticTokensStartAbovePreviousRunsCommittedToken: a second
// "process" mints synthetic tokens strictly newer than the one the first left
// committed, so its commit wait cannot be satisfied by the old value.
func TestNoDedupSyntheticTokensStartAbovePreviousRunsCommittedToken(t *testing.T) {
	firstRun := &fakeChannel{name: "c"}
	if err := testOutputNoDedup(t, firstRun).WriteBatch(t.Context(), service.MessageBatch{msg("row-1")}); err != nil {
		t.Fatalf("first run WriteBatch: %v", err)
	}
	if len(firstRun.waited) != 1 {
		t.Fatalf("first run: expected one commit wait, got %v", firstRun.waited)
	}
	carriedOver := firstRun.waited[0]
	if !v2.IsSyntheticToken(carriedOver) {
		t.Fatalf("first run committed %q, want a synthetic token (v2.FormatSyntheticToken form)", carriedOver)
	}

	// "Restart": a new output (new seed) against a channel whose committed
	// token is whatever the first run left behind.
	secondRun := &fakeChannel{name: "c", committed: carriedOver}
	if err := testOutputNoDedup(t, secondRun).WriteBatch(t.Context(), service.MessageBatch{msg("row-2")}); err != nil {
		t.Fatalf("second run WriteBatch: %v", err)
	}
	if got := v2.CompareOffsetTokens(secondRun.waited[0], carriedOver); got != 1 {
		t.Errorf("second run's first token %q must compare newer than the previous run's committed %q (got %d)", secondRun.waited[0], carriedOver, got)
	}

	// A committed token from a run whose clock was far ahead: the counter
	// must be bumped from the token, not just seeded from the clock.
	farFuture := v2.FormatSyntheticToken(time.Now().Add(365 * 24 * time.Hour).UnixNano())
	secondRun = &fakeChannel{name: "c", committed: farFuture}
	carriedOver = farFuture
	if err := testOutputNoDedup(t, secondRun).WriteBatch(t.Context(), service.MessageBatch{msg("row-2")}); err != nil {
		t.Fatalf("second run WriteBatch: %v", err)
	}
	if len(secondRun.waited) != 1 {
		t.Fatalf("second run: expected one commit wait, got %v", secondRun.waited)
	}
	if got := v2.CompareOffsetTokens(secondRun.waited[0], carriedOver); got != 1 {
		t.Errorf("second run's first token %q must compare newer than the previous run's committed %q (got %d); "+
			"WaitUntilCommitted would otherwise return before this run's append was confirmed", secondRun.waited[0], carriedOver, got)
	}
}

// TestNoDedupRejectedRowsWarnInsteadOfFailingTheBatch: in at-least-once mode
// a rejected row warns and is counted but does not fail the batch, since a
// retry would only re-insert the good rows and be rejected again.
func TestNoDedupRejectedRowsWarnInsteadOfFailingTheBatch(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 1, rowsErrorCount: 1, lastErrorMessage: "cannot cast"}
	o := testOutputNoDedup(t, ch)
	o.tolerateRowErrors = false
	o.logger = captureLogger(&logBuf)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	if err := o.WriteBatch(t.Context(), service.MessageBatch{msg(`{"a":1}`), msg(`{"a":"bad"}`)}); err != nil {
		t.Fatalf("WriteBatch must not fail in at-least-once mode over rejected rows (it would loop inserting duplicates), got: %v", err)
	}
	if got := readCounter("snowflake_rows_error_count"); got != 1 {
		t.Errorf("snowflake_rows_error_count = %d, want 1", got)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, "level=WARN") || !strings.Contains(logs, "cannot cast") || !strings.Contains(logs, "not failing the batch") {
		t.Errorf("expected a WARN naming the rejection and why the batch isn't failed, got logs:\n%s", logs)
	}
}

// TestReopenAfterFailureReportsRejectedRowsBeforeReopening: a rejection on a
// batch whose commit wait fails is metered and warned before the reopen
// resets the baseline.
func TestReopenAfterFailureReportsRejectedRowsBeforeReopening(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 2, rowsErrorCount: 2, lastErrorMessage: "invalid JSON", waitErr: fmt.Errorf("status poll failed")}
	o := testOutput(t, ch)
	o.logger = captureLogger(&logBuf)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	err := o.WriteBatch(t.Context(), offsetBatch(t, []int{1, 2}))
	if err == nil || !strings.Contains(err.Error(), "status poll failed") {
		t.Fatalf("expected the commit-wait error to surface, got %v", err)
	}
	if got := readCounter("snowflake_rows_error_count"); got != 2 {
		t.Errorf("snowflake_rows_error_count = %d, want 2 (reported before the reopen)", got)
	}
	if logs := logBuf.String(); !strings.Contains(logs, "invalid JSON") || !strings.Contains(logs, "before this batch failed") {
		t.Errorf("expected a WARN about the rejected rows recorded before the failure, got logs:\n%s", logs)
	}
}

// TestNoDedupProceedsOnChannelPreviouslyWrittenWithRealTokens: dropping
// offset_token on a channel with real tokens is allowed, and the committed
// token is fetched once per channel, not per batch.
func TestNoDedupProceedsOnChannelPreviouslyWrittenWithRealTokens(t *testing.T) {
	for _, committed := range []string{"5000", "postgres-0/16B3748", "9223372036854775807"} {
		t.Run(committed, func(t *testing.T) {
			ch := &fakeChannel{name: "c", committed: committed}
			o := testOutputNoDedup(t, ch)
			if err := o.WriteBatch(t.Context(), service.MessageBatch{msg("row-1")}); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}
			if err := o.WriteBatch(t.Context(), service.MessageBatch{msg("row-2")}); err != nil {
				t.Fatalf("WriteBatch (second): %v", err)
			}
			if len(ch.appends) != 2 {
				t.Fatalf("expected both batches appended, got %d appends", len(ch.appends))
			}
			for _, tok := range ch.waited {
				if v2.CompareOffsetTokens(tok, committed) != 1 {
					t.Errorf("synthetic token %q does not compare newer than the channel's real committed token %q", tok, committed)
				}
			}
			if ch.latestCalls != 1 {
				t.Errorf("LatestOffsetToken called %d times across two batches, want exactly 1 (checked once per channel per process)", ch.latestCalls)
			}
		})
	}
}

// TestNoDedupRefusesChannelWhoseRealTokenOutranksSyntheticOnes: a real
// committed token that sorts above synthetic ones is refused, since every
// commit wait would otherwise succeed early.
func TestNoDedupRefusesChannelWhoseRealTokenOutranksSyntheticOnes(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: "~zzz-not-ours"}
	err := testOutputNoDedup(t, ch).WriteBatch(t.Context(), service.MessageBatch{msg("row-1")})
	if err == nil {
		t.Fatal("expected WriteBatch to refuse a channel whose committed token outranks synthetic tokens, got nil")
	}
	if !strings.Contains(err.Error(), "fresh channel_name") {
		t.Errorf("error should tell the operator the fix (a fresh channel_name), got: %v", err)
	}
	if len(ch.appends) != 0 || len(ch.waited) != 0 {
		t.Errorf("nothing must be appended or waited on when refused; appends=%d waited=%v", len(ch.appends), ch.waited)
	}
}

// TestExactlyOnceRefusesChannelLastWrittenAtLeastOnce: adding offset_token
// to a channel carrying synthetic tokens fails every time with nothing
// appended, rather than filtering every row as a duplicate.
func TestExactlyOnceRefusesChannelLastWrittenAtLeastOnce(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: v2.FormatSyntheticToken(time.Now().UnixNano())}
	o := testOutput(t, ch)
	for attempt := range 2 {
		err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6}))
		if err == nil {
			t.Fatalf("attempt %d: expected WriteBatch to refuse a channel last written at-least-once, got nil (rows would have been silently dropped as duplicates)", attempt)
		}
		if !strings.Contains(err.Error(), "fresh channel_name") {
			t.Errorf("attempt %d: error should tell the operator the fix (a fresh channel_name), got: %v", attempt, err)
		}
	}
	if len(ch.appends) != 0 || len(ch.waited) != 0 {
		t.Errorf("nothing must be appended or waited on when refused; appends=%d waited=%v", len(ch.appends), ch.waited)
	}
}

// TestConfigLintRejectsKafkaOffsetTokenWithoutPartitionedChannel covers the
// lint rules; the private_key cases check that all rules in the single
// LintRule block are still live.
func TestConfigLintRejectsKafkaOffsetTokenWithoutPartitionedChannel(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	base := `
snowflake_streaming_pipe:
  account: MYACC
  user: U
  role: R
  database: D
  schema: S
  pipe: P
`
	cases := map[string]struct {
		extra    string
		wantLint string // substring expected in a lint, or "" for clean
	}{
		"kafka offset, default channel":           {"  private_key_file: k.p8\n  offset_token: ${! @kafka_offset }\n", "kafka_partition"},
		"kafka offset, channel without partition": {"  private_key_file: k.p8\n  offset_token: ${! @kafka_offset }\n  channel_name: fixed-name\n", "kafka_partition"},
		"kafka offset, per-partition channel":     {"  private_key_file: k.p8\n  offset_token: ${! @kafka_offset }\n  channel_name: P-p${! @kafka_partition }\n", ""},
		"kafka offset, topic+partition channel":   {"  private_key_file: k.p8\n  offset_token: ${! @kafka_offset }\n  channel_name: ${! @kafka_topic }-p${! @kafka_partition }\n", ""},
		"no offset_token, default channel":        {"  private_key_file: k.p8\n", ""},
		// Rule 4: any offset_token on the default channel needs max_in_flight 1.
		"non-kafka token, default channel, max_in_flight unset (defaults to 4)": {"  private_key_file: k.p8\n  offset_token: ${! @lsn }\n", "max_in_flight"},
		"non-kafka token, default channel, max_in_flight 4":                     {"  private_key_file: k.p8\n  offset_token: ${! @lsn }\n  max_in_flight: 4\n", "max_in_flight"},
		"non-kafka token, default channel, max_in_flight 1":                     {"  private_key_file: k.p8\n  offset_token: ${! @lsn }\n  max_in_flight: 1\n", ""},
		"non-kafka token, explicit channel, max_in_flight 4":                    {"  private_key_file: k.p8\n  offset_token: ${! @lsn }\n  channel_name: cdc-stream\n  max_in_flight: 4\n", ""},
		"both keys set (earlier rule still live)":                               {"  private_key_file: k.p8\n  private_key: abc\n", "can't be set simultaneously"},
		"no key set (earlier rule still live)":                                  {"", "exactly one of"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			lints, err := linter.LintYAML("output", []byte(base+c.extra))
			if err != nil {
				t.Fatalf("LintYAML: %v", err)
			}
			var got []string
			for _, l := range lints {
				got = append(got, l.What)
			}
			if c.wantLint == "" {
				if len(lints) != 0 {
					t.Fatalf("expected a clean lint, got: %v", got)
				}
				return
			}
			for _, w := range got {
				if strings.Contains(w, c.wantLint) {
					return
				}
			}
			t.Fatalf("expected a lint containing %q, got: %v", c.wantLint, got)
		})
	}
}

// TestExactlyOnceRefusesIncomparableTokenSpaces: a numeric batch against a
// non-numeric committed token (an offset_token expression changed on a live
// channel) is refused rather than compared as strings.
func TestExactlyOnceRefusesIncomparableTokenSpaces(t *testing.T) {
	for name, committed := range map[string]string{"prefixed-committed": "postgres-0/16B3748", "hex-committed": "0/16B3748"} {
		t.Run(name, func(t *testing.T) {
			ch := &fakeChannel{name: "c", committed: committed}
			o := testOutput(t, ch) // offset_token = ${! @kafka_offset }, numeric
			for attempt := range 2 {
				err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5000, 5001}))
				if err == nil {
					t.Fatalf("attempt %d: expected refusal for incomparable token spaces, got nil", attempt)
				}
				if !strings.Contains(err.Error(), "cannot be compared") || !strings.Contains(err.Error(), "fresh channel_name") {
					t.Errorf("attempt %d: unexpected error text: %v", attempt, err)
				}
			}
			if len(ch.appends) != 0 {
				t.Errorf("nothing must be appended when refused; got %d appends", len(ch.appends))
			}
		})
	}
	// Same-shape tokens are still compared and filtered as before.
	ch := &fakeChannel{name: "c", committed: "5000"}
	o := testOutput(t, ch)
	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{4999, 5000, 5001})); err != nil {
		t.Fatalf("numeric vs numeric must still work: %v", err)
	}
	if len(ch.appends) != 1 || len(ch.appends[0]) != 1 {
		t.Errorf("expected exactly the one new row (5001) appended, got %+v", ch.appends)
	}
}

// TestWriteBatchAllEmptyRowsSkipsCommitWaitAndAcks: an all-empty batch acks
// without a commit wait and counts the skipped rows, in both modes.
func TestWriteBatchAllEmptyRowsSkipsCommitWaitAndAcks(t *testing.T) {
	cases := map[string]struct {
		build func(*testing.T, *fakeChannel) *snowpipeIndexedOutputPipe
		batch func(*testing.T) service.MessageBatch
	}{
		"at-least-once": {
			build: testOutputNoDedup,
			batch: func(*testing.T) service.MessageBatch { return service.MessageBatch{msg(""), msg("  \n\t")} },
		},
		"exactly-once": {
			build: testOutput,
			batch: func(t *testing.T) service.MessageBatch {
				b := offsetBatch(t, []int{7, 8})
				b[0].SetBytes(nil)
				b[1].SetBytes([]byte("   "))
				return b
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ch := &fakeChannel{name: "c"}
			o := c.build(t, ch)
			metrics, readCounter := newCountingMetrics(t)
			o.metrics = newSnowpipePipeMetrics(metrics)

			if err := o.WriteBatch(t.Context(), c.batch(t)); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}
			if len(ch.appends) != 0 {
				t.Errorf("expected no append recorded for an all-empty batch, got %d", len(ch.appends))
			}
			if len(ch.waited) != 0 {
				t.Errorf("expected no commit wait for an all-empty batch (nothing was sent), got waits on %v", ch.waited)
			}
			if got := readCounter("snowflake_empty_rows_skipped"); got != 2 {
				t.Errorf("snowflake_empty_rows_skipped = %d, want 2", got)
			}
			// A later, real batch on the same channel must still go through
			// normally -- the skip must not have left the channel checked out
			// of the pool or otherwise wedged.
			follow := service.MessageBatch{msg(`{"a":1}`)}
			if o.offsetToken != nil {
				follow = offsetBatch(t, []int{9})
			}
			if err := o.WriteBatch(t.Context(), follow); err != nil {
				t.Fatalf("follow-up WriteBatch: %v", err)
			}
			if len(ch.appends) != 1 || len(ch.waited) != 1 {
				t.Errorf("follow-up batch: appends=%d waited=%d, want 1 and 1", len(ch.appends), len(ch.waited))
			}
		})
	}
}

// TestWriteBatchMixedEmptyRowsCountsSkippedAndSendsTheRest: in a batch that
// mixes tombstones with real rows, the real rows must be appended (and
// waited on) as normal, and only the empty ones counted as skipped.
func TestWriteBatchMixedEmptyRowsCountsSkippedAndSendsTheRest(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutputNoDedup(t, ch)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	if err := o.WriteBatch(t.Context(), service.MessageBatch{msg(`{"a":1}`), msg(""), msg(`{"a":2}`), msg(" ")}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(ch.appends) != 1 || len(ch.waited) != 1 {
		t.Fatalf("appends=%d waited=%d, want 1 and 1", len(ch.appends), len(ch.waited))
	}
	if got := readCounter("snowflake_empty_rows_skipped"); got != 2 {
		t.Errorf("snowflake_empty_rows_skipped = %d, want 2", got)
	}
}

// captureLogger returns a logger writing Info and above into buf.
func captureLogger(buf *bytes.Buffer) *service.Logger {
	return service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(buf, nil)))
}

// TestWriteBatchWarnsAndReportsRowsRejected: with tolerate_row_errors, a
// rejection warns (naming channel, delta and last error) and the batch acks.
func TestWriteBatchWarnsAndReportsRowsRejected(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 3, rowsErrorCount: 3, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)
	o.tolerateRowErrors = true
	o.logger = captureLogger(&logBuf)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	logged := logBuf.String()
	if !strings.Contains(logged, "WARN") {
		t.Fatalf("expected a warning log line, got: %s", logged)
	}
	for _, want := range []string{"c", "3", "invalid JSON in row"} {
		if !strings.Contains(logged, want) {
			t.Errorf("warning should mention %q (channel, delta, and last error message), got: %s", want, logged)
		}
	}

	if got := readCounter("snowflake_rows_error_count"); got != 3 {
		t.Errorf("snowflake_rows_error_count = %d, want 3 (the RowsRejected delta)", got)
	}
}

// TestWriteBatchFailsOnRowsRejectedByDefault: by default a rejection fails
// the batch with an error naming the channel, counts and last error.
func TestWriteBatchFailsOnRowsRejectedByDefault(t *testing.T) {
	ch := &fakeChannel{name: "c", rowsRejected: 3, rowsErrorCount: 8, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6}))
	if err == nil {
		t.Fatalf("expected WriteBatch to fail by default on a nonzero RowsRejected delta")
	}
	msg := err.Error()
	for _, want := range []string{"c", "3", "8", "5", "invalid JSON in row"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error should mention %q (channel, new=3, total=8, initial=5, last error message), got: %s", want, msg)
		}
	}

	// The default (failing) mode must still report the metric. Assert the
	// exact delta (3), not merely nonzero.
	if got := readCounter("snowflake_rows_error_count"); got != 3 {
		t.Errorf("snowflake_rows_error_count = %d, want 3 (the RowsRejected delta)", got)
	}
}

// TestWriteBatchDoesNotRefailOnStaleRowsRejected: a later clean batch does
// not re-fail on an already-reported rejection.
func TestWriteBatchDoesNotRefailOnStaleRowsRejected(t *testing.T) {
	ch := &fakeChannel{name: "c", rowsRejected: 3, rowsErrorCount: 3, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)

	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6})); err == nil {
		t.Fatalf("expected the first batch to fail on the initial delta")
	}

	// The real Channel.RowsRejected only ever reports a given increase once
	// (its baseline advances on read); the fake must mirror that here since
	// it has no baseline logic of its own.
	ch.rowsRejected = 0
	var logBuf bytes.Buffer
	o.logger = captureLogger(&logBuf)
	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{7, 8})); err != nil {
		t.Fatalf("expected the second batch to succeed once RowsRejected reports no new delta, got: %v", err)
	}
	if logged := logBuf.String(); strings.Contains(logged, "WARN") {
		t.Errorf("expected no warning once the historical error was already reported, got: %s", logged)
	}
}

// TestWriteBatchDebugLogsPreExistingErrorsAtOpen: historical errors with no
// new rejection only debug-log.
func TestWriteBatchDebugLogsPreExistingErrorsAtOpen(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 0, rowsErrorCount: 5, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)
	o.logger = captureLogger(&logBuf)

	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6})); err != nil {
		t.Fatalf("expected pre-existing-only errors not to fail the batch, got: %v", err)
	}
	if logged := logBuf.String(); strings.Contains(logged, "WARN") {
		t.Errorf("expected no WARN for pre-existing-only errors (debug only), got: %s", logged)
	}
}

// TestWriteBatchDoesNotWarnWhenNoRowsRejected: a clean batch logs nothing.
func TestWriteBatchDoesNotWarnWhenNoRowsRejected(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)
	o.logger = captureLogger(&logBuf)

	if err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if logged := logBuf.String(); strings.Contains(logged, "WARN") {
		t.Errorf("expected no warning when RowsRejected is 0, got: %s", logged)
	}
}

// TestWriteBatchDoesNotReopenChannelOnBackpressureAppendError: a 429 on
// append returns the channel to the pool unchanged (one open total).
func TestWriteBatchDoesNotReopenChannelOnBackpressureAppendError(t *testing.T) {
	ch := &fakeChannel{name: "c", appendErr: realAppendError(t, 429, `{"error_code":"ReceiverSaturated"}`)}
	o, opens := testOutputCountingOpens(t, ch)

	err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6}))
	if err == nil {
		t.Fatal("expected WriteBatch to propagate the backpressure append error")
	}
	if *opens != 1 {
		t.Errorf("open count = %d, want 1 (initial open only, no reopen on backpressure)", *opens)
	}
}

// TestWriteBatchReopensChannelOnNonBackpressureAppendError: any other append
// failure reopens the channel (two opens total).
func TestWriteBatchReopensChannelOnNonBackpressureAppendError(t *testing.T) {
	ch := &fakeChannel{name: "c", appendErr: realAppendError(t, 500, `{"message":"internal error"}`)}
	o, opens := testOutputCountingOpens(t, ch)

	err := o.WriteBatch(t.Context(), offsetBatch(t, []int{5, 6}))
	if err == nil {
		t.Fatal("expected WriteBatch to propagate the non-backpressure append error")
	}
	if *opens != 2 {
		t.Errorf("open count = %d, want 2 (initial open plus one reopen on a non-backpressure failure)", *opens)
	}
}

// minimalStreamingPipeConfigYAML explicitly sets every field the migration/
// removal checks care about, so configSpecHasField's answer never depends
// on default-value or Optional() semantics either way.
const minimalStreamingPipeConfigYAML = `
account: ORG-ACCOUNT
user: USER
role: ROLE
database: DB
schema: SCHEMA
pipe: PIPE
private_key: dummy-key
channel_name: "${! @kafka_partition }"
offset_token: "${! @kafka_offset }"
max_in_flight: 4
batching: {}
`

// configSpecHasField reports whether name is a field of the output's config
// spec, by parsing a config that sets every expected field.
func configSpecHasField(t *testing.T, name string) bool {
	t.Helper()
	conf, err := snowpipeStreamingPipeOutputConfig().ParseYAML(minimalStreamingPipeConfigYAML, nil)
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}
	return conf.Contains(name)
}

func TestConfigRejectsRemovedFields(t *testing.T) {
	for _, field := range []string{"schema_evolution", "build_options", "mapping", "channel_prefix"} {
		if configSpecHasField(t, field) {
			t.Errorf("field %q must not exist: it is out of scope for SSv2", field)
		}
	}
}

func TestConfigKeepsInheritedFieldsForMigration(t *testing.T) {
	for _, field := range []string{
		"account", "user", "private_key", "role", "database", "schema",
		"channel_name", "offset_token", "max_in_flight", "batching",
		"pipe",
	} {
		if !configSpecHasField(t, field) {
			t.Errorf("field %q must exist: migration from their SSv1 config depends on it", field)
		}
	}
}

// generatedPrivateKeyBase64 returns a fresh RSA key as bare base64 PKCS8 DER
// (getPrivateKey's non-PEM branch).
func generatedPrivateKeyBase64(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	return base64.StdEncoding.EncodeToString(der)
}

// testResources returns mock resources with an enterprise license injected.
func testResources() *service.Resources {
	resources := service.MockResources()
	license.InjectTestService(resources)
	return resources
}

// discoveryProbeTransport answers requests in-process via
// connectDiscoveryHandler and records each request's scheme://host by path,
// so a test can observe which host the constructor-built client dialled.
type discoveryProbeTransport struct {
	seen map[string]string // request path -> "scheme://host"
}

func (rt *discoveryProbeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.seen == nil {
		rt.seen = map[string]string{}
	}
	rt.seen[req.URL.Path] = req.URL.Scheme + "://" + req.URL.Host
	rec := httptest.NewRecorder()
	connectDiscoveryHandler().ServeHTTP(rec, req)
	return rec.Result(), nil
}

// connectDiscoveryHandler answers discovery, the pre-flight status probe,
// and the token exchange so Connect succeeds without network access.
func connectDiscoveryHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.invalid")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		default:
			_, _ = io.WriteString(w, "scoped")
		}
	})
}

// TestAccountHostReachesClientConfig: account_host reaches the client
// (observed via the discovery host dialled), and unset derives
// <account>.snowflakecomputing.com.
func TestAccountHostReachesClientConfig(t *testing.T) {
	const yamlTemplate = `
account: org-account
%s
user: USER
role: ROLE
database: DB
schema: SCHEMA
pipe: PIPE
private_key: %q
`
	for _, tc := range []struct {
		name         string
		accountHost  string // account_host YAML line, empty to omit the field
		wantAcctBase string // expected scheme://host for the /v2/streaming/hostname request
	}{
		{
			name:         "account_host set is used as the control-plane host",
			accountHost:  "account_host: my-account.privatelink.snowflakecomputing.com",
			wantAcctBase: "https://my-account.privatelink.snowflakecomputing.com",
		},
		{
			name:         "account_host unset derives <account>.snowflakecomputing.com",
			accountHost:  "",
			wantAcctBase: "https://org-account.snowflakecomputing.com",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := &discoveryProbeTransport{}
			orig := http.DefaultTransport
			http.DefaultTransport = rt
			defer func() { http.DefaultTransport = orig }()

			yaml := fmt.Sprintf(yamlTemplate, tc.accountHost, generatedPrivateKeyBase64(t))
			conf, err := snowpipeStreamingPipeOutputConfig().ParseYAML(yaml, nil)
			if err != nil {
				t.Fatalf("ParseYAML: %v", err)
			}
			out, _, _, err := newSnowpipeStreamingPipeOutput(conf, testResources())
			if err != nil {
				t.Fatalf("newSnowpipeStreamingPipeOutput: %v", err)
			}
			// The handshake runs in Connect, not the constructor.
			if err := out.Connect(t.Context()); err != nil {
				t.Fatalf("Connect: %v", err)
			}

			got, ok := rt.seen["/v2/streaming/hostname"]
			if !ok {
				t.Fatalf("no hostname-discovery request observed; requests seen: %v", rt.seen)
			}
			if got != tc.wantAcctBase {
				t.Errorf("hostname discovery dialed %q, want %q", got, tc.wantAcctBase)
			}
		})
	}
}

// TestNewSnowflakeStreamerDefaultsChannelNameToConstantPipeChannel: through
// the config path, an unset channel_name is <DATABASE>.<SCHEMA>.<pipe>
// (database/schema upper-cased, pipe as written).
func TestNewSnowflakeStreamerDefaultsChannelNameToConstantPipeChannel(t *testing.T) {
	srv := httptest.NewServer(connectDiscoveryHandler())
	defer srv.Close()

	yaml := fmt.Sprintf(`
account: ORG-ACCOUNT
user: USER
role: ROLE
database: db
schema: schema
pipe: pipe
private_key: %q
url: %q
max_in_flight: 4
batching: {}
`, generatedPrivateKeyBase64(t), srv.URL)

	conf, err := snowpipeStreamingPipeOutputConfig().ParseYAML(yaml, nil)
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}
	out, _, _, err := newSnowpipeStreamingPipeOutput(conf, testResources())
	if err != nil {
		t.Fatalf("newSnowpipeStreamingPipeOutput: %v", err)
	}
	// The handshake now lives in Connect; run it against the local discovery
	// handler so this test still proves the config-built client connects,
	// and a second call is a free no-op.
	for i := range 2 {
		if err := out.Connect(t.Context()); err != nil {
			t.Fatalf("Connect (call %d): %v", i+1, err)
		}
	}
	indexed, ok := out.(*snowpipeIndexedOutputPipe)
	if !ok {
		t.Fatalf("expected *snowpipeIndexedOutputPipe, got %T", out)
	}

	// Zero Kafka (or any other) metadata: the whole point of the new default
	// is that it works without it.
	name, err := renderChannelName(indexed.channelName, service.MessageBatch{msg("row")})
	if err != nil {
		t.Fatalf("renderChannelName: %v", err)
	}
	if name != "DB.SCHEMA.pipe" {
		t.Errorf("channel name = %q, want DB.SCHEMA.pipe (database/schema uppercased, pipe unchanged, no partition suffix)", name)
	}
}

// realAppendHandler extends connectDiscoveryHandler with channel open (always
// SUCCESS) and rows append (the caller's status and body).
func realAppendHandler(appendStatus int, appendBody string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.invalid")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		case r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/channels/"):
			_, _ = io.WriteString(w, `{"next_continuation_token":"ct-0","channel_status":{"channel_status_code":"SUCCESS"}}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/rows"):
			if appendStatus/100 != 2 {
				w.WriteHeader(appendStatus)
			}
			_, _ = io.WriteString(w, appendBody)
		default:
			_, _ = io.WriteString(w, "scoped")
		}
	})
}

// realAppendError harvests a genuine AppendRows error for the given
// status/body from a real client against a local server, since the error
// type IsBackpressure classifies is unexported.
func realAppendError(t *testing.T, appendStatus int, appendBody string) error {
	t.Helper()
	srv := httptest.NewServer(realAppendHandler(appendStatus, appendBody))
	defer srv.Close()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	c, err := v2.NewClient(v2.Config{
		Account: "ORG-ACCOUNT", User: "USER", PrivateKey: key,
		Database: "DB", Schema: "SCHEMA", Pipe: "PIPE",
		BaseURL: srv.URL,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(t.Context()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(t.Context(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	_, err = ch.AppendRows(t.Context(), [][]byte{[]byte(`{"a":1}`)}, "1", "1")
	if err == nil {
		t.Fatalf("expected AppendRows to fail against a %d append response, got nil error", appendStatus)
	}
	return err
}

// newCountingMetrics builds a *service.Metrics backed by an in-process
// exporter plugin (the only public route to a readable Metrics is via a
// ResourceBuilder) plus a reader for a named counter's value. The exporter
// holds the values itself: benthos re-invokes the counter ctor on every
// Incr, so a per-handle count would reset each time.
func newCountingMetrics(t *testing.T) (*service.Metrics, func(name string) int64) {
	t.Helper()

	exporter := &countingMetricsExporter{counts: map[string]int64{}}

	env := service.NewEnvironment()
	const pluginName = "counting_test_exporter"
	if err := env.RegisterMetricsExporter(pluginName, service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Logger) (service.MetricsExporter, error) {
			return exporter, nil
		},
	); err != nil {
		t.Fatalf("newCountingMetrics: RegisterMetricsExporter: %v", err)
	}

	// Build needs a "none" tracer provider, normally registered by
	// internal/impl/pure's init(), which this test binary doesn't import.
	if err := env.RegisterOtelTracerProvider("none", service.NewConfigSpec(),
		func(*service.ParsedConfig) (trace.TracerProvider, error) {
			return noop.NewTracerProvider(), nil
		},
	); err != nil {
		t.Fatalf("newCountingMetrics: RegisterOtelTracerProvider: %v", err)
	}

	builder := env.NewResourceBuilder()
	if err := builder.SetMetricsYAML(fmt.Sprintf("%s: {}", pluginName)); err != nil {
		t.Fatalf("newCountingMetrics: SetMetricsYAML: %v", err)
	}

	res, closeFn, err := builder.Build()
	if err != nil {
		t.Fatalf("newCountingMetrics: Build: %v", err)
	}
	t.Cleanup(func() {
		if err := closeFn(t.Context()); err != nil {
			t.Errorf("newCountingMetrics: close: %v", err)
		}
	})

	return res.Metrics(), exporter.get
}

// countingMetricsExporter accumulates counter values by metric name,
// ignoring labels.
type countingMetricsExporter struct {
	mu     sync.Mutex
	counts map[string]int64
}

var _ service.MetricsExporter = (*countingMetricsExporter)(nil)

func (e *countingMetricsExporter) NewCounterCtor(name string, _ ...string) service.MetricsExporterCounterCtor {
	return func(_ ...string) service.MetricsExporterCounter {
		return &countingMetricsExporterCounter{exporter: e, name: name}
	}
}

// NewTimerCtor and NewGaugeCtor are no-ops; only counters are asserted on.
func (*countingMetricsExporter) NewTimerCtor(string, ...string) service.MetricsExporterTimerCtor {
	return func(_ ...string) service.MetricsExporterTimer { return noopMetricsExporterTimer{} }
}

func (*countingMetricsExporter) NewGaugeCtor(string, ...string) service.MetricsExporterGaugeCtor {
	return func(_ ...string) service.MetricsExporterGauge { return noopMetricsExporterGauge{} }
}

func (*countingMetricsExporter) Close(context.Context) error { return nil }

func (e *countingMetricsExporter) get(name string) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counts[name]
}

// incr adds count to the named counter, taking the exporter's own lock
// directly rather than having a caller reach through a chained
// counter.exporter.mu selector.
func (e *countingMetricsExporter) incr(name string, count int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.counts[name] += count
}

// countingMetricsExporterCounter mutates the exporter's shared map on Incr
// (see newCountingMetrics for why it holds no state).
type countingMetricsExporterCounter struct {
	exporter *countingMetricsExporter
	name     string
}

var _ service.MetricsExporterCounter = (*countingMetricsExporterCounter)(nil)

func (c *countingMetricsExporterCounter) Incr(count int64) {
	c.exporter.incr(c.name, count)
}

type noopMetricsExporterTimer struct{}

var _ service.MetricsExporterTimer = noopMetricsExporterTimer{}

func (noopMetricsExporterTimer) Timing(int64) {}

type noopMetricsExporterGauge struct{}

var _ service.MetricsExporterGauge = noopMetricsExporterGauge{}

func (noopMetricsExporterGauge) Set(int64) {}
