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

	// rowsRejected, rowsErrorCount and lastErrorMessage are returned verbatim
	// by RowsRejected, RowsErrorCount and LastErrorMessage, letting tests
	// drive WriteBatch's post-commit warning/error/metric behaviour without a
	// real streamingv2.Channel. They're independent fields (unlike the real
	// Channel, rowsErrorCount here isn't derived from rowsRejected) so a test
	// can set "delta since baseline" and "lifetime total" separately, e.g. to
	// simulate pre-existing errors at open (rowsRejected == 0, rowsErrorCount
	// > 0) alongside new ones (rowsRejected > 0, rowsErrorCount >=
	// rowsRejected).
	rowsRejected     int64
	rowsErrorCount   int64
	lastErrorMessage string

	// latestCalls counts LatestOffsetToken calls, so a test can assert that
	// checkSyntheticTokenSpace's control-plane lookup happens once per
	// channel per process rather than once per batch.
	latestCalls int
}

func (f *fakeChannel) Name() string { return f.name }

// AppendRows mirrors the real Channel's EncodeRows-driven behaviour: rows
// that are empty or whitespace-only after trimming are skipped, and a batch
// consisting entirely of such rows makes no request and reports sent=false,
// err=nil -- not appendErr, and not recorded in f.appends/startOffsets/
// endOffsets, matching that the real client never reaches the network for
// this case either.
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
		// Mirrors the real Channel closely enough to catch the bug
		// noDedupCommitToken (output_snowflake_streaming_pipe.go) exists to
		// prevent: its actual success condition requires a non-empty
		// LastCommittedOffsetToken echoed back by Snowflake, and Snowflake
		// only ever has what an AppendRows call sent it to echo -- so a
		// caller polling for the empty string can never succeed and would
		// hang/time out forever in production. Fail loudly here instead of
		// silently returning nil, so a regression to passing an empty commit
		// token (e.g. when offset_token is unset) is caught immediately.
		return fmt.Errorf("fakeChannel: WaitUntilCommitted called with an empty offset token, which can never be satisfied")
	}
	return f.waitErr
}

// RowsRejected mirrors the real Channel's delta-and-advance semantics: the
// pending delta is reported once and then cleared, so a second call (e.g.
// the post-commit check after reopenAfterFailure already reported it)
// returns 0.
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

// defaultChannelNameExpr builds the channel_name default's InterpolatedString
// (defaultChannelName) for a fully-qualified db/schema/pipe: a single,
// constant, unpartitioned channel with no interpolation at all -- unlike the
// old per-partition default this replaced, resolving it needs no message
// metadata of any kind.
func defaultChannelNameExpr(t *testing.T, db, schema, pipe string) *service.InterpolatedString {
	t.Helper()
	expr, err := service.NewInterpolatedString(defaultChannelName(db, schema, pipe))
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	return expr
}

// kafkaOffsetTokenExpr builds "${! @kafka_offset }" as an
// *service.InterpolatedString. offset_token has no default anymore (see the
// field's Description on snowpipeStreamingPipeOutputConfig) -- this is just a
// concrete, realistic expression these tests use to exercise
// preprocessForExactlyOncePipe and WriteBatch's dedup/ordering path when
// offset_token IS configured, mirroring the config's own Kafka/Redpanda
// example. See testOutputNoDedup for offset_token left unset instead.
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

// testOutput builds a snowpipeIndexedOutputPipe wired to a fake channel via
// openChannelFn, so WriteBatch can be exercised without any network I/O.
// channel_name is left at its new default (a single, constant, unpartitioned
// channel); offset_token is explicitly set to "${! @kafka_offset }" so
// WriteBatch's dedup/ordering path is exercised -- offset_token itself has no
// default anymore. See testOutputNoDedup for offset_token left unset, and
// testOutputMultiChannel for a channel_name that resolves to more than one
// channel.
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
			// The real Channel returned by Client.OpenChannel carries the
			// name it was opened under (see streamingv2/channel.go), and
			// WriteBatch releases the pool entry by channel.Name() rather
			// than by the name it acquired with. The fake must mirror that
			// or Release keys the pool's map under a name Acquire never
			// created, sending on a nil channel and deadlocking forever.
			ch.name = name
			return ch, nil
		},
	}
	o.channelPool = pool.NewIndexed(func(ctx context.Context, name string) (v2.IngestChannel, error) {
		return o.openChannelFn(ctx, name)
	})
	return o
}

// testOutputNoDedup is testOutput with offset_token left unset (offsetToken
// nil), exercising writeChannelGroup's else branch: dedup/ordering is skipped
// entirely, and WaitUntilCommitted is driven by a synthetic per-call commit
// token from noDedupCommitToken instead of one derived from offset_token.
func testOutputNoDedup(t *testing.T, ch *fakeChannel) *snowpipeIndexedOutputPipe {
	t.Helper()
	o := testOutput(t, ch)
	o.offsetToken = nil
	return o
}

// testOutputMultiChannel builds a snowpipeIndexedOutputPipe whose channel_name
// resolves per-message from a "channel" metadata field, and whose
// openChannelFn hands back a distinct *fakeChannel per resolved name (created
// lazily on first Acquire, guarded by mu since WriteBatch may Acquire more
// than one channel from the same call). This lets a test assert that a batch
// spanning multiple channels actually reaches every one of them, in their
// own relative order -- the regression case for the bug that motivated
// groupMessagesByChannel: the old code only ever resolved channel_name from
// message 0 and applied that single name to the whole batch.
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

// testOutputCountingOpens is testOutput plus an incrementing counter wrapped
// around openChannelFn, so a test can assert on how many times WriteBatch
// actually opened/reopened a channel -- as opposed to releasing the existing
// one back to the pool unchanged. o.openChannel (see
// output_snowflake_streaming_pipe.go) is a thin pass-through to
// o.openChannelFn, and channelPool's constructor closure reads
// o.openChannelFn dynamically on every call rather than capturing it at
// construction time, so overwriting the field after testOutput returns still
// intercepts both the pool's first-Acquire open and WriteBatch's explicit
// reopen-on-failure call.
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

// TestChannelNameDefaultsToSingleConstantChannel guards the new channel_name
// default (defaultChannelName): a single, constant, unpartitioned channel
// scoped to db.schema.pipe, with no interpolation and no dependency on any
// Kafka metadata -- unlike the old default this replaced, which baked in a
// kafka_partition suffix and so silently broke on any non-Kafka input. The
// batch here carries zero @kafka_* (or any other) metadata on purpose, to
// prove that isn't needed.
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

// TestChannelNamePerPartitionInterpolation proves the pattern the docs now
// recommend for partitioned Kafka/Redpanda inputs -- setting channel_name
// directly to "<prefix>-p${!@kafka_partition}", since channel_prefix (a
// convenience over exactly this expression) was removed -- actually resolves
// against a real @kafka_partition-tagged message. There's no dedicated
// field for this anymore, so this is the only coverage that this
// documented, copy-pasteable pattern doesn't silently drift from what
// InterpolatedString actually accepts.
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
	filtered, first, last, err := preprocessForExactlyOncePipe(context.Background(), ch, kafkaOffsetTokenExpr(t), batch)
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
	filtered, _, _, err := preprocessForExactlyOncePipe(context.Background(), ch, kafkaOffsetTokenExpr(t), batch)
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
	filtered, _, _, err := preprocessForExactlyOncePipe(context.Background(), ch, kafkaOffsetTokenExpr(t), batch)
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}
	if len(filtered) != 0 {
		t.Errorf("expected the whole batch to be dropped, got %d rows", len(filtered))
	}
}

func TestExactlyOnceFallsBackToLexicographicForNonNumericTokens(t *testing.T) {
	// v2.CompareOffsetTokens only compares numerically when both tokens parse
	// as integers; non-numeric offset tokens (e.g. a custom offset_token
	// expression that isn't @kafka_offset) fall back to a plain string
	// comparison. "0/A" < "0/B" lexicographically, so committing "0/A" must
	// keep "0/B" and drop nothing older.
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
	filtered, first, last, err := preprocessForExactlyOncePipe(context.Background(), ch, offsetTok, batch)
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
	// A message with no kafka_offset metadata (e.g. a non-Kafka input whose
	// offset_token was explicitly set to "${! @kafka_offset }" anyway) doesn't
	// interpolate to "": Bloblang
	// stringifies the missing-metadata null as the literal "null". Either
	// way requireOffsetToken must fail loud rather than let a fake token
	// through -- "null" would otherwise be silently treated as a real,
	// identical offset token for every such row, and "" would collide with
	// LatestOffsetToken's "nothing committed" sentinel.
	ch := &fakeChannel{name: "c", committed: ""}
	batch := service.MessageBatch{msg("row")} // no kafka_offset metadata set
	_, _, _, err := preprocessForExactlyOncePipe(context.Background(), ch, kafkaOffsetTokenExpr(t), batch)
	if err == nil {
		t.Fatal("expected an error when the offset token has no @kafka_offset metadata to resolve")
	}
	if !strings.Contains(err.Error(), ssopFieldOffsetToken) {
		t.Errorf("error %q should mention %s", err.Error(), ssopFieldOffsetToken)
	}
}

func TestExactlyOnceFailsLoudOnEmptyOffsetToken(t *testing.T) {
	// An explicit offset_token expression that resolves to an empty string
	// must also fail loud: an empty token collides with LatestOffsetToken's
	// "nothing committed" sentinel, so silently accepting one would make a
	// real row indistinguishable from that sentinel.
	ch := &fakeChannel{name: "c", committed: ""}
	offsetTok, err := service.NewInterpolatedString("${! @custom_offset }")
	if err != nil {
		t.Fatalf("NewInterpolatedString: %v", err)
	}
	m := msg("row")
	m.MetaSetMut("custom_offset", "")
	batch := service.MessageBatch{m}
	_, _, _, err = preprocessForExactlyOncePipe(context.Background(), ch, offsetTok, batch)
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
	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6})); err != nil {
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

// TestWriteBatchDetectsOutOfOrderSubmissionAndFailsLoud proves
// checkSubmissionOrder's actual job: a batch whose own tokens are lower
// than one this process already submitted for the same channel must fail
// loudly, not silently vanish through preprocessForExactlyOncePipe's
// already-committed filter as if it were an unremarkable duplicate. This is
// the scenario streaming_pipe_maxinflight_it_test.go exercises against a
// real account (many concurrent WriteBatch calls racing on one channel); this
// version pins the exact mechanism with a fake channel, no network involved.
//
// fakeChannel.committed is deliberately left at "" throughout: the point is
// that checkSubmissionOrder catches this using its own in-process watermark
// before preprocessForExactlyOncePipe ever queries the channel's committed
// token, precisely because the committed token alone can't tell "my own
// batch landing a second time" apart from "a different, later batch already
// landed before mine got its turn" (see checkSubmissionOrder's doc comment).
func TestWriteBatchDetectsOutOfOrderSubmissionAndFailsLoud(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)

	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{20})); err != nil {
		t.Fatalf("WriteBatch (offset 20): %v", err)
	}

	err := o.WriteBatch(context.Background(), offsetBatch(t, []int{10}))
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

// TestWriteBatchAppendsWithEqualStartAndEndOffsetsForSingleRowBatch guards the
// single-row case explicitly: a one-message batch has no distinct first/last
// offset, so start and end must both equal that row's own offset token,
// matching the reference SDK's test_get_insert_rows_url_with_same_start_end_token
// case.
func TestWriteBatchAppendsWithEqualStartAndEndOffsetsForSingleRowBatch(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)
	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{7})); err != nil {
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

// chMsg builds a message tagged with a "channel" metadata field (read by
// testOutputMultiChannel's channel_name mapping) and a kafka_offset (so
// offset_token, left at kafkaOffsetTokenExpr, still has something to
// interpolate).
func chMsg(channel, payload string, offset int) *service.Message {
	m := msg(payload)
	m.MetaSetMut("channel", channel)
	m.MetaSetMut("kafka_offset", offset)
	return m
}

// TestGroupMessagesByChannel unit-tests groupMessagesByChannel directly and
// in isolation, without spinning up a whole output: an empty batch, a single
// channel, and 2+ interleaved channels. It asserts both that groups come out
// in first-appearance order and that each group preserves its own messages'
// relative order from the original batch, since preprocessForExactlyOncePipe
// downstream assumes increasing offset-token order within a channel.
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

// TestWriteBatchGroupsInterleavedMessagesByChannel is the regression test for
// the bug groupMessagesByChannel exists to fix: the old WriteBatch resolved
// channel_name from message 0 only and applied that single name to the whole
// batch, so a batch spanning more than one channel silently misrouted every
// message after the first. This batch interleaves two channels (A, B, A, A,
// B) rather than partitioning them into two contiguous halves, and asserts
// both channels are actually written to -- not just the first -- each
// receiving only its own rows, in their original relative order.
func TestWriteBatchGroupsInterleavedMessagesByChannel(t *testing.T) {
	o, channels := testOutputMultiChannel(t)

	batch := service.MessageBatch{
		chMsg("A", "a1", 1),
		chMsg("B", "b1", 1),
		chMsg("A", "a2", 2),
		chMsg("A", "a3", 3),
		chMsg("B", "b2", 2),
	}
	if err := o.WriteBatch(context.Background(), batch); err != nil {
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

// TestWriteBatchRetryAfterPartialMultiChannelFailureDoesNotReAppend verifies
// a case the code comment on WriteBatch's error-return already claims but
// nothing had actually exercised: a batch spanning channels A and B, where
// A's group succeeds and B's group fails checkSubmissionOrder (B has
// already seen a higher token from some other concurrent submission).
// WriteBatch returns that error for the whole call -- which, on a real
// nack/redelivery, means the identical batch (A's already-landed messages
// included) flows through WriteBatch a second time.
//
// The claim under test: A's group must not be re-appended on that retry --
// preprocessForExactlyOncePipe's ordinary already-committed filter should
// catch it, exactly as it would for any single-channel redelivery, with
// checkSubmissionOrder never getting a chance to object (A's token on retry
// equals what this process already recorded, not less than it). B's group
// must still fail the same way, since nothing about a retry changes why it
// lost the race.
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

	err := o.WriteBatch(context.Background(), batch)
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
	// A's append landed for real (as far as the fake is concerned) --
	// advance its committed token the way a real channel status poll would
	// report after a successful commit, so the retry's dedup filter has
	// something to catch this against.
	chA.committed = "2"

	// Redeliver the identical batch, as Benthos would after this nack.
	err = o.WriteBatch(context.Background(), batch)
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

// TestWriteBatchWithoutOffsetTokenSkipsDedupAndUsesSyntheticCommitToken pins
// the fix noDedupCommitToken exists for: with offset_token unset (offsetToken
// nil), WriteBatch must skip preprocessForExactlyOncePipe entirely -- so it
// needs no @kafka_* (or any other) metadata to succeed -- and must still make
// forward progress waiting for commit, which requires a non-empty,
// call-unique synthetic token rather than the empty string. fakeChannel's
// WaitUntilCommitted now errors on an empty token specifically to catch a
// regression to the old (buggy) behaviour instead of silently passing.
func TestWriteBatchWithoutOffsetTokenSkipsDedupAndUsesSyntheticCommitToken(t *testing.T) {
	ch := &fakeChannel{name: "c"}
	o := testOutputNoDedup(t, ch)

	// Plain messages with zero Kafka (or any other) metadata: offset_token
	// being unset must not require any such metadata to be present.
	if err := o.WriteBatch(context.Background(), service.MessageBatch{msg("row-1"), msg("row-2")}); err != nil {
		t.Fatalf("WriteBatch (first batch): %v", err)
	}
	if err := o.WriteBatch(context.Background(), service.MessageBatch{msg("row-3")}); err != nil {
		t.Fatalf("WriteBatch (second batch): %v", err)
	}

	if len(ch.appends) != 2 {
		t.Fatalf("expected two appends (one per WriteBatch call), got %d: %+v", len(ch.appends), ch.appends)
	}
	if len(ch.appends[0]) != 2 || len(ch.appends[1]) != 1 {
		t.Fatalf("expected no rows filtered out (dedup is skipped when offset_token is unset), got %+v", ch.appends)
	}

	// Every append/commit token must be non-empty: an empty target can never
	// be satisfied by WaitUntilCommitted's real success condition (see
	// noDedupCommitToken's doc comment), and fakeChannel's WaitUntilCommitted
	// would already have errored above had one been empty -- these checks
	// are the more specific diagnostic if it ever does.
	for i, tok := range ch.startOffsets {
		if tok == "" {
			t.Errorf("append %d: start offset token is empty, want a non-empty synthetic token", i)
		}
	}
	if len(ch.waited) != 2 {
		t.Fatalf("expected two commit waits, got %d: %v", len(ch.waited), ch.waited)
	}

	// The two calls must get two DISTINCT synthetic tokens: a fixed or reused
	// value (empty string included) can't distinguish "this call's append
	// landed" from "an earlier call's append with the same token already
	// landed" -- exactly the defect noDedupCommitToken fixes.
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

// TestNoDedupSyntheticTokensStartAbovePreviousRunsCommittedToken pins the
// restart case noDedupCommitToken's wall-clock seeding exists for. A
// channel's name is stable across restarts by default, and Snowflake's
// committed token for it carries over. If a fresh process's synthetic
// sequence restarted at 1, every batch whose token fell at or below the
// previous run's last committed value would satisfy WaitUntilCommitted's
// "committed >= mine" check instantly -- acked without its own append ever
// being confirmed. Two directly-constructed outputs stand in for two
// process runs here: the second must mint tokens strictly newer than what
// the first left committed.
func TestNoDedupSyntheticTokensStartAbovePreviousRunsCommittedToken(t *testing.T) {
	firstRun := &fakeChannel{name: "c"}
	if err := testOutputNoDedup(t, firstRun).WriteBatch(context.Background(), service.MessageBatch{msg("row-1")}); err != nil {
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
	if err := testOutputNoDedup(t, secondRun).WriteBatch(context.Background(), service.MessageBatch{msg("row-2")}); err != nil {
		t.Fatalf("second run WriteBatch: %v", err)
	}
	if got := v2.CompareOffsetTokens(secondRun.waited[0], carriedOver); got != 1 {
		t.Errorf("second run's first token %q must compare newer than the previous run's committed %q (got %d)", secondRun.waited[0], carriedOver, got)
	}

	// Same again, but with a committed synthetic token from a previous run
	// whose clock was far AHEAD of this host's (a restored snapshot, an
	// unsynced failover node): the wall-clock seed alone can't get past
	// it, so checkSyntheticTokenSpace must bump the counter from the token
	// itself.
	farFuture := v2.FormatSyntheticToken(time.Now().Add(365 * 24 * time.Hour).UnixNano())
	secondRun = &fakeChannel{name: "c", committed: farFuture}
	carriedOver = farFuture
	if err := testOutputNoDedup(t, secondRun).WriteBatch(context.Background(), service.MessageBatch{msg("row-2")}); err != nil {
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

// TestNoDedupRejectedRowsWarnInsteadOfFailingTheBatch: with offset_token
// unset there is no dedup, so failing a batch over rows Snowflake rejected
// would make the redelivery re-insert every good row as a duplicate and hit
// the same rejection again, forever. Even with tolerate_row_errors: false the
// batch must therefore succeed, with the rejection warned about (naming the
// reason the batch isn't failed) and counted in the metric.
func TestNoDedupRejectedRowsWarnInsteadOfFailingTheBatch(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 1, rowsErrorCount: 1, lastErrorMessage: "cannot cast"}
	o := testOutputNoDedup(t, ch)
	o.tolerateRowErrors = false
	o.logger = captureLogger(&logBuf)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	if err := o.WriteBatch(context.Background(), service.MessageBatch{msg(`{"a":1}`), msg(`{"a":"bad"}`)}); err != nil {
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

// TestReopenAfterFailureReportsRejectedRowsBeforeReopening pins the fix for
// rejections being erased by a reopen: a commit-wait failure on a batch that
// also had a rejected row must still increment the metric and warn, before
// the reopened channel's fresh baseline swallows the count. On redelivery an
// exactly-once batch that had in fact committed dedupes to nothing, so this
// is the only place the rejection can ever be reported.
func TestReopenAfterFailureReportsRejectedRowsBeforeReopening(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 2, rowsErrorCount: 2, lastErrorMessage: "invalid JSON", waitErr: fmt.Errorf("status poll failed")}
	o := testOutput(t, ch)
	o.logger = captureLogger(&logBuf)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	err := o.WriteBatch(context.Background(), offsetBatch(t, []int{1, 2}))
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

// TestNoDedupProceedsOnChannelPreviouslyWrittenWithRealTokens covers the
// documented, supported direction of a mode switch: an operator removing
// offset_token from a pipeline whose channel already carries real (numeric
// or otherwise realistic) tokens. checkSyntheticTokenSpace must let it
// through -- the synthetic token compares above the real one, so the commit
// wait genuinely waits -- and must ask the channel for its token only once
// per process, not once per batch.
func TestNoDedupProceedsOnChannelPreviouslyWrittenWithRealTokens(t *testing.T) {
	for _, committed := range []string{"5000", "postgres-0/16B3748", "9223372036854775807"} {
		t.Run(committed, func(t *testing.T) {
			ch := &fakeChannel{name: "c", committed: committed}
			o := testOutputNoDedup(t, ch)
			if err := o.WriteBatch(context.Background(), service.MessageBatch{msg("row-1")}); err != nil {
				t.Fatalf("WriteBatch: %v", err)
			}
			if err := o.WriteBatch(context.Background(), service.MessageBatch{msg("row-2")}); err != nil {
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

// TestNoDedupRefusesChannelWhoseRealTokenOutranksSyntheticOnes covers the
// exotic remainder checkSyntheticTokenSpace exists for: a real committed
// token that nonetheless compares at or above a synthetic one (only
// possible for tokens starting with '~' or a non-ASCII byte). Every commit
// wait would succeed before its append was confirmed, so the batch must be
// refused, with nothing appended.
func TestNoDedupRefusesChannelWhoseRealTokenOutranksSyntheticOnes(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: "~zzz-not-ours"}
	err := testOutputNoDedup(t, ch).WriteBatch(context.Background(), service.MessageBatch{msg("row-1")})
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

// TestExactlyOnceRefusesChannelLastWrittenAtLeastOnce pins the guard in
// preprocessForExactlyOncePipe for the dangerous direction of a mode
// switch: offset_token ADDED to a pipeline whose channel already carries
// synthetic tokens from running without it. A synthetic token compares
// newer than every real one, so without the guard the per-row filter would
// drop every message as an already-committed duplicate -- and ack the
// batch. The batch must instead fail, every time, with nothing appended.
func TestExactlyOnceRefusesChannelLastWrittenAtLeastOnce(t *testing.T) {
	ch := &fakeChannel{name: "c", committed: v2.FormatSyntheticToken(time.Now().UnixNano())}
	o := testOutput(t, ch)
	for attempt := range 2 {
		err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6}))
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

// TestWriteBatchAllEmptyRowsSkipsCommitWaitAndAcks pins the output side of
// AppendRows' sent=false contract: a group whose rows all encode to nothing
// (a batch of tombstones) makes no request, so there is no token to wait
// for. WriteBatch must return nil without calling WaitUntilCommitted at all
// -- waiting would block for the full commit_timeout on every redelivery,
// forever -- and must count the skipped rows. Checked in both modes, since
// the exactly-once path additionally advances checkSubmissionOrder's
// watermark before the append.
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

			if err := o.WriteBatch(context.Background(), c.batch(t)); err != nil {
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
			if err := o.WriteBatch(context.Background(), follow); err != nil {
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

	if err := o.WriteBatch(context.Background(), service.MessageBatch{msg(`{"a":1}`), msg(""), msg(`{"a":2}`), msg(" ")}); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if len(ch.appends) != 1 || len(ch.waited) != 1 {
		t.Fatalf("appends=%d waited=%d, want 1 and 1", len(ch.appends), len(ch.waited))
	}
	if got := readCounter("snowflake_empty_rows_skipped"); got != 2 {
		t.Errorf("snowflake_empty_rows_skipped = %d, want 2", got)
	}
}

// captureLogger builds a *service.Logger backed by an slog text handler
// writing into buf, so a test can assert on which lines WriteBatch emitted.
// The default slog level (Info) excludes the package's Debugf noise while
// still catching Warnf, keeping assertions focused on the line under test.
func captureLogger(buf *bytes.Buffer) *service.Logger {
	return service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(buf, nil)))
}

// TestWriteBatchWarnsAndReportsRowsRejected exercises the errors.tolerance
// "all" equivalent: with tolerateRowErrors set, a nonzero RowsRejected()
// delta for the batch just committed must warn -- naming the channel, the
// delta, and the last error message -- and WriteBatch must still return nil.
// The default (tolerateRowErrors=false) behaviour is covered separately by
// TestWriteBatchFailsOnRowsRejectedByDefault.
func TestWriteBatchWarnsAndReportsRowsRejected(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 3, rowsErrorCount: 3, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)
	o.tolerateRowErrors = true
	o.logger = captureLogger(&logBuf)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6})); err != nil {
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

	// The tolerating mode must still report the metric: ReportRowsRejected
	// fires unconditionally in WriteBatch, before the tolerate/fail branch
	// (see output_snowflake_streaming_pipe.go). Assert the exact delta (3),
	// not merely a nonzero reading -- a counter incremented by the wrong
	// amount is exactly the bug this test exists to catch.
	if got := readCounter("snowflake_rows_error_count"); got != 3 {
		t.Errorf("snowflake_rows_error_count = %d, want 3 (the RowsRejected delta)", got)
	}
}

// TestWriteBatchFailsOnRowsRejectedByDefault exercises the errors.tolerance
// "none" default (tolerateRowErrors left false, matching the Kafka
// Connector's default): a nonzero RowsRejected() delta must fail the batch,
// and the returned error must name the channel plus the new/total/initial
// counts and Snowflake's last_error_message, mirroring
// SnowpipeStreamingPartitionChannel.handleChannelErrors's error format.
func TestWriteBatchFailsOnRowsRejectedByDefault(t *testing.T) {
	ch := &fakeChannel{name: "c", rowsRejected: 3, rowsErrorCount: 8, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)
	metrics, readCounter := newCountingMetrics(t)
	o.metrics = newSnowpipePipeMetrics(metrics)

	err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6}))
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

// TestWriteBatchDoesNotRefailOnStaleRowsRejected guards the anti-wedge
// property this design exists for: RowsRejected() is a delta that resets to
// 0 once reported (see streamingv2.Channel.RowsRejected), so a second batch
// on the same channel, with no new rejections, must not re-fail or re-warn
// on the same historical error -- even under the default failing mode.
func TestWriteBatchDoesNotRefailOnStaleRowsRejected(t *testing.T) {
	ch := &fakeChannel{name: "c", rowsRejected: 3, rowsErrorCount: 3, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)

	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6})); err == nil {
		t.Fatalf("expected the first batch to fail on the initial delta")
	}

	// The real Channel.RowsRejected only ever reports a given increase once
	// (its baseline advances on read); the fake must mirror that here since
	// it has no baseline logic of its own.
	ch.rowsRejected = 0
	var logBuf bytes.Buffer
	o.logger = captureLogger(&logBuf)
	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{7, 8})); err != nil {
		t.Fatalf("expected the second batch to succeed once RowsRejected reports no new delta, got: %v", err)
	}
	if logged := logBuf.String(); strings.Contains(logged, "WARN") {
		t.Errorf("expected no warning once the historical error was already reported, got: %s", logged)
	}
}

// TestWriteBatchDebugLogsPreExistingErrorsAtOpen exercises the Kafka
// Connector's "else if (currentErrorCount > 0)" branch: a channel that
// already had errors before this batch (RowsRejected()==0 because nothing
// new happened, but RowsErrorCount()>0 from history) must not fail or warn
// -- only debug-log -- so the failing-by-default mode doesn't refuse to
// start against any channel with pre-existing history.
func TestWriteBatchDebugLogsPreExistingErrorsAtOpen(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c", rowsRejected: 0, rowsErrorCount: 5, lastErrorMessage: "invalid JSON in row"}
	o := testOutput(t, ch)
	o.logger = captureLogger(&logBuf)

	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6})); err != nil {
		t.Fatalf("expected pre-existing-only errors not to fail the batch, got: %v", err)
	}
	if logged := logBuf.String(); strings.Contains(logged, "WARN") {
		t.Errorf("expected no WARN for pre-existing-only errors (debug only), got: %s", logged)
	}
}

// TestWriteBatchDoesNotWarnWhenNoRowsRejected guards against a regression
// that would resurface the reverted behaviour's symptom under a different
// name: warning on every batch regardless of RowsRejected would be just as
// useless to an operator as the old hard error, so a clean batch must stay
// silent.
func TestWriteBatchDoesNotWarnWhenNoRowsRejected(t *testing.T) {
	var logBuf bytes.Buffer
	ch := &fakeChannel{name: "c"}
	o := testOutput(t, ch)
	o.logger = captureLogger(&logBuf)

	if err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6})); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	if logged := logBuf.String(); strings.Contains(logged, "WARN") {
		t.Errorf("expected no warning when RowsRejected is 0, got: %s", logged)
	}
}

// TestWriteBatchDoesNotReopenChannelOnBackpressureAppendError pins gap B of
// the backpressure fix: a 429 append failure (Snowpipe Streaming rejecting a
// write for capacity reasons) must not trigger the reopen-on-failure path --
// the channel is still valid, so it goes back to the pool unchanged, and
// only the append error propagates. testOutputCountingOpens's counter must
// stay at 1: the single open the pool performed to service the initial
// Acquire, and no reopen on top of it.
func TestWriteBatchDoesNotReopenChannelOnBackpressureAppendError(t *testing.T) {
	ch := &fakeChannel{name: "c", appendErr: realAppendError(t, 429, `{"error_code":"ReceiverSaturated"}`)}
	o, opens := testOutputCountingOpens(t, ch)

	err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6}))
	if err == nil {
		t.Fatal("expected WriteBatch to propagate the backpressure append error")
	}
	if *opens != 1 {
		t.Errorf("open count = %d, want 1 (initial open only, no reopen on backpressure)", *opens)
	}
}

// TestWriteBatchReopensChannelOnNonBackpressureAppendError guards the other
// side of gap B: an append failure that is NOT backpressure (a plain 500
// here) must still go through the pre-existing reopen-on-failure path,
// unchanged. testOutputCountingOpens's counter must reach 2: the initial
// open plus the reopen WriteBatch performs after the failed append.
func TestWriteBatchReopensChannelOnNonBackpressureAppendError(t *testing.T) {
	ch := &fakeChannel{name: "c", appendErr: realAppendError(t, 500, `{"message":"internal error"}`)}
	o, opens := testOutputCountingOpens(t, ch)

	err := o.WriteBatch(context.Background(), offsetBatch(t, []int{5, 6}))
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

// configSpecHasField reports whether name is a field of
// snowpipeStreamingPipeOutputConfig's schema. It parses
// minimalStreamingPipeConfigYAML -- which sets every field the schema is
// expected to declare -- and checks Contains, the same existence check the
// production code already relies on (see newSnowflakeStreamerPipe's
// conf.Contains calls). A name that isn't part of the schema is never added
// to the parsed config regardless of what else is set, so this also
// correctly reports absence for removed fields, as long as they are never
// added to the probe YAML themselves.
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

// generatedPrivateKeyBase64 produces a fresh RSA key encoded as bare
// base64(PKCS8 DER), with no PEM header/footer or embedded newlines: it
// exercises getPrivateKey's base64 fallback branch (auth.go), which would
// otherwise need a literal multi-line PEM block shoehorned into a YAML
// scalar.
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

// testResources returns a *service.Resources with an enterprise license
// injected, for exercising newSnowpipeStreamingPipeOutput directly: that
// constructor calls license.CheckRunningEnterprise before anything else, so
// a bare service.MockResources() would fail every one of these tests on the
// license check rather than on the behaviour under test.
func testResources() *service.Resources {
	resources := service.MockResources()
	license.InjectTestService(resources)
	return resources
}

// discoveryProbeTransport is an http.RoundTripper that answers every request
// in-process (no real DNS/TCP/TLS) via connectDiscoveryHandler, and records
// the resolved scheme://host of each request keyed by URL path. It exists
// because newSnowflakeStreamerPipe's v2.Config never sets HTTPClient, so
// v2.NewClient falls back to http.DefaultTransport -- and v2.Client's cfg
// field is unexported with no getter, so this is the only way from this
// package to observe which host account_host/url actually reached inside
// Config, short of modifying streamingv2 (out of scope for this change).
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

// connectDiscoveryHandler mirrors the handler in
// streamingv2/client_test.go's TestConnectUsesDiscoveredHostWithoutOverride:
// it answers hostname discovery, the bulk-channel-status preflight probe,
// and (via the default case) the /oauth/token exchange, whose handler in
// token.go accepts any non-empty trimmed response body as a valid token. It
// lets v2.Client.Connect succeed end-to-end without touching the network.
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

// TestAccountHostReachesClientConfig asserts that the account_host field
// reaches streamingv2.Config.AccountHost -- observed indirectly via the
// control-plane host that v2.Client.Connect dials during hostname discovery
// -- and that leaving it unset still derives the previous
// <account>.snowflakecomputing.com default, unchanged for existing configs.
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
			if _, _, _, err := newSnowpipeStreamingPipeOutput(conf, testResources()); err != nil {
				t.Fatalf("newSnowpipeStreamingPipeOutput: %v", err)
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

// TestNewSnowflakeStreamerDefaultsChannelNameToConstantPipeChannel exercises
// the full config-parsing/construction path (newSnowpipeStreamingPipeOutput),
// not just the standalone defaultChannelName helper, to confirm that leaving
// channel_name unset produces a single, constant, unpartitioned channel
// scoped to <DATABASE>.<SCHEMA>.<pipe>. database/schema
// are normalized to uppercase the same way role already is (see
// newSnowflakeStreamerPipe's strings.ToUpper calls); pipe deliberately is not,
// so the expected name below keeps pipe's original casing. Resolving the
// channel needs no Kafka metadata at all. `url` points at a local in-process
// discovery handler so Connect() succeeds without any real network access.
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

// realAppendHandler extends connectDiscoveryHandler with the two additional
// endpoints AppendRows's harvesting round trip in realAppendError below
// needs: channel open (PUT .../channels/{name}), answered with a
// synthetic-but-valid SUCCESS status so OpenChannel itself never fails, and
// rows append (POST .../rows), answered with the caller's chosen status and
// body -- the response under test.
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

// realAppendError drives a real streamingv2.Client (Connect, OpenChannel,
// AppendRows) against a local realAppendHandler server and returns the error
// AppendRows produced for the given status/body. This package's test file
// cannot fabricate v2.IsBackpressure's unexported *httpStatusError directly
// -- IngestChannel/IsBackpressure are the only surface streamingv2 exports to
// this package -- so the only way to hand WriteBatch's fakeChannel a
// classifiable error is to harvest a genuine one exactly as production code
// would produce it.
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
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "ch1")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	_, err = ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "1", "1")
	if err == nil {
		t.Fatalf("expected AppendRows to fail against a %d append response, got nil error", appendStatus)
	}
	return err
}

// newCountingMetrics builds a *service.Metrics backed by a custom, in-process
// metrics exporter plugin, plus a reader closure that returns the current
// value of a named counter.
//
// This exists because benthos's public/service API has no direct constructor
// from a bare service.MetricsExporter to *service.Metrics: the only two ways
// to obtain a *service.Metrics are service.MockResources().Metrics() (which
// returns a no-op that never surfaces to a test) and Resources.Metrics() on
// a *service.Resources built by a service.ResourceBuilder. So this goes
// through the latter: register a MetricsExporter plugin on an isolated
// Environment (RegisterMetricsExporter), select it via SetMetricsYAML, and
// Build() the resources, which is what actually wires the registered
// exporter into the Resources this returns.
//
// One wrinkle drove the implementation below: service.MetricCounter.Incr
// calls cv.With(labelValues...).Incr(count) on every single Incr, and the
// airGapCounterVec.With that backs a custom exporter calls the registered
// MetricsExporterCounterCtor -- i.e. our ctor -- again on every call, not
// just once at NewCounter time. A counter implementation that only tracked
// its own state would silently reset on every increment. So the value has to
// live in the exporter itself, keyed by name, and the per-Incr object
// returned by the ctor must mutate that shared map rather than hold its own
// count.
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

	// ResourceBuilder.Build always initialises a tracer too (defaulting to
	// type "none"), but the "none" tracer provider is itself a plugin --
	// registered by internal/impl/pure/tracer_none.go's init() in the real
	// connector binary, which imports that package for its side effects.
	// This test binary only exercises internal/impl/snowflake, so that
	// init() never runs and NewEnvironment's clone has no "none" tracer
	// registered, which otherwise fails Build with "tracer type of 'none'
	// was not recognised". Register the same no-op provider directly on
	// our isolated env rather than pulling in an unrelated package import.
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
		if err := closeFn(context.Background()); err != nil {
			t.Errorf("newCountingMetrics: close: %v", err)
		}
	})

	return res.Metrics(), exporter.get
}

// countingMetricsExporter is a service.MetricsExporter that accumulates
// counter values in a mutex-guarded map keyed by metric name, ignoring
// labels: this plugin only needs to answer "what is the current value of
// metric X" for tests, not reproduce benthos's own label-vector fan-out.
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

// NewTimerCtor and NewGaugeCtor are no-ops: the production snowpipePipeMetrics
// also creates timers (snowflake_append_latency_ns, snowflake_commit_
// latency_ns), and newSnowpipePipeMetrics must not panic or fail when it calls
// m.NewTimer against this exporter, even though this task only asserts the
// counter.
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

// countingMetricsExporterCounter is the per-Incr handle returned by the
// ctor above. It carries no state of its own -- see the doc comment on
// newCountingMetrics for why -- and instead mutates the exporter's shared
// map on every Incr, exactly the accumulation a real counter performs.
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
