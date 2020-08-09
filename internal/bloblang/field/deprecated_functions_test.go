package field

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
)

func TestUUIDV4Function(t *testing.T) {
	results := map[string]struct{}{}
	fn, _ := query.DeprecatedFunction("uuid_v4", "")
	e := NewExpression(NewQueryResolver(fn))

	for i := 0; i < 100; i++ {
		res := e.String(0, message.New(nil))
		if _, exists := results[res]; exists {
			t.Errorf("Duplicate UUID generated: %v", res)
		}
		results[res] = struct{}{}
	}
}

func TestTimestamps(t *testing.T) {
	now := time.Now()

	fn, _ := query.DeprecatedFunction("timestamp_unix_nano", "")
	e := NewExpression(NewQueryResolver(fn))

	tStamp := e.String(0, message.New(nil))

	nanoseconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen := time.Unix(0, nanoseconds)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()

	fn, _ = query.DeprecatedFunction("timestamp_unix", "")
	e = NewExpression(NewQueryResolver(fn))

	tStamp = e.String(0, message.New(nil))

	seconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(seconds, 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	fn, _ = query.DeprecatedFunction("timestamp_unix", "10")
	e = NewExpression(NewQueryResolver(fn))

	tStamp = e.String(0, message.New(nil))

	var secondsF float64
	secondsF, err = strconv.ParseFloat(tStamp, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(int64(secondsF), 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	fn, _ = query.DeprecatedFunction("timestamp", "")
	e = NewExpression(NewQueryResolver(fn))

	tStamp = e.String(0, message.New(nil))

	tThen, err = time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	fn, _ = query.DeprecatedFunction("timestamp_utc", "")
	e = NewExpression(NewQueryResolver(fn))

	tStamp = e.String(0, message.New(nil))

	tThen, err = time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
	if !strings.Contains(tStamp, "UTC") {
		t.Errorf("Non-UTC timezone detected: %v", tStamp)
	}

	now = time.Now()
	fn, _ = query.DeprecatedFunction("timestamp_utc", "2006-01-02T15:04:05.000Z")
	e = NewExpression(NewQueryResolver(fn))

	tStamp = e.String(0, message.New(nil))

	tThen, err = time.Parse("2006-01-02T15:04:05.000Z", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
}
