package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrometheusNoPushGateway(t *testing.T) {
	config := NewConfig()

	p, err := NewPrometheus(config)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.Nil(t, p.(*Prometheus).pusher)
}

func TestPrometheusWithPushGateway(t *testing.T) {
	pusherChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		pusherChan <- struct{}{}
	}))
	defer server.Close()

	config := NewConfig()
	config.Prometheus.PushURL = server.URL

	p, err := NewPrometheus(config)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, p.(*Prometheus).pusher)

	go func() {
		err = p.Close()
		assert.NoError(t, err)
	}()

	// Wait for message for the PushGateway after close
	select {
	case <-pusherChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "PushGateway did not receive expected messages")
	}
}

func TestPrometheusWithPushGatewayAndPushInterval(t *testing.T) {
	pusherChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		pusherChan <- struct{}{}
	}))
	defer server.Close()

	pushInterval := 1 * time.Millisecond
	config := NewConfig()
	config.Prometheus.PushURL = server.URL
	config.Prometheus.PushInterval = pushInterval.String()

	p, err := NewPrometheus(config)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, p.(*Prometheus).pusher)

	// Wait for first message for the PushGateway
	select {
	case <-pusherChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "PushGateway did not receive expected messages")
	}

	go func() {
		err = p.Close()
		assert.NoError(t, err)
	}()

	// Wait for another message for the PushGateway (might not be the one sent on close)
	select {
	case <-pusherChan:
	case <-time.After(100 * time.Millisecond):
		assert.Fail(t, "PushGateway did not receive expected messages after close")
	}
}

func getTestProm(t *testing.T) (Type, http.HandlerFunc) {
	t.Helper()

	conf := NewConfig()
	conf.Prometheus.Prefix = ""
	conf.Type = TypePrometheus

	prom, err := New(conf)
	require.NoError(t, err)

	wHandler, ok := prom.(WithHandlerFunc)
	require.True(t, ok)

	return prom, wHandler.HandlerFunc()
}

func getPage(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()

	req := httptest.NewRequest("GET", "http://example.com/foo", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)

	return string(body)
}

func TestPrometheusMetrics(t *testing.T) {
	nm, handler := getTestProm(t)

	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)

	ctrTwo := nm.GetCounterVec("countertwo", []string{"label1"})
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", []string{"label2"})
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", []string{"label3", "label4"})
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ntimerone_sum 13")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
}
