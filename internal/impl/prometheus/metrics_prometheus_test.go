package prometheus

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestPrometheusNoPushGateway(t *testing.T) {
	config := metrics.NewConfig()

	p, err := newPrometheus(config, mock.NewManager())
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.Nil(t, p.(*prometheusMetrics).pusher)
}

func TestPrometheusWithPushGateway(t *testing.T) {
	pusherChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		pusherChan <- struct{}{}
	}))
	defer server.Close()

	config := metrics.NewConfig()
	config.Prometheus.PushURL = server.URL

	p, err := newPrometheus(config, mock.NewManager())
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, p.(*prometheusMetrics).pusher)

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
	config := metrics.NewConfig()
	config.Prometheus.PushURL = server.URL
	config.Prometheus.PushInterval = pushInterval.String()

	p, err := newPrometheus(config, mock.NewManager())
	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.NotNil(t, p.(*prometheusMetrics).pusher)

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

func getTestProm(t *testing.T) (metrics.Type, http.HandlerFunc) {
	t.Helper()

	prom, err := newPrometheus(metrics.NewConfig(), mock.NewManager())
	require.NoError(t, err)

	return prom, prom.HandlerFunc()
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

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)

	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(13)

	body := getPage(t, handler)

	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ntimerone_count 1")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 13")
}

func TestPrometheusHistMetrics(t *testing.T) {
	conf := metrics.NewConfig()
	conf.Prometheus.UseHistogramTiming = true

	nm, err := newPrometheus(conf, mock.NewManager())
	require.NoError(t, err)

	applyTestMetrics(nm)

	tmr := nm.GetTimer("timerone")
	tmr.Timing(13)
	tmrTwo := nm.GetTimerVec("timertwo", "label3", "label4")
	tmrTwo.With("value4", "value5").Timing(14)

	handler := nm.HandlerFunc()
	body := getPage(t, handler)

	assertContainsTestMetrics(t, body)
	assert.Contains(t, body, "\ntimerone_sum 1.3e-08")
	assert.Contains(t, body, "\ntimertwo_sum{label3=\"value4\",label4=\"value5\"} 1.4e-08")
}

func TestPrometheusWithFileOutputPath(t *testing.T) {
	config := metrics.NewConfig()
	config.Prometheus.FileOutputPath = os.TempDir() + "/benthos_metrics.prom"

	defer os.Remove(config.Prometheus.FileOutputPath)

	p, err := newPrometheus(config, mock.NewManager())
	applyTestMetrics(p)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	assert.Nil(t, p.(*prometheusMetrics).pusher)
	assert.Equal(t, config.Prometheus.FileOutputPath, p.(*prometheusMetrics).fileOutputPath)

	err = p.Close()
	assert.NoError(t, err)

	assert.FileExists(t, config.Prometheus.FileOutputPath)
	file, err := os.ReadFile(config.Prometheus.FileOutputPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, file)

	assertContainsTestMetrics(t, string(file))
}

func applyTestMetrics(nm metrics.Type) {
	ctr := nm.GetCounter("counterone")
	ctr.Incr(10)
	ctr.Incr(11)

	gge := nm.GetGauge("gaugeone")
	gge.Set(12)

	ctrTwo := nm.GetCounterVec("countertwo", "label1")
	ctrTwo.With("value1").Incr(10)
	ctrTwo.With("value2").Incr(11)

	ggeTwo := nm.GetGaugeVec("gaugetwo", "label2")
	ggeTwo.With("value3").Set(12)
}

func assertContainsTestMetrics(t *testing.T, body string) {
	assert.Contains(t, body, "\ncounterone 21")
	assert.Contains(t, body, "\ngaugeone 12")
	assert.Contains(t, body, "\ncountertwo{label1=\"value1\"} 10")
	assert.Contains(t, body, "\ncountertwo{label1=\"value2\"} 11")
	assert.Contains(t, body, "\ngaugetwo{label2=\"value3\"} 12")
}
