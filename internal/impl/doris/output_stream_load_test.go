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

package doris

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestEncodeBatchAsJSONLines(t *testing.T) {
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1}`)),
		service.NewMessage([]byte(`{"id":2}`)),
	}

	body, err := encodeBatchAsJSON(batch, true, false)
	require.NoError(t, err)
	assert.Equal(t, "{\"id\":1}\n{\"id\":2}", string(body))
}

func TestEncodeBatchAsJSONArray(t *testing.T) {
	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1}`)),
		service.NewMessage([]byte(`{"id":2}`)),
	}

	body, err := encodeBatchAsJSON(batch, false, true)
	require.NoError(t, err)
	assert.JSONEq(t, `[{"id":1},{"id":2}]`, string(body))
}

func TestEncodeBatchAsCSV(t *testing.T) {
	batch := service.MessageBatch{
		service.NewMessage([]byte("1,alice")),
		service.NewMessage([]byte("2,bob")),
	}

	body, err := encodeBatchAsCSV(batch, "\n")
	require.NoError(t, err)
	assert.Equal(t, "1,alice\n2,bob", string(body))
}

func TestEscapeDorisHeaderValue(t *testing.T) {
	assert.Equal(t, "\\n", escapeDorisHeaderValue("\n"))
	assert.Equal(t, "\\t", escapeDorisHeaderValue("\t"))
	assert.Equal(t, ",", escapeDorisHeaderValue(","))
}

func TestClassifyDorisStreamLoadResponse(t *testing.T) {
	require.NoError(t, classifyDorisStreamLoadResponse(http.StatusOK, dorisStreamLoadResponse{
		Status: dsStatusSuccess,
	}, []byte(`{"Status":"Success"}`)))

	require.NoError(t, classifyDorisStreamLoadResponse(http.StatusOK, dorisStreamLoadResponse{
		Status:            dsStatusLabelExists,
		ExistingJobStatus: dsExistingJobFinished,
	}, []byte(`{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED"}`)))

	err := classifyDorisStreamLoadResponse(http.StatusOK, dorisStreamLoadResponse{
		Status: dsStatusPublishTimeout,
	}, []byte(`{"Status":"Publish Timeout"}`))
	require.Error(t, err)

	err = classifyDorisStreamLoadResponse(http.StatusOK, dorisStreamLoadResponse{
		Status: dsStatusLabelExists,
		Msg:    "There is no 100-continue header",
	}, []byte(`{"status":"FAILED","msg":"There is no 100-continue header"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "There is no 100-continue header")
}

func TestDorisStreamLoadOutputWriteBatchRedirect(t *testing.T) {
	t.Log("Given: a FE that redirects to a BE and a batch of JSON messages")

	var (
		feRequests atomic.Int32
		beRequests atomic.Int32
		feBody     atomic.Value
		beBody     atomic.Value
		beLabel    atomic.Value
	)

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		beRequests.Add(1)
		body, err := ioReadAll(r)
		require.NoError(t, err)
		beBody.Store(string(body))
		beLabel.Store(r.Header.Get(dsHeaderLabel))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","Message":"OK","NumberLoadedRows":2}`))
	}))
	defer be.Close()

	fe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		feRequests.Add(1)
		body, err := ioReadAll(r)
		require.NoError(t, err)
		feBody.Store(string(body))
		w.Header().Set(dsHeaderLocation, be.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer fe.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		URL:             fe.URL,
		Database:        "db",
		Table:           "tbl",
		Username:        "user",
		Password:        "pass",
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1}`)),
		service.NewMessage([]byte(`{"id":2}`)),
	}

	t.Log("When: the batch is written")
	err = out.WriteBatch(context.Background(), batch)
	require.NoError(t, err)

	t.Log("Then: FE is called once, BE is called once, and the BE receives the encoded batch")
	assert.Equal(t, int32(1), feRequests.Load())
	assert.Equal(t, int32(1), beRequests.Load())
	assert.Equal(t, "{\"id\":1}\n{\"id\":2}", feBody.Load())
	assert.Equal(t, "{\"id\":1}\n{\"id\":2}", beBody.Load())
	assert.NotEmpty(t, beLabel.Load())
}

func TestDorisStreamLoadOutputWriteBatchFailoverFE(t *testing.T) {
	t.Log("Given: multiple FE endpoints where the first is unreachable and the second redirects to a BE")

	var (
		feRequests atomic.Int32
		beRequests atomic.Int32
	)

	badFE := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	badURL := badFE.URL
	badFE.Close()

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		beRequests.Add(1)
		body, err := ioReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, "{\"id\":1}\n{\"id\":2}", string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","Message":"OK","NumberLoadedRows":2}`))
	}))
	defer be.Close()

	goodFE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		feRequests.Add(1)
		_, err := ioReadAll(r)
		require.NoError(t, err)
		w.Header().Set(dsHeaderLocation, be.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer goodFE.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		FEURLs:          []string{badURL, goodFE.URL},
		Database:        "db",
		Table:           "tbl",
		Username:        "user",
		Password:        "pass",
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1}`)),
		service.NewMessage([]byte(`{"id":2}`)),
	}

	t.Log("When: the batch is written")
	err = out.WriteBatch(context.Background(), batch)
	require.NoError(t, err)

	t.Log("Then: the sink fails over to the second FE and still reaches the BE")
	assert.Equal(t, int32(1), feRequests.Load())
	assert.Equal(t, int32(1), beRequests.Load())
}

func TestGetOrCreateLabelReusesMetadata(t *testing.T) {
	out := &dorisStreamLoadOutput{
		conf: dorisStreamLoadConfig{
			LabelPrefix: "doris_test",
		},
	}

	batch := service.MessageBatch{
		service.NewMessage([]byte(`{"id":1}`)),
		service.NewMessage([]byte(`{"id":2}`)),
	}

	first := out.getOrCreateLabel(batch)
	second := out.getOrCreateLabel(batch)

	assert.Equal(t, first, second)
}

func TestConnectionTest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		URL:             srv.URL,
		Database:        "db",
		Table:           "tbl",
		Username:        "user",
		Password:        "",
		QueryPort:       0,
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)

	results := out.ConnectionTest(context.Background())
	require.Len(t, results, 1)
	assert.NoError(t, results[0].Err)
}

func TestConnectionTestFailoverFE(t *testing.T) {
	badFE := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	badURL := badFE.URL
	badFE.Close()

	goodFE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer goodFE.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		FEURLs:          []string{badURL, goodFE.URL},
		Database:        "db",
		Table:           "tbl",
		Username:        "user",
		Password:        "",
		QueryPort:       0,
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)

	results := out.ConnectionTest(context.Background())
	require.Len(t, results, 1)
	assert.NoError(t, results[0].Err)
}

func TestConnectionTestIncludesQueryPortAndTableCheck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		URL:             srv.URL,
		Database:        "db",
		Table:           "tbl",
		Username:        "user",
		Password:        "",
		QueryPort:       9030,
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)
	out.connectionCheckFn = func(_ context.Context, feURL string, queryPort int) error {
		assert.Equal(t, srv.URL, feURL)
		assert.Equal(t, 9030, queryPort)
		return nil
	}

	results := out.ConnectionTest(context.Background())
	require.Len(t, results, 1)
	assert.NoError(t, results[0].Err)
}

func TestConnectionTestReportsTableMissing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		URL:             srv.URL,
		Database:        "db",
		Table:           "missing_tbl",
		Username:        "user",
		Password:        "",
		QueryPort:       9030,
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)
	out.connectionCheckFn = func(_ context.Context, _ string, queryPort int) error {
		return fmt.Errorf("Doris table db.missing_tbl was not found via query_port 127.0.0.1:%d", queryPort)
	}

	results := out.ConnectionTest(context.Background())
	require.Len(t, results, 1)
	require.Error(t, results[0].Err)
	assert.Contains(t, results[0].Err.Error(), "missing_tbl")
	assert.Contains(t, results[0].Err.Error(), "query_port")
}

func TestConnectionTestFailoverFEWithQueryPortCheck(t *testing.T) {
	badFE := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	badURL := badFE.URL
	badFE.Close()

	goodFE := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer goodFE.Close()

	out, err := newDorisStreamLoadOutput(dorisStreamLoadConfig{
		FEURLs:          []string{badURL, goodFE.URL},
		Database:        "db",
		Table:           "tbl",
		Username:        "user",
		Password:        "",
		QueryPort:       9030,
		Format:          "json",
		ReadJSONByLine:  true,
		LabelPrefix:     "doris_test",
		Timeout:         5 * time.Second,
		ColumnSeparator: dsDefaultColumnSep,
		LineDelimiter:   dsDefaultLineDelimiter,
	}, service.MockResources())
	require.NoError(t, err)

	var checks atomic.Int32
	out.connectionCheckFn = func(_ context.Context, feURL string, queryPort int) error {
		checks.Add(1)
		assert.Equal(t, 9030, queryPort)
		assert.Equal(t, goodFE.URL, feURL)
		return nil
	}

	results := out.ConnectionTest(context.Background())
	require.Len(t, results, 1)
	assert.NoError(t, results[0].Err)
	assert.Equal(t, int32(1), checks.Load())
}

func TestNewRequestSetsPromotedHeaders(t *testing.T) {
	maxFilter := 0.25
	skipLines := 2
	out := &dorisStreamLoadOutput{
		conf: dorisStreamLoadConfig{
			Database:         "db",
			Table:            "tbl",
			Format:           "csv",
			Columns:          []string{"id", "name"},
			Where:            "id > 0",
			ColumnSeparator:  ",",
			LineDelimiter:    "\n",
			GroupCommit:      "off_mode",
			MaxFilterRatio:   &maxFilter,
			Partitions:       []string{"p1", "p2"},
			SkipLines:        &skipLines,
			EmptyAsNull:      true,
			TrimDoubleQuotes: true,
			NumAsString:      true,
			FuzzyParse:       true,
			LabelPrefix:      "doris_test",
			Timeout:          5 * time.Second,
			Headers: map[string]string{
				"custom-header": "custom-value",
			},
		},
	}

	req, err := out.newRequest(context.Background(), "http://example.com", "label-1", []byte("1,alice"))
	require.NoError(t, err)

	assert.Equal(t, "label-1", req.Header.Get(dsHeaderLabel))
	assert.Equal(t, "csv", req.Header.Get(dsHeaderFormat))
	assert.Equal(t, "id,name", req.Header.Get(dsHeaderColumns))
	assert.Equal(t, "id > 0", req.Header.Get(dsHeaderWhere))
	assert.Equal(t, "off_mode", req.Header.Get(dsHeaderGroupCommit))
	assert.Equal(t, "0.25", req.Header.Get(dsHeaderMaxFilterRatio))
	assert.Equal(t, "p1,p2", req.Header.Get(dsHeaderPartitions))
	assert.Equal(t, "2", req.Header.Get(dsHeaderSkipLines))
	assert.Equal(t, "true", req.Header.Get(dsHeaderEmptyAsNull))
	assert.Equal(t, "true", req.Header.Get(dsHeaderTrimQuotes))
	assert.Equal(t, "true", req.Header.Get(dsHeaderNumAsString))
	assert.Equal(t, "true", req.Header.Get(dsHeaderFuzzyParse))
	assert.Equal(t, ",", req.Header.Get(dsHeaderColumnSeparator))
	assert.Equal(t, "\\n", req.Header.Get(dsHeaderLineDelimiter))
	assert.Equal(t, "custom-value", req.Header.Get("custom-header"))
	assert.Equal(t, "100-continue", req.Header.Get("Expect"))
}

func TestConfigValidationRejectsPartitionMix(t *testing.T) {
	pConf, err := dorisStreamLoadSpec().ParseYAML(`
url: http://localhost:8030
database: test
table: foo
username: root
password: ""
format: json
read_json_by_line: true
partitions: [p1]
temporary_partitions: [tp1]
`, nil)
	require.NoError(t, err)

	_, err = dorisStreamLoadConfigFromParsed(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partitions and temporary_partitions")
}

func ioReadAll(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}
