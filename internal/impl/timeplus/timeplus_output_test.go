package timeplus

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestOutputTimeplus(t *testing.T) {
	env := service.NewEnvironment()

	t.Run("Fail if workspace is empty", func(t *testing.T) {
		outputConfig := `
url: http://localhost:8000
stream: mystream
`
		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		_, _, _, err = newTimeplusOutput(conf, service.MockResources())
		require.ErrorContains(t, err, "workspace")
	})

	t.Run("Successful send data to local Timeplus Enterprise", func(t *testing.T) {
		ch := make(chan bool)
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodPost, req.Method)
			require.Equal(t, "/default/api/v1beta2/streams/mystream/ingest", req.RequestURI)

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, "{\"columns\":[\"col1\",\"col2\",\"col3\"],\"data\":[[\"hello\",5,50],[\"world\",10,100]]}", string(body))

			require.Equal(t, "application/json", req.Header.Get("Content-Type"))

			close(ch)
		}))

		outputConfig := fmt.Sprintf(`
url: %s
workspace: default
stream: mystream
`, svr.URL)

		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		out, _, _, err := newTimeplusOutput(conf, service.MockResources())
		require.NoError(t, err)

		err = out.Connect(t.Context())
		require.NoError(t, err)

		content1 := map[string]any{
			"col1": "hello",
			"col2": 5,
			"col3": 50,
		}

		content2 := map[string]any{
			"col1": "world",
			"col2": 10,
			"col3": 100,
		}

		msg1 := service.NewMessage(nil)
		msg1.SetStructured(content1)

		msg2 := service.NewMessage(nil)
		msg2.SetStructured(content2)

		batch := service.MessageBatch{
			msg1,
			msg2,
		}
		err = out.WriteBatch(t.Context(), batch)
		require.NoError(t, err)

		<-ch

		err = out.Close(t.Context())
		require.NoError(t, err)
	})

	t.Run("Successful send data to remote Timeplus Enterprise", func(t *testing.T) {
		ch := make(chan bool)
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

			require.Equal(t, http.MethodPost, req.Method)
			require.Equal(t, "/nextgen/api/v1beta2/streams/test_rp/ingest", req.RequestURI)

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, "{\"columns\":[\"col1\",\"col2\",\"col3\",\"col4\"],\"data\":[[\"hello\",5,false,3.14],[\"world\",10,true,3.1415926]]}", string(body))

			require.Equal(t, "application/json", req.Header.Get("Content-Type"))
			require.Equal(t, "7v3fHptcgZBBkFyi4qpG1-scsUnrLbLLgA2PFXTy0H-bcqVBF5iPdU3KG1_k", req.Header.Get("X-Api-Key"))

			close(ch)
		}))

		outputConfig := fmt.Sprintf(`
url: %s
workspace: nextgen
stream: test_rp
apikey: 7v3fHptcgZBBkFyi4qpG1-scsUnrLbLLgA2PFXTy0H-bcqVBF5iPdU3KG1_k
`, svr.URL)

		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		out, _, _, err := newTimeplusOutput(conf, service.MockResources())
		require.NoError(t, err)

		err = out.Connect(t.Context())
		require.NoError(t, err)

		content1 := map[string]any{
			"col1": "hello",
			"col2": 5,
			"col3": false,
			"col4": 3.14,
		}

		content2 := map[string]any{
			"col1": "world",
			"col2": 10,
			"col3": true,
			"col4": 3.1415926,
		}

		msg1 := service.NewMessage(nil)
		msg1.SetStructured(content1)

		msg2 := service.NewMessage(nil)
		msg2.SetStructured(content2)

		batch := service.MessageBatch{
			msg1,
			msg2,
		}
		err = out.WriteBatch(t.Context(), batch)
		require.NoError(t, err)

		<-ch

		err = out.Close(t.Context())
		require.NoError(t, err)
	})

}

func TestOutputTimeplusd(t *testing.T) {
	env := service.NewEnvironment()

	t.Run("Successful ingest data", func(t *testing.T) {
		ch := make(chan bool)
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

			require.Equal(t, http.MethodPost, req.Method)
			require.Equal(t, "/timeplusd/v1/ingest/streams/mystream", req.RequestURI)

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, "{\"columns\":[\"col1\"],\"data\":[[\"hello\"],[\"world\"]]}", string(body))

			require.Equal(t, "application/json", req.Header.Get("Content-Type"))
			require.Equal(t, "Basic ZGVmYXVsdDpoZWxsbw==", req.Header.Get("Authorization"))

			close(ch)
		}))

		outputConfig := fmt.Sprintf(`
target: timeplusd
url: %s
stream: mystream
username: default
password: hello
`, svr.URL)

		conf, err := outputConfigSpec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		out, _, _, err := newTimeplusOutput(conf, service.MockResources())
		require.NoError(t, err)

		err = out.Connect(t.Context())
		require.NoError(t, err)

		content1 := map[string]any{
			"col1": "hello",
		}

		content2 := map[string]any{
			"col1": "world",
		}

		msg1 := service.NewMessage(nil)
		msg1.SetStructured(content1)

		msg2 := service.NewMessage(nil)
		msg2.SetStructured(content2)

		batch := service.MessageBatch{
			msg1,
			msg2,
		}
		err = out.WriteBatch(t.Context(), batch)
		require.NoError(t, err)

		err = out.Close(t.Context())
		require.NoError(t, err)
	})

}
