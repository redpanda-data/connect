// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package writer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/auth"
	"github.com/Jeffail/benthos/lib/util/text"
	"github.com/olivere/elastic"
)

//------------------------------------------------------------------------------

// ElasticsearchConfig is configuration for the Elasticsearch output type.
type ElasticsearchConfig struct {
	URLs      []string             `json:"urls" yaml:"urls"`
	ID        string               `json:"id" yaml:"id"`
	Index     string               `json:"index" yaml:"index"`
	TimeoutMS int                  `json:"timeout_ms" yaml:"timeout_ms"`
	Auth      auth.BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
}

// NewElasticsearchConfig creates a new ElasticsearchConfig with default values.
func NewElasticsearchConfig() ElasticsearchConfig {
	return ElasticsearchConfig{
		URLs:      []string{"http://localhost:9200"},
		ID:        "${!count:elastic_ids}-${!timestamp_unix}",
		Index:     "benthos_index",
		TimeoutMS: 5000,
		Auth:      auth.NewBasicAuthConfig(),
	}
}

//------------------------------------------------------------------------------

// Elasticsearch is a writer type that writes messages into elasticsearch.
type Elasticsearch struct {
	log   log.Modular
	stats metrics.Type

	urls []string
	conf ElasticsearchConfig

	idBytes       []byte
	interpolateID bool

	client *elastic.Client
}

// NewElasticsearch creates a new Elasticsearch writer type.
func NewElasticsearch(conf ElasticsearchConfig, log log.Modular, stats metrics.Type) (*Elasticsearch, error) {
	idBytes := []byte(conf.ID)
	interpolateID := text.ContainsFunctionVariables(idBytes)

	e := Elasticsearch{
		log:           log.NewModule(".output.elasticsearch"),
		stats:         stats,
		conf:          conf,
		idBytes:       idBytes,
		interpolateID: interpolateID,
	}

	for _, u := range conf.URLs {
		for _, splitURL := range strings.Split(u, ",") {
			if len(splitURL) > 0 {
				e.urls = append(e.urls, splitURL)
			}
		}
	}

	return &e, nil
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to a Elasticsearch broker.
func (e *Elasticsearch) Connect() error {
	if e.client != nil {
		return nil
	}

	opts := []elastic.ClientOptionFunc{
		elastic.SetURL(e.urls...),
		elastic.SetHttpClient(&http.Client{
			Timeout: time.Duration(e.conf.TimeoutMS) * time.Millisecond,
		}),
	}

	if e.conf.Auth.Enabled {
		opts = append(opts, elastic.SetBasicAuth(
			e.conf.Auth.Username, e.conf.Auth.Password,
		))
	}

	var err error
	if e.client, err = elastic.NewClient(opts...); err != nil {
		return err
	}

	if err == nil {
		var indexExists bool
		indexExists, err = e.client.IndexExists(e.conf.Index).Do(context.Background())
		if err == nil && !indexExists {
			err = fmt.Errorf("index '%v' does not exist", e.conf.Index)
		}
	}

	if err == nil {
		e.log.Infof("Sending messages to Elasticsearch index at urls: %s\n", e.urls)
	}
	return err
}

// Write will attempt to write a message to Elasticsearch, wait for acknowledgement, and
// returns an error if applicable.
func (e *Elasticsearch) Write(msg types.Message) error {
	if e.client == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, part []byte) error {
		id := e.idBytes
		if e.interpolateID {
			id = text.ReplaceFunctionVariables(id)
		}

		_, err := e.client.Index().
			Index(e.conf.Index).
			Type("doc").
			Id(string(id)).
			BodyString(string(part)).
			Do(context.Background())

		return err
	})
}

// CloseAsync shuts down the Elasticsearch writer and stops processing messages.
func (e *Elasticsearch) CloseAsync() {
}

// WaitForClose blocks until the Elasticsearch writer has closed down.
func (e *Elasticsearch) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
