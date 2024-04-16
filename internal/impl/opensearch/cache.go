package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/opensearch-project/opensearch-go/v3/opensearchapi"
)

func opensearchCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("1.0.0").
		Summary(`Use a OpenSearch instance as a cache. Only GET is supported`).
		Fields(service.NewStringListField(esoFieldURLs)).
		Fields(httpclient.BasicAuthField()).
		Fields(OAuthAuthField()).
		Fields(
			service.NewStringField("index").
				Description("The name of the target index."),
			service.NewStringField("key_field").
				Description("The field in the document that is used as the key."),
			service.NewStringField("value_field").
				Description("The field in the document that is used as the value.").Optional(),
		)
}

func init() {
	err := service.RegisterCache(
		"opensearch", opensearchCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newOpensearchCacheFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newOpensearchCacheFromConfig(parsedConf *service.ParsedConfig, mgr *service.Resources) (*opensearchCache, error) {
	conf, err := esoConfigFromParsed(parsedConf, mgr)
	if err != nil {
		return nil, err
	}

	indexName, err := parsedConf.FieldString("index")
	if err != nil {
		return nil, err
	}

	keyField, err := parsedConf.FieldString("key_field")
	if err != nil {
		return nil, err
	}

	valueField, err := parsedConf.FieldString("value_field")
	if err != nil {
		return nil, err
	}

	return newOpensearchCache(indexName, keyField, valueField, conf)
}

//------------------------------------------------------------------------------

type opensearchCache struct {
	client    *opensearchapi.Client
	indexName string

	keyField   string
	valueField string
}

func newOpensearchCache(indexName, keyField, valueField string, conf esoConfig) (*opensearchCache, error) {

	client, err := opensearchapi.NewClient(conf.clientOpts)
	if err != nil {
		return nil, err
	}

	return &opensearchCache{
		client:     client,
		indexName:  indexName,
		keyField:   keyField,
		valueField: valueField,
	}, nil
}

func (m *opensearchCache) Get(ctx context.Context, key string) ([]byte, error) {
	query := fmt.Sprintf(`{
		"query": {
		  "term": {
			"%s": {
			  "value": "%s"
			}
		  }
		}
	  }`, m.keyField, key)

	search := &opensearchapi.SearchReq{
		Indices: []string{m.indexName},
		Body:    strings.NewReader(query),
		Params: opensearchapi.SearchParams{
			Size: opensearchapi.ToPointer(1),
		},
	}

	searchResponse, err := m.client.Search(ctx, search)

	if err != nil {
		return nil, fmt.Errorf("error searching for key %s: %v", key, err)
	}

	if searchResponse.Hits.Total.Value == 0 {
		return nil, service.ErrKeyNotFound
	}

	if m.valueField != "" {
		var message map[string]interface{}
		err = json.Unmarshal(searchResponse.Hits.Hits[0].Source, &message)
		if err != nil {
			return nil, fmt.Errorf("error getting field from document %s: %v", m.valueField, err)
		}
		var val, ok = message[m.valueField].(string)
		if ok {
			return []byte(val), nil
		}
		return nil, fmt.Errorf("error getting field from document %s: %v", m.valueField, val)
	} else {
		return searchResponse.Hits.Hits[0].Source, nil // return the entire document
	}

}

func (m *opensearchCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	return errors.New("not implemented")
	// document := map[string]interface{}{
	// 	m.keyField:   key,
	// 	m.valueField: string(value),
	// }

	// req := &opensearchapi.IndexReq{
	// 	Index: m.collection,
	// 	Body:  bytes.NewReader(encodeJSON(document)),
	// }
	// m.client.Index(ctx, req)
}

func (m *opensearchCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	return errors.New("not implemented")
}

func (m *opensearchCache) Delete(ctx context.Context, key string) error {
	return errors.New("not implemented")
}

func (m *opensearchCache) Close(ctx context.Context) error {
	return nil
}
