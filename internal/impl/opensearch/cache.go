package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
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
		Summary(`Use a OpenSearch instance as a cache. The get operator can be used to look into any existing document in Opensearch`).
		Fields(service.NewStringListField(esoFieldURLs)).
		Fields(httpclient.BasicAuthField()).
		Fields(OAuthAuthField()).
		Fields(service.NewTLSToggledField(esoFieldTLS)).
		Fields(
			service.NewStringField("index").
				Description("The name of the target index."),
			service.NewStringField("key_field").
				Description("Not used together with get,set and delete operatior. The field in the document that is used as the key. If not set, it will use the _id on the document.").
				Advanced().
				Optional(),
			service.NewStringField("value_field").
				Description("The field in the document that is used as the value. If set to empty, it will retrieve the entire document").
				Default("value").
				Optional(),
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
	conf, err := esoClientConfigFromParsed(parsedConf, mgr)
	if err != nil {
		return nil, err
	}

	indexName, err := parsedConf.FieldString("index")
	if err != nil {
		return nil, err
	}

	keyField, _ := parsedConf.FieldString("key_field")

	valueField, _ := parsedConf.FieldString("value_field")

	return newOpensearchCache(indexName, keyField, valueField, conf)
}

type opensearchCache struct {
	client    *opensearchapi.Client
	indexName string

	keyField   string
	valueField string
}

func newOpensearchCache(indexName, keyField, valueField string, clientOpts opensearchapi.Config) (*opensearchCache, error) {

	client, err := opensearchapi.NewClient(clientOpts)
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
	var searchHit json.RawMessage
	if m.keyField == "" {
		documentResponse, err := m.client.Document.Get(ctx, opensearchapi.DocumentGetReq{
			Index:      m.indexName,
			DocumentID: key,
		})

		if err != nil {
			return nil, fmt.Errorf("error getting document %s: %v", key, err)
		}

		if !documentResponse.Found {
			return nil, service.ErrKeyNotFound
		}

		searchHit = documentResponse.Source
	} else {

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
		searchHit = searchResponse.Hits.Hits[0].Source
	}

	if m.valueField == "" {
		json, _ := searchHit.MarshalJSON()
		return json, nil
	}

	var message map[string]interface{}
	err := json.Unmarshal(searchHit, &message)
	if err != nil {
		return nil, fmt.Errorf("error getting field from document %s: %v", m.valueField, err)
	}
	var val, ok = message[m.valueField].(string)
	if ok {
		return []byte(val), nil
	}
	return nil, fmt.Errorf("error getting field from document %s: %v", m.valueField, val)

}

func (m *opensearchCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	if m.keyField != "" {
		return fmt.Errorf("key_field is used, cannot be used with set operator. key_field is only supported for get")
	}
	return index(ctx, m, value, key, "index")

}

func (m *opensearchCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	if m.keyField != "" {
		return fmt.Errorf("key_field is used, cannot be used with set operator. key_field is only supported for get")
	}
	return index(ctx, m, value, key, "create")
}

func (m *opensearchCache) Delete(ctx context.Context, key string) error {
	if m.keyField != "" {
		return fmt.Errorf("key_field is used, cannot be used with set operator. key_field is only supported for get")
	}
	_, err := m.client.Document.Delete(ctx, opensearchapi.DocumentDeleteReq{
		Index:      m.indexName,
		DocumentID: key,
	})
	return err
}

func (m *opensearchCache) Close(ctx context.Context) error {
	return nil
}

func index(ctx context.Context, m *opensearchCache, value []byte, key string, optype string) error {
	if m.keyField != "" {
		return fmt.Errorf("key_field is used, cannot be used with set operator. key_field is only supported for get")
	}
	value_field := m.valueField
	if m.valueField == "" {
		value_field = "value"
	}
	document := map[string]interface{}{
		value_field: string(value),
	}

	data, err := json.Marshal(document)
	if err != nil {
		return err
	}

	req := opensearchapi.IndexReq{
		Index:      m.indexName,
		DocumentID: key,
		Body:       bytes.NewReader(data),
		Params: opensearchapi.IndexParams{
			OpType: optype,
		},
	}
	_, err = m.client.Index(ctx, req)
	return err
}
