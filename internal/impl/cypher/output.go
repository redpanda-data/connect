// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cypher

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jconfig "github.com/neo4j/neo4j-go-driver/v5/neo4j/config"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	coFieldURI               = "uri"
	coFieldBatching          = "batching"
	coFieldCypher            = "cypher"
	coFieldArgsMapping       = "args_mapping"
	coFieldDatabase          = "database_name"
	coFieldTLS               = "tls"
	coFieldBasicAuth         = "basic_auth"
	coFieldBasicAuthEnabled  = "enabled"
	coFieldBasicAuthUsername = "username"
	coFieldBasicAuthPassword = "password"
	coFieldBasicAuthRealm    = "realm"
)

func basicAuthField() *service.ConfigField {
	return service.NewObjectField(coFieldBasicAuth,
		service.NewBoolField(coFieldBasicAuthEnabled).
			Description("Whether to use basic authentication in requests.").
			Default(false),
		service.NewStringField(coFieldBasicAuthUsername).
			Default("").
			Description("A username to authenticate as."),
		service.NewStringField(coFieldBasicAuthPassword).
			Description("A password to authenticate with.").
			Default("").
			Secret(),
		service.NewStringField(coFieldBasicAuthRealm).
			Advanced().
			Default("").
			Description("The realm for authentication challenges."),
	).Description("Allows you to specify basic authentication.").
		Optional()
}

func extractAuth(conf *service.ParsedConfig) (neo4j.AuthToken, error) {
	if !conf.Contains(coFieldBasicAuth) {
		return neo4j.NoAuth(), nil
	}
	conf = conf.Namespace(coFieldBasicAuth)
	enabled, err := conf.FieldBool(coFieldBasicAuthEnabled)
	if !enabled || err != nil {
		return neo4j.NoAuth(), err
	}
	user, err := conf.FieldString(coFieldBasicAuthUsername)
	if err != nil {
		return neo4j.NoAuth(), err
	}
	pass, err := conf.FieldString(coFieldBasicAuthPassword)
	if err != nil {
		return neo4j.NoAuth(), err
	}
	realm, err := conf.FieldString(coFieldBasicAuthRealm)
	if err != nil {
		return neo4j.NoAuth(), err
	}
	return neo4j.BasicAuth(user, pass, realm), nil
}

func outputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Description("The cypher output type writes a batch of messages to any graph database that supports the Neo4j or Bolt protocols.").
		Categories("Services").
		Version("4.37.0").
		Fields(
			service.NewStringField(coFieldURI).
				Description(`The connection URI to connect to.
See https://neo4j.com/docs/go-manual/current/connect-advanced/[Neo4j's documentation^] for more information. `).
				Examples(
					"neo4j://demo.neo4jlabs.com",
					"neo4j+s://aura.databases.neo4j.io",
					"neo4j+ssc://self-signed.demo.neo4jlabs.com",
					"bolt://127.0.0.1:7687",
					"bolt+s://core.db.server:7687",
					"bolt+ssc://10.0.0.43",
				),
			service.NewStringField(coFieldCypher).
				Description("The cypher expression to execute against the graph database.").
				Examples(
					"MERGE (p:Person {name: $name})",
					`MATCH (o:Organization {id: $orgId})
MATCH (p:Person {name: $name})
MERGE (p)-[:WORKS_FOR]->(o)`,
				),
			service.NewStringField(coFieldDatabase).
				Description("Set the target database for which expressions are evaluated against.").
				Default(""),
			service.NewBloblangField(coFieldArgsMapping).
				Description(`The mapping from the message to the data that is passed in as parameters to the cypher expression. Must be an object. By default the entire payload is used.`).
				Examples(
					`root.name = this.displayName`,
					`root = {"orgId": this.org.id, "name": this.user.name}`,
				).
				Optional(),
			basicAuthField(),
			service.NewTLSField(coFieldTLS),
			service.NewBatchPolicyField(coFieldBatching),
			service.NewOutputMaxInFlightField(),
		).Example(
		"Write to Neo4j Aura",
		"This is an example of how to write to Neo4j Aura",
		`
output:
  cypher:
    uri: neo4j+s://example.databases.neo4j.io
    cypher: |
      MERGE (product:Product {id: $id})
        ON CREATE SET product.name = $product,
                       product.title = $title,
                       product.description = $description,
    args_mapping: |
      root = {}
      root.id = this.product.id 
      root.product = this.product.summary.name
      root.title = this.product.summary.displayName
      root.description = this.product.fullDescription
    basic_auth:
      enabled: true
      username: "${NEO4J_USER}"
      password: "${NEO4J_PASSWORD}"
`,
	)
}

func init() {
	service.MustRegisterBatchOutput(
		"cypher", outputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy(coFieldBatching); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newCypherOutput(conf, mgr)
			return
		})

}

func newCypherOutput(conf *service.ParsedConfig, mgr *service.Resources) (*output, error) {
	var err error
	output := &output{}
	output.logger = mgr.Logger()
	if output.target, err = conf.FieldString(coFieldURI); err != nil {
		return nil, err
	}
	if output.cypher, err = conf.FieldString(coFieldCypher); err != nil {
		return nil, err
	}
	if output.db, err = conf.FieldString(coFieldDatabase); err != nil {
		return nil, err
	}
	if conf.Contains(coFieldArgsMapping) {
		if output.argsMapping, err = conf.FieldBloblang(coFieldArgsMapping); err != nil {
			return nil, err
		}
	}
	if output.auth, err = extractAuth(conf); err != nil {
		return nil, err
	}
	if conf.Contains(coFieldTLS) {
		if output.tlsConfig, err = conf.FieldTLS(coFieldTLS); err != nil {
			return nil, err
		}
	}
	if output.maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
		return nil, err
	}
	return output, nil
}

type output struct {
	driver neo4j.DriverWithContext

	logger      *service.Logger
	target      string
	auth        neo4j.AuthToken
	db          string
	cypher      string
	argsMapping *bloblang.Executor

	maxInFlight int
	tlsConfig   *tls.Config
}

func (o *output) Connect(ctx context.Context) error {
	driver, err := neo4j.NewDriverWithContext(o.target, o.auth, func(config *neo4jconfig.Config) {
		config.MaxConnectionPoolSize = o.maxInFlight
		config.TlsConfig = o.tlsConfig
		config.Log = &loggerAdapter{o.logger}
	})
	if err != nil {
		return err
	}
	if err := driver.VerifyConnectivity(ctx); err != nil {
		return fmt.Errorf("unable to verify connectivity: %w", err)
	}
	if err := driver.VerifyAuthentication(ctx, nil); err != nil {
		return fmt.Errorf("unable to verify correct authentication: %w", err)
	}
	o.driver = driver
	return nil
}

func (o *output) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	session := o.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: o.db,
	})
	// This returns the physical connection to the pool
	defer session.Close(ctx)
	var argsMapper *service.MessageBatchBloblangExecutor
	if o.argsMapping != nil {
		argsMapper = batch.BloblangExecutor(o.argsMapping)
	}
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		for i, msg := range batch {
			mapped := msg
			if argsMapper != nil {
				var err error
				mapped, err = argsMapper.Query(i)
				if err != nil {
					return nil, fmt.Errorf("unable to execute %s: %w", coFieldArgsMapping, err)
				}
			}
			data, err := mapped.AsStructured()
			if err != nil {
				return nil, fmt.Errorf("unable to extract %s output: %w", coFieldArgsMapping, err)
			}
			params, ok := data.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("unable to convert output to object, instead got: %T", data)
			}
			res, err := tx.Run(ctx, o.cypher, params)
			if err != nil {
				return nil, err
			}
			if _, err = res.Consume(ctx); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	return err
}

func (o *output) Close(ctx context.Context) error {
	if o.driver == nil {
		return nil
	}
	return o.driver.Close(ctx)
}
