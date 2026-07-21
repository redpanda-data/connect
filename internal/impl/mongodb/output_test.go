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

package mongodb

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestExtJSONFromMapDontPreserveBinary(t *testing.T) {
	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	base64String := base64.StdEncoding.EncodeToString(rawBytes)

	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"binary_field": rawBytes,
	})

	executor, err := bloblang.Parse("root = this")
	require.NoError(t, err)

	batch := service.MessageBatch{msg}
	mappingExec := batch.BloblangExecutor(executor)

	result, err := extJSONFromMap(0, mappingExec)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultD, ok := result.(bson.D)
	require.True(t, ok)

	var fieldValue any
	found := false
	for _, elem := range resultD {
		if elem.Key == "binary_field" {
			fieldValue = elem.Value
			found = true
			break
		}
	}
	require.True(t, found)

	_, isBytes := fieldValue.([]byte)
	assert.False(t, isBytes)

	extractedString, isString := fieldValue.(string)
	assert.True(t, isString)
	assert.Equal(t, base64String, extractedString)
}

func TestStructuredFromMapPreservesBinary(t *testing.T) {
	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"binary_field": rawBytes,
	})

	executor, err := bloblang.Parse("root = this")
	require.NoError(t, err)

	batch := service.MessageBatch{msg}
	mappingExec := batch.BloblangExecutor(executor)

	result, err := structuredFromMap(0, mappingExec)
	require.NoError(t, err)
	require.NotNil(t, result)

	resultMap, ok := result.(map[string]any)
	require.True(t, ok)

	extractedBytes, isBytes := resultMap["binary_field"].([]byte)
	assert.True(t, isBytes)
	assert.Equal(t, rawBytes, extractedBytes)
}

func generateCollectionName(testID string) string {
	return regexp.MustCompile("[^a-zA-Z]+").ReplaceAllString(testID, "")
}

func TestIntegrationMongoDBDoesNotPreserveBinaryOutputFieldByDefault(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)

	var mongoClient *mongo.Client
	require.Eventually(t, func() bool {
		mongoClient, err = mongo.Connect(options.Client().
			SetConnectTimeout(10 * time.Second).
			SetTimeout(30 * time.Second).
			SetServerSelectionTimeout(30 * time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:" + mp.Port()))
		return err == nil
	}, time.Minute, time.Second)

	db := mongoClient.Database("TestDB")
	collName := generateCollectionName(t.Name())

	require.NoError(t, db.CreateCollection(t.Context(), collName))
	collection := db.Collection(collName)

	config := fmt.Sprintf(`
url: mongodb://localhost:%v
database: TestDB
collection: %v
username: mongoadmin
password: secret
operation: insert-one
document_map: "root = this"
`, mp.Port(), collName)

	parsedConf, err := outputSpec().ParseYAML(config, nil)
	require.NoError(t, err)

	outputWriter, err := newOutputWriter(parsedConf, service.MockResources())
	require.NoError(t, err)

	err = outputWriter.Connect(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = outputWriter.Close(t.Context()) })

	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"id":             "bin_test",
		"binary_payload": rawBytes,
	})
	batch := service.MessageBatch{msg}

	err = outputWriter.WriteBatch(t.Context(), batch)
	require.NoError(t, err)

	filter := bson.M{"id": "bin_test"}
	var doc bson.M
	err = collection.FindOne(t.Context(), filter).Decode(&doc)
	require.NoError(t, err)

	payload, exists := doc["binary_payload"]
	require.True(t, exists)

	payloadString, isString := payload.(string)
	require.True(t, isString)

	expectedPayloadString := base64.StdEncoding.EncodeToString(rawBytes)
	require.Equal(t, expectedPayloadString, payloadString)
}

func TestIntegrationMongoDBPreserveBinaryOutputField(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "mongo:latest",
		testcontainers.WithExposedPorts("27017/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD": "secret",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("27017/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "27017/tcp")
	require.NoError(t, err)

	var mongoClient *mongo.Client
	require.Eventually(t, func() bool {
		mongoClient, err = mongo.Connect(options.Client().
			SetConnectTimeout(10 * time.Second).
			SetTimeout(30 * time.Second).
			SetServerSelectionTimeout(30 * time.Second).
			SetAuth(options.Credential{
				Username: "mongoadmin",
				Password: "secret",
			}).
			ApplyURI("mongodb://localhost:" + mp.Port()))
		return err == nil
	}, time.Minute, time.Second)

	db := mongoClient.Database("TestDB")
	collName := generateCollectionName(t.Name())

	require.NoError(t, db.CreateCollection(t.Context(), collName))
	collection := db.Collection(collName)

	config := fmt.Sprintf(`
url: mongodb://localhost:%v
database: TestDB
collection: %v
username: mongoadmin
password: secret
operation: insert-one
document_map: "root = this"
preserve_binary: true
`, mp.Port(), collName)

	parsedConf, err := outputSpec().ParseYAML(config, nil)
	require.NoError(t, err)

	outputWriter, err := newOutputWriter(parsedConf, service.MockResources())
	require.NoError(t, err)

	err = outputWriter.Connect(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = outputWriter.Close(t.Context()) })

	rawBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{
		"id":             "bin_test",
		"binary_payload": rawBytes,
	})
	batch := service.MessageBatch{msg}

	err = outputWriter.WriteBatch(t.Context(), batch)
	require.NoError(t, err)

	filter := bson.M{"id": "bin_test"}
	var doc bson.M
	err = collection.FindOne(t.Context(), filter).Decode(&doc)
	require.NoError(t, err)

	payload, exists := doc["binary_payload"]
	require.True(t, exists)

	payloadBinary, isBinary := payload.(bson.Binary)
	require.True(t, isBinary)

	require.Equal(t, rawBytes, payloadBinary.Data)
}
