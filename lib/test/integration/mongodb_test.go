package integration

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func generateCollectionName(testID string) string {
	reg, _ := regexp.Compile("[^a-zA-Z]+")
	return reg.ReplaceAllString(testID, "")
}

func createCollection(resource *dockertest.Resource, collectionName string, username string, password string) error {
	_, err := resource.Exec([]string{
		"mongo",
		"-u", username,
		"-p", password,
		"--authenticationDatabase", "admin",
		"--eval", "db.createCollection(\"" + collectionName + "\")",
		"TestDB",
	}, dockertest.ExecOptions{})
	return err
}

var _ = registerIntegrationTest("mongodb", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mongo",
		Tag:        "latest",
		Env: []string{
			"MONGO_INITDB_ROOT_USERNAME=mongoadmin",
			"MONGO_INITDB_ROOT_PASSWORD=secret",
		},
		ExposedPorts: []string{"27017"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var mongoClient *mongo.Client

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := "mongodb://localhost:" + resource.GetPort("27017/tcp")
		conf := client.NewConfig()
		conf.URL = url
		conf.Database = "TestDB"
		conf.Collection = "TestCollection"
		conf.Username = "mongoadmin"
		conf.Password = "secret"

		mongoClient, err = conf.Client()
		if err != nil {
			return err
		}
		return mongoClient.Connect(context.Background())
	}))

	t.Run("with JSON", func(t *testing.T) {
		template := `
output:
  mongodb:
    url: mongodb://localhost:$PORT
    database: TestDB
    collection: $VAR1
    username: mongoadmin
    password: secret
    operation: insert-one
    document_map: |
      root.id = this.id
      root.content = this.content
    write_concern:
      w: 1
      w_timeout: 1s
`
		queryGetFn := func(en *testEnvironment, id string) (string, []string, error) {
			db := mongoClient.Database("TestDB")
			collection := db.Collection(generateCollectionName(en.configVars.id))
			idInt, err := strconv.Atoi(id)
			if err != nil {
				return "", nil, err
			}

			filter := bson.M{"id": idInt}
			document, err := collection.FindOne(context.Background(), filter).DecodeBytes()
			if err != nil {
				return "", nil, err
			}

			value, err := document.LookupErr("content")
			if err != nil {
				return "", nil, err
			}

			return fmt.Sprintf(`{"id":%v,"content":%v}`, id, value.String()), nil, err
		}

		suite := integrationTests(
			integrationTestOutputOnlySendSequential(10, queryGetFn),
			integrationTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
			testOptPort(resource.GetPort("27017/tcp")),
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				cName := generateCollectionName(env.configVars.id)
				env.configVars.var1 = cName
				require.NoError(t, createCollection(resource, cName, "mongoadmin", "secret"))
			}),
		)
	})
})
