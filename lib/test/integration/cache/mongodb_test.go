package cache

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/service/mongodb/client"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	if err != nil {
		return err
	}

	time.Sleep(time.Second * 1)

	_, err = resource.Exec([]string{
		"mongo",
		"-u", username,
		"-p", password,
		"--authenticationDatabase", "admin",
		"--eval", "db." + collectionName + ".createIndex({ \"key\": 1 }, { unique: true })",
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

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := "mongodb://localhost:" + resource.GetPort("27017/tcp")
		conf := client.NewConfig()
		conf.URL = url
		conf.Database = "TestDB"
		conf.Collection = "TestCollection"
		conf.Username = "mongoadmin"
		conf.Password = "secret"

		mongoClient, err := conf.Client()
		if err != nil {
			return err
		}
		return mongoClient.Connect(context.Background())
	}))

	template := `
resources:
  caches:
    testcache:
      mongodb:
        url: mongodb://localhost:$PORT
        database: TestDB
        collection: $VAR1
        key_field: key
        value_field: value
        username: mongoadmin
        password: secret
`
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMissingKey(),
		integrationTestDoubleAdd(),
		integrationTestDelete(),
		integrationTestGetAndSet(50),
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
