package cache

import (
	"regexp"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateCollectionName(testID string) string {
	reg, _ := regexp.Compile("[^a-zA-Z]+")
	return reg.ReplaceAllString(testID, "")
}

func createCollection(resource *dockertest.Resource, collectionName string, username string, password string) error {
	time.Sleep(time.Second * 1)

	_, err := resource.Exec([]string{
		"mongo",
		"-u",
		username,
		"-p",
		password,
		"--authenticationDatabase",
		"admin",
		"--eval",
		"db.createCollection(\"" + collectionName + "\")",
		"TestDB",
	}, dockertest.ExecOptions{})
	if err != nil {
		return err
	}

	time.Sleep(time.Second * 1)

	_, err = resource.Exec([]string{
		"mongo",
		"-u",
		username,
		"-p",
		password,
		"--authenticationDatabase",
		"admin",
		"--eval",
		"db." + collectionName + ".createIndex({ \"key\": 1 }, { unique: true })",
		"TestDB",
	}, dockertest.ExecOptions{})
	if err != nil {
		return err
	}

	time.Sleep(time.Second * 1)
	return nil
}

var _ = registerIntegrationTest("mongo", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	portBindings := map[docker.Port][]docker.PortBinding{}
	portBindings["27017/tcp"] = []docker.PortBinding{{
		HostIP:   "",
		HostPort: "27017/tcp",
	}}

	env := []string{
		"MONGO_INITDB_ROOT_USERNAME=mongoadmin",
		"MONGO_INITDB_ROOT_PASSWORD=secret",
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "mongo",
		Tag:          "latest",
		Env:          env,
		PortBindings: portBindings,
	})
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	_, err = resource.Exec([]string{
		"mongo",
		"-u",
		"mongoadmin",
		"-p",
		"secret",
		"--authenticationDatabase",
		"admin",
		"TestDB",
	}, dockertest.ExecOptions{})
	require.NoError(t, err)

	time.Sleep(time.Second * 1)

	generateCollectionName("TestCollection")

	time.Sleep(time.Second * 1)

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		url := "mongodb://localhost:27017"
		conf := cache.NewConfig()
		conf.MongoDB.URL = url
		conf.MongoDB.Database = "TestDB"
		conf.MongoDB.Collection = "TestCollection"
		conf.MongoDB.KeyField = "key"
		conf.MongoDB.ValueField = "value"
		conf.MongoDB.Username = "mongoadmin"
		conf.MongoDB.Password = "secret"

		m, cErr := cache.NewMongoDB(conf, nil, log.Noop(), metrics.Noop())
		if cErr != nil {
			return cErr
		}
		cErr = m.Add("benthos_test_mongo_connect", []byte("foo bar"))
		return cErr
	}))

	template := `
resources:
  caches:
    testcache:
      mongo:
        url: mongodb://localhost:27017
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
			env.configVars.var1 = generateCollectionName(env.configVars.id)
			require.NoError(t, createCollection(resource, generateCollectionName(env.configVars.id), "mongoadmin", "secret"))
		}),
	)
})
