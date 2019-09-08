package cache

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ory/dockertest"
)

func TestS3Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4572/tcp"},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	endpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("4572/tcp"))
	bucket := "mybucket"

	client := s3.New(session.Must(session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("eu-west-1"),
	})))
	if err = pool.Retry(func() error {
		_, berr := client.CreateBucket(&s3.CreateBucketInput{
			Bucket: &bucket,
		})
		if berr != nil {
			return berr
		}
		return client.WaitUntilBucketExists(&s3.HeadBucketInput{
			Bucket: &bucket,
		})
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	conf := NewConfig()
	conf.S3.Endpoint = endpoint
	conf.S3.Credentials.ID = "xxxxx"
	conf.S3.Credentials.Secret = "xxxxx"
	conf.S3.Credentials.Token = "xxxxx"
	conf.S3.Region = "eu-west-1"
	conf.S3.Bucket = bucket
	conf.S3.ForcePathStyleURLs = true

	t.Run("testS3GetAndSet", func(t *testing.T) {
		testS3GetAndSet(t, conf)
	})

	t.Run("testS3GetAndSetMulti", func(t *testing.T) {
		testS3GetAndSetMulti(t, conf)
	})

	t.Run("testS3AddAndDelete", func(t *testing.T) {
		testS3AddAndDelete(t, conf)
	})
}

func testS3GetAndSet(t *testing.T, conf Config) {
	c, err := NewS3(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := []byte(`{"foo":"bar"}`)
	if err := c.Set("foo", exp); err != nil {
		t.Error(err)
	}

	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != string(exp) {
		t.Errorf("Expected key 'foo' to have value %s, got %s", string(exp), string(act))
	}

	if err := c.Delete("foo"); err != nil {
		t.Error(err)
	}

	if _, err := c.Get("foo"); err != types.ErrKeyNotFound {
		t.Errorf("Expected '%v', received: '%v'", types.ErrKeyNotFound, err)
	}
}

func testS3GetAndSetMulti(t *testing.T, conf Config) {
	c, err := NewS3(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := map[string][]byte{
		"foo": []byte(`{"test":"foo"}`),
		"bar": []byte(`{"test":"bar"}`),
		"baz": []byte(`{"test":"baz"}`),
	}

	if err := c.SetMulti(exp); err != nil {
		t.Fatal(err)
	}

	for k, v := range exp {
		if act, err := c.Get(k); err != nil {
			t.Error(err)
		} else if string(act) != string(v) {
			t.Errorf("Expected key '%v' to have value %s, got %s", k, string(v), string(act))
		}
	}

	for k := range exp {
		if err := c.Delete(k); err != nil {
			t.Error(err)
		}
	}
}

func testS3AddAndDelete(t *testing.T, conf Config) {
	c, err := NewS3(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Add("foo", []byte(`{"foo":"bar"}`)); err != nil {
		t.Error(err)
	}
	defer func() {
		if err := c.Delete("foo"); err != nil {
			t.Error(err)
		}
	}()

	exp := []byte(`{"foo":"baz"}`)
	if err := c.Add("foo", exp); err != types.ErrKeyAlreadyExists {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrKeyAlreadyExists)
	}

	if err := c.Delete("foo"); err != nil {
		t.Error(err)
	}

	if err := c.Add("foo", exp); err != nil {
		t.Error(err)
	}

	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != string(exp) {
		t.Errorf("Expected key 'foo' to have value %s, got %s", string(exp), string(act))
	}
}
