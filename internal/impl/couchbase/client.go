package couchbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocb/v2"

	"github.com/benthosdev/benthos/v4/internal/impl/couchbase/client"
	"github.com/benthosdev/benthos/v4/public/service"
)

// ErrInvalidTranscoder specified transcoder is not supported.
var ErrInvalidTranscoder = errors.New("invalid transcoder")

type couchbaseClient struct {
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

func getClient(conf *service.ParsedConfig, mgr *service.Resources) (*couchbaseClient, error) {

	// retrieve params
	url, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}
	bucket, err := conf.FieldString("bucket")
	if err != nil {
		return nil, err
	}
	timeout, err := conf.FieldDuration("timeout")
	if err != nil {
		return nil, err
	}

	// setup couchbase
	opts := gocb.ClusterOptions{
		// TODO add opentracing Tracer:
		// TODO add metrics Meter:
	}

	opts.TimeoutsConfig = gocb.TimeoutsConfig{
		ConnectTimeout:    timeout,
		KVTimeout:         timeout,
		KVDurableTimeout:  timeout,
		ViewTimeout:       timeout,
		QueryTimeout:      timeout,
		AnalyticsTimeout:  timeout,
		SearchTimeout:     timeout,
		ManagementTimeout: timeout,
	}

	if conf.Contains("username") {
		username, err := conf.FieldString("username")
		if err != nil {
			return nil, err
		}
		password, err := conf.FieldString("password")
		if err != nil {
			return nil, err
		}
		opts.Authenticator = gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	tr, err := conf.FieldString("transcoder")
	if err != nil {
		return nil, err
	}
	switch client.Transcoder(tr) {
	case client.TranscoderJSON:
		opts.Transcoder = gocb.NewJSONTranscoder()
	case client.TranscoderRaw:
		opts.Transcoder = gocb.NewRawBinaryTranscoder()
	case client.TranscoderRawJSON:
		opts.Transcoder = gocb.NewRawJSONTranscoder()
	case client.TranscoderRawString:
		opts.Transcoder = gocb.NewRawStringTranscoder()
	case client.TranscoderLegacy:
		opts.Transcoder = gocb.NewLegacyTranscoder()
	default:
		return nil, fmt.Errorf("%w: %s", ErrInvalidTranscoder, tr)
	}

	cluster, err := gocb.Connect(url, opts)
	if err != nil {
		return nil, err
	}

	// check that we can do query
	err = cluster.Bucket(bucket).WaitUntilReady(timeout, nil)
	if err != nil {
		return nil, err
	}

	proc := &couchbaseClient{
		cluster: cluster,
	}

	// retrieve collection
	if conf.Contains("collection") {
		collectionStr, err := conf.FieldString("collection")
		if err != nil {
			return nil, err
		}
		proc.collection = cluster.Bucket(bucket).Collection(collectionStr)
	} else {
		proc.collection = cluster.Bucket(bucket).DefaultCollection()
	}

	return proc, nil
}

func (p *couchbaseClient) Close(ctx context.Context) error {
	return p.cluster.Close(&gocb.ClusterCloseOptions{})
}
