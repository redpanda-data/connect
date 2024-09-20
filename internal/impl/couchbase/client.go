// Copyright 2024 Redpanda Data, Inc.
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

package couchbase

import (
	"context"
	"errors"
	"fmt"

	"github.com/couchbase/gocb/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase/client"
)

// ErrInvalidTranscoder specified transcoder is not supported.
var ErrInvalidTranscoder = errors.New("invalid transcoder")

type couchbaseConfig struct {
	url        string
	opts       gocb.ClusterOptions
	bucket     string
	collection string
}

type couchbaseClient struct {
	collection *gocb.Collection
	cluster    *gocb.Cluster
}

func getClient(conf *service.ParsedConfig) (*couchbaseClient, error) {
	cfg, err := getClientConfig(conf)
	if err != nil {
		return nil, err
	}
	return makeClient(cfg)
}

func getClientConfig(conf *service.ParsedConfig) (*couchbaseConfig, error) {
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
	var collection string
	if conf.Contains("collection") {
		collection, err = conf.FieldString("collection")
		if err != nil {
			return nil, err
		}
	}
	return &couchbaseConfig{url, opts, bucket, collection}, nil
}

func makeClient(cfg *couchbaseConfig) (*couchbaseClient, error) {
	cluster, err := gocb.Connect(cfg.url, cfg.opts)
	if err != nil {
		return nil, err
	}

	// check that we can do query
	err = cluster.Bucket(cfg.bucket).WaitUntilReady(cfg.opts.TimeoutsConfig.ConnectTimeout, nil)
	if err != nil {
		return nil, err
	}

	proc := &couchbaseClient{
		cluster: cluster,
	}

	// retrieve collection
	if cfg.collection != "" {
		proc.collection = cluster.Bucket(cfg.bucket).Collection(cfg.collection)
	} else {
		proc.collection = cluster.Bucket(cfg.bucket).DefaultCollection()
	}

	return proc, nil
}

func (p *couchbaseClient) Close(ctx context.Context) error {
	return p.cluster.Close(&gocb.ClusterCloseOptions{})
}
