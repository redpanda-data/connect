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

package spicedb

import (
	"crypto/tls"

	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type clientConfig struct {
	endpoint                     string
	bearerToken                  string
	tlsConf                      *tls.Config
	maxReceiveMessageSizeInBytes int
}

// load v1 client
func (cc *clientConfig) loadSpiceDBClient() (*authzed.Client, error) {
	creds := insecure.NewCredentials()
	if cc.tlsConf != nil {
		creds = credentials.NewTLS(cc.tlsConf)
	}
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cc.maxReceiveMessageSizeInBytes)),
		grpc.WithTransportCredentials(creds),
	}
	if cc.bearerToken != "" {
		tokenOpt := grpcutil.WithInsecureBearerToken(cc.bearerToken)
		if cc.tlsConf != nil {
			tokenOpt = grpcutil.WithBearerToken(cc.bearerToken)
		}
		opts = append(opts, tokenOpt)
	}
	return authzed.NewClient(
		cc.endpoint,
		opts...,
	)
}
