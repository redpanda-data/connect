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

package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseObjectPathsSNSEnvelope(t *testing.T) {
	tests := []struct {
		name            string
		envelopePath    string
		body            string
		expectedKey     string
		expectedBucket  string
		expectTestEvent bool
	}{
		{
			name:            "s3 test event",
			body:            `{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2025-08-19T17:34:58.550Z","Bucket":"bucket-test","RequestId":"N99ABJ6Q","HostId":"+3DhJHKGDGBwqSTufMSS1UgAMIoRovmGa9vkZwWIb1="}`,
			expectTestEvent: true,
		},
		{
			name:           "regular object created notification",
			body:           `{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"bucket-test"},"object":{"key":"foo.txt"}}}]}`,
			expectedKey:    "foo.txt",
			expectedBucket: "bucket-test",
		},
		{
			name: "Event field is not a string",
			body: `{"Event":123}`,
		},
		{
			name: "Event field missing entirely",
			body: `{"Service":"Amazon S3"}`,
		},
		{
			name:            "SNS-enveloped test event",
			envelopePath:    "Message",
			body:            `{"Type":"Notification","MessageId":"abc","TopicArn":"arn:aws:sns:eu-west-1:000000000000:topic","Message":"{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2025-08-19T17:34:58.550Z\",\"Bucket\":\"bucket-test\",\"RequestId\":\"N99ABJ6Q\",\"HostId\":\"+3DhJHKGDGBwqSTufMSS1UgAMIoRovmGa9vkZwWIb1=\"}"}`,
			expectTestEvent: true,
		},
		{
			name:           "SNS-enveloped object created notification",
			envelopePath:   "Message",
			body:           `{"Type":"Notification","MessageId":"abc","TopicArn":"arn:aws:sns:eu-west-1:000000000000:topic","Message":"{\"Records\":[{\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"bucket\":{\"name\":\"bucket-test\"},\"object\":{\"key\":\"foo.txt\"}}}]}"}`,
			expectedKey:    "foo.txt",
			expectedBucket: "bucket-test",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &sqsTargetReader{
				conf: s3iConfig{
					SQS: s3iSQSConfig{
						EnvelopePath: test.envelopePath,
						KeyPath:      "Records.*.s3.object.key",
						BucketPath:   "Records.*.s3.bucket.name",
					},
				},
			}

			gObj, objects, err := reader.parseObjectPaths(&test.body)
			require.NoError(t, err)

			if test.expectedKey == "" {
				assert.Empty(t, objects)
			} else {
				require.Len(t, objects, 1)
				assert.Equal(t, test.expectedKey, objects[0].key)
				assert.Equal(t, test.expectedBucket, objects[0].bucket)
			}

			assert.Equal(t, test.expectTestEvent, isS3TestEvent(gObj))
		})
	}
}
