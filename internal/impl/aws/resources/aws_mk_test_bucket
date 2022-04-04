#!/bin/bash

aws s3 mb --endpoint http://localhost:4566 s3://benthos-test

sqs_queue_url=$(aws sqs create-queue \
  --endpoint http://localhost:4566 \
  --queue-name benthos-test \
  --region eu-west-1 \
  --attributes 'ReceiveMessageWaitTimeSeconds=20,VisibilityTimeout=300'  \
  --output text \
  --query 'QueueUrl')

echo sqs_queue_url=$sqs_queue_url

sqs_queue_arn=$(aws sqs get-queue-attributes \
  --endpoint http://localhost:4566 \
  --queue-url "$sqs_queue_url" \
  --region eu-west-1 \
  --attribute-names QueueArn \
  --output text \
  --query 'Attributes.QueueArn')

echo sqs_queue_arn=$sqs_queue_arn

sqs_policy='{
    "Version":"2012-10-17",
    "Statement":[
      {
        "Effect":"Allow",
        "Principal": { "AWS": "*" },
        "Action":"sqs:SendMessage",
        "Resource":"'$sqs_queue_arn'",
        "Condition":{
          "ArnLike": {
            "aws:SourceArn": "arn:aws:s3:*:*:benthos-test"
          }
        }
      }
    ]
  }'

sqs_policy_escaped=$(echo $sqs_policy | perl -pe 's/"/\\"/g')
sqs_attributes='{"Policy":"'$sqs_policy_escaped'"}'
aws sqs set-queue-attributes \
  --endpoint http://localhost:4566 \
  --queue-url "$sqs_queue_url" \
  --region eu-west-1 \
  --attributes "$sqs_attributes"

aws s3api put-bucket-notification-configuration \
  --endpoint http://localhost:4566 \
  --bucket "benthos-test" \
  --region eu-west-1 \
  --notification-configuration '{
    "QueueConfigurations": [{
      "Events": [ "s3:ObjectCreated:*" ],
      "QueueArn": "'$sqs_queue_arn'"
    }]
  }'
