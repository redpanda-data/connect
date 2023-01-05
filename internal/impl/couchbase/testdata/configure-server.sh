#!bin/bash

set -m

/entrypoint.sh couchbase-server &

sleep 8

# Setup initial cluster/ Initialize Node
couchbase-cli cluster-init -c 127.0.0.1 --cluster-name $CLUSTER_NAME --cluster-username $COUCHBASE_ADMINISTRATOR_USERNAME \
  --cluster-password $COUCHBASE_ADMINISTRATOR_PASSWORD --services data --cluster-ramsize 1024

sleep 5

# Setup Administrator username and password
curl -s http://127.0.0.1:8091/settings/web -d port=8091 -d username=$COUCHBASE_ADMINISTRATOR_USERNAME -d password=$COUCHBASE_ADMINISTRATOR_PASSWORD

# Setup buckets
for BUCKET_NAME in testing
do
  couchbase-cli bucket-create -c 127.0.0.1:8091 --username $COUCHBASE_ADMINISTRATOR_USERNAME \
    --password $COUCHBASE_ADMINISTRATOR_PASSWORD  --bucket $BUCKET_NAME --bucket-type couchbase \
    --bucket-ramsize 128

  sleep 5
done

touch /is-ready

fg 1
