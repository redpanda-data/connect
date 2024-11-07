Redpanda Benchmarking
=====================

I've created this directory as a convenient way to create Redpanda topics and benchmark Redpanda Connect instances against them with various configs.

## Getting Started

```sh
# Start redpanda, grafana, etc
docker-compose up -d

# Create some test topics
rpk topic create testing_a -p 10
rpk topic create testing_b -p 10
rpk topic create testing_c -p 10
rpk topic create testing_d -p 10
```

## Generate Data

```sh
# Inserts 100,000,000 records into topic testing_a
redpanda-connect run ./generate.yaml
```

