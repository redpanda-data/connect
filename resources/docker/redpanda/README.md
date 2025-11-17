# Redpanda Test Cluster

Three-broker Redpanda cluster with Redpanda Console for local testing.

Based on: https://docs.redpanda.com/redpanda-labs/docker-compose/three-brokers/

## Prerequisites

- Docker and Docker Compose installed
- Task (taskfile) installed

## Quick Start

```bash
task setup    # Download docker-compose.yml
task up       # Start the cluster
task console  # Open Redpanda Console in browser
```

## Available Tasks

- `task setup` - Download docker-compose.yml from Redpanda Labs
- `task up` - Start the Redpanda cluster
- `task down` - Stop and remove the cluster
- `task restart` - Restart the cluster
- `task logs` - View cluster logs
- `task console` - Open Redpanda Console (http://localhost:8080)
- `task status` - Check cluster status
- `task clean` - Stop cluster and remove volumes

## Cluster Configuration

### Brokers

Three Redpanda brokers with the following external ports:

| Broker | Kafka | Schema Registry | HTTP Proxy | Admin API |
|--------|-------|-----------------|------------|-----------|
| redpanda-0 | 19092 | 18081 | 18082 | 19644 |
| redpanda-1 | 29092 | 28081 | 28082 | 29644 |
| redpanda-2 | 39092 | 38081 | 38082 | 39644 |

### Console

- **URL**: http://localhost:8080
- **Kafka Broker**: redpanda-0:9092 (internal)
- **Schema Registry**: http://redpanda-0:8081
- **Admin API**: http://redpanda-0:9644

## Connection Strings

### From Host Machine

```bash
# Kafka
localhost:19092,localhost:29092,localhost:39092

# Schema Registry (any broker)
http://localhost:18081
http://localhost:28081
http://localhost:38081
```

### From Docker Network

```bash
# Kafka
redpanda-0:9092,redpanda-1:9092,redpanda-2:9092

# Schema Registry
http://redpanda-0:8081
```

## Notes

- The `docker-compose.yml` file is downloaded from Redpanda Labs and not committed to git
- Run `task setup` to download the latest version
- Cluster runs in `dev-container` mode with 1 CPU core per broker
- Data is persisted in Docker volumes: `redpanda-0`, `redpanda-1`, `redpanda-2`
