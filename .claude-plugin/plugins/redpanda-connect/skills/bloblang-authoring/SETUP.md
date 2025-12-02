# Setup

This skill requires: `rpk`, `rpk connect`, `python3`, `jq`

## macOS

```bash
brew install redpanda-data/tap/redpanda python3 jq
rpk connect install
rpk connect upgrade
```

## Ubuntu (Intel/AMD64)

```bash
apt-get update && apt-get install -y curl unzip python3 jq

curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip && \
  unzip rpk-linux-amd64.zip -d /usr/local/bin/ && \
  rm rpk-linux-amd64.zip

rpk connect install
rpk connect upgrade
```

## Ubuntu (ARM64)

```bash
apt-get update && apt-get install -y curl unzip python3 jq

curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-arm64.zip && \
  unzip rpk-linux-arm64.zip -d /usr/local/bin/ && \
  rm rpk-linux-arm64.zip

rpk connect install
rpk connect upgrade
```
