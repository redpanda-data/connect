# Redpanda Connect Python Plugins

This library allows you to create python plugins for [Redpanda Connect](https://www.redpanda.com/connect).

In order to use create a processor plugin you can follow these steps:

```shell
uv init project

cd project

uv add redpanda_connect

cat <<EOF > main.py
import asyncio
import logging
import redpanda_connect


@redpanda_connect.processor
def yell(msg: redpanda_connect.Message) -> redpanda_connect.Message:
    msg.payload = msg.payload.upper()
    return msg

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(redpanda_connect.processor_main(yell))
EOF

cat <<EOF > plugin.yaml
name: foo
summary: Just the simplest example
command: ["uv", "run", "main.py"]
type: processor
fields: []
EOF

cat <<EOF > connect.yaml
pipeline:
  processors:
    - foo: {}
EOF

rpk connect run --rpc-plugin=plugin.yaml connect.yaml
```
