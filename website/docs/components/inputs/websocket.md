---
title: websocket
type: input
---

```yaml
websocket:
  basic_auth:
    enabled: false
    password: ""
    username: ""
  oauth:
    access_token: ""
    access_token_secret: ""
    consumer_key: ""
    consumer_secret: ""
    enabled: false
    request_url: ""
  open_message: ""
  url: ws://localhost:4195/get/ws
```

Connects to a websocket server and continuously receives messages.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

It is possible to configure an `open_message`, which when set to a
non-empty string will be sent to the websocket server each time a connection is
first established.


