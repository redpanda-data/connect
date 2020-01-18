---
title: socket
type: output
---

```yaml
socket:
  address: /tmp/benthos.sock
  network: unix
```

Sends messages as a continuous stream of line delimited data over a
(tcp/udp/unix) socket by connecting to a server.

If batched messages are sent the final message of the batch will be followed by
two line breaks in order to indicate the end of the batch.


