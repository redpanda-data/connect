---
title: subprocess
type: processor
---

```yaml
subprocess:
  args: []
  max_buffer: 65536
  name: cat
  parts: []
```

Subprocess is a processor that runs a process in the background and, for each
message, will pipe its contents to the stdin stream of the process followed by a
newline.

The subprocess must then either return a line over stdout or stderr. If a
response is returned over stdout then its contents will replace the message. If
a response is instead returned from stderr it will be logged and the message
will continue unchanged and will be [marked as failed](/docs/configuration/error_handling).

The field `max_buffer` defines the maximum response size able to be
read from the subprocess. This value should be set significantly above the real
expected maximum response size.

#### Subprocess requirements

It is required that subprocesses flush their stdout and stderr pipes for each
line.

Benthos will attempt to keep the process alive for as long as the pipeline is
running. If the process exits early it will be restarted.

#### Messages containing line breaks

If a message contains line breaks each line of the message is piped to the
subprocess and flushed, and a response is expected from the subprocess before
another line is fed in.


