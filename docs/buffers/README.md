Buffers
=======

This document was generated with `benthos --list-buffers`

Buffers can solve a number of typical streaming problems and are worth
considering if you face circumstances similar to the following:

- Input sources can periodically spike beyond the capacity of your output sinks.
- You want to use parallel [processing pipelines](../pipeline.md).
- You have more outputs than inputs and wish to distribute messages across them
  in order to maximize overall throughput.
- Your input source needs occasional protection against back pressure from your
  sink, e.g. during restarts. Please keep in mind that all buffers have an
  eventual limit.

If you believe that a problem you have would be solved by a buffer the next step
is to choose an implementation based on the throughput and delivery guarantees
you need. In order to help here are some simplified tables outlining the
different options and their qualities:

#### Performance

| Type      | Throughput | Consumers | Capacity |
| --------- | ---------- | --------- | -------- |
| Memory    | Highest    | Parallel  | RAM      |
| Mmap File | High       | Single    | Disk     |

#### Delivery Guarantees

| Type      | On Restart | On Crash  | On Disk Corruption |
| --------- | ---------- | --------- | ------------------ |
| Memory    | Lost       | Lost      | Lost               |
| Mmap File | Persisted  | Lost      | Lost               |

### Contents

1. [`memory`](#memory)
2. [`mmap_file`](#mmap_file)
3. [`none`](#none)

## `memory`

``` yaml
type: memory
memory:
  limit: 5.24288e+08
```

The memory buffer type simply allocates a set amount of RAM for buffering
messages. This can be useful when reading from sources that produce large bursts
of data. Messages inside the buffer are lost if the service is stopped.

## `mmap_file`

``` yaml
type: mmap_file
mmap_file:
  clean_up: true
  directory: ""
  file_size: 2.62144e+08
  reserved_disk_space: 1.048576e+08
  retry_period: 1s
```

DEPRECATED: This buffer type is due to be removed in V3.

The mmap file buffer type uses memory mapped files to perform low-latency,
file-persisted buffering of messages.

To configure the mmap file buffer you need to designate a writeable directory
for storing the mapped files. Benthos will create multiple files in this
directory as it fills them.

When files are fully read from they will be deleted. You can disable this
feature if you wish to preserve the data indefinitely, but the directory will
fill up as fast as data passes through.

WARNING: This buffer currently wipes all metadata from message payloads. If you
are using metadata in your pipeline you should avoid using this buffer, or
preferably all buffers altogether.

## `none`

``` yaml
type: none
none: {}
```

Selecting no buffer (default) means the output layer is directly coupled with
the input layer. This is the safest and lowest latency option since
acknowledgements from at-least-once protocols can be propagated all the way from
the output protocol to the input protocol.

If the output layer is hit with back pressure it will propagate all the way to
the input layer, and further up the data stream. If you need to relieve your
pipeline of this back pressure consider using a more robust buffering solution
such as Kafka before resorting to alternatives.
