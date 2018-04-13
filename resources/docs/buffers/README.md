BUFFERS
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
| Badger    | Medium     | Parallel  | Disk     |

#### Delivery Guarantees

| Type      | On Restart | On Crash  | On Disk Corruption |
| --------- | ---------- | --------- | ------------------ |
| Memory    | Lost       | Lost      | Lost               |
| Mmap File | Persisted  | Lost      | Lost               |
| Badger    | Persisted  | Persisted | Lost               |

Please note that the badger buffer can be set to disable synchronous writes.
This removes the guarantee of message persistence after a crash, but brings
performance on par with the mmap file buffer. This can make it the faster
overall disk persisted buffer when writing to multiple outputs.

## `badger`

The badger buffer type uses a [badger](https://github.com/dgraph-io/badger) db
in order to persist messages to disk as a key/value store. The benefit of this
method is that unlike the mmap_file approach this buffer can be emptied by
parallel consumers.

Note that throughput can be significantly improved by disabling 'sync_writes',
but this comes at the cost of delivery guarantees under crashes.

This buffer has stronger delivery guarantees and higher throughput across
brokered outputs (except for the fan_out pattern) at the cost of lower single
output throughput.

## `memory`

The memory buffer type simply allocates a set amount of RAM for buffering
messages. This can be useful when reading from sources that produce large bursts
of data. Messages inside the buffer are lost if the service is stopped.

## `mmap_file`

The mmap file buffer type uses memory mapped files to perform low-latency,
file-persisted buffering of messages.

To configure the mmap file buffer you need to designate a writeable directory
for storing the mapped files. Benthos will create multiple files in this
directory as it fills them.

When files are fully read from they will be deleted. You can disable this
feature if you wish to preserve the data indefinitely, but the directory will
fill up as fast as data passes through.

## `none`

Selecting no buffer (default) is the lowest latency option since no extra work
is done to messages that pass through. With this option back pressure from the
output will be directly applied down the pipeline.
