BUFFERS
=======

This document has been generated with `benthos --list-buffers`.

## `memory`

The memory buffer type simply allocates a set amount of RAM for buffering
messages. This protects the pipeline against backpressure until this buffer is
full. The messages are lost if the service is stopped.

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
