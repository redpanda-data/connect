Buffers
=======

Within Benthos is support for multiple buffering options. This document outlines
the various types of buffer along with potential use cases and configuration
examples.

## None

Benthos works well without a buffer, this means the rate at which messages are
written will limit the rate messages are read, applying back pressure to the
input where applicable. This is perfectly acceptible for cases where Benthos is
required only as a bridge between two other message protocols and benefits such
as surge protection or persistence are not needed.

## Memory

Benthos can use RAM to buffer messages whenever the output applies back
pressure. This helps in situations where the output service is likely to reach
throughput capacity during its lifetime (during surges in data etc), or if the
output service is likely to need restarts and the input service needs to be
flushed.

Using memory is the lowest latency buffer option but will result in data loss if
the service is restarted without being fully flushed.

## Memory-Mapped File

Memory-Mapped files are blocks of data stored in RAM that are flushed to disk by
the host operating system. This means the service is not blocked on file IO
during the write and that messages are ready to dispatch to the output service
much sooner.

For most use cases this is the best option as it provides persistence across
service/operating system restarts and has a low latency overhead.

However, this option does not add protections around service or operating system
crashes, as there is no guarantee during runtime of exactly when the message is
flushed to disk.
