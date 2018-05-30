Streams Mode
============

A Benthos stream consists of four components; an input, an optional buffer,
processor pipelines and an output. Under normal use a Benthos instance is a
single stream, and the components listed are configured within the service
config file.

Alternatively, Benthos can be run in `--streams` mode, where a single running
Benthos instance is able to run multiple entirely isolated streams. Streams can
be configured by setting the `--streams-dir` flag to a directory containing a
config file for each stream (`/benthos/streams` by default).

During runtime streams can also be added, updated, removed and monitored using
[a REST HTTP interface][http-interface].

[http-interface]: api/streams.md
