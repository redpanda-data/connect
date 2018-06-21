Streams Mode
============

A Benthos stream consists of four components; an input, an optional buffer,
processor pipelines and an output. Under normal use a Benthos instance is a
single stream, and these components are configured within the service config
file.

Alternatively, Benthos can be run in `--streams` mode, where a single running
Benthos instance is able to run multiple entirely isolated streams. Adding
streams in this mode can be done in two ways:

1. [Static configuration files][static-files] allows you to maintain a directory
   of static stream configuration files that will be traversed by Benthos.

2. An [HTTP REST API][rest-api] allows you to dynamically create, read the
   status of, update, and delete streams at runtime.

These two methods can be used in combination, i.e. it's possible to update and
delete streams that were created with static files.

[static-files]: using_config_files.md
[rest-api]: using_REST_API.md
