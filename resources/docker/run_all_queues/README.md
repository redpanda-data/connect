Run All Queues
==============

Use this docker compose config to set up each supported queue system and create
a Benthos instance to route messages to all of them through round-robin. In
order to send test messages use curl:

``` sh
curl http://localhost:4195/post -d "hello world"
```

And you can also stream the output messages with curl:

``` sh
curl http://localhost:4196/get/stream
```
