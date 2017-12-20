Run All Queues
==============

Use this docker compose config to set up each supported queue system and create
a Benthos instance to route messages to all of them through round-robin. In
order to send test messages use curl:

``` sh
curl http://localhost:8080/post -d "hello world"
```
