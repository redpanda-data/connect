# Snowflake Integration SDK for Redpanda Connect


### Testing

To enable integration tests, you need to follow the instructions here to generate a public/private key for snowflake: https://docs.snowflake.com/en/user-guide/key-pair-auth

Run the `openssl` commands from that guide in the `resources` directory to generate the correct keys for the integration test (the test requires the private key is unencrypted), then run the following:

```
SNOWFLAKE_USER=XXX \
  SNOWFLAKE_ACCOUNT=alskjd-asdaks \
  SNOWFLAKE_DB=xxx \
  go test -v .
```
