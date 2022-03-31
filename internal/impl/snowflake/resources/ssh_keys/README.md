# Commands used to generate private SSH keys for Snowpipe tests

```shell
> openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 des3 -inform PEM -passout pass:test123 -out internal/impl/snowflake/resources/ssh_keys/snowflake_rsa_key.p8
> openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -nocrypt -out internal/impl/snowflake/resources/ssh_keys/snowflake_rsa_key.pem
```

Note: For the encrypted key we're using `-v2 des3` because we only support PKCS#5 v2.0: https://linux.die.net/man/1/pkcs8
