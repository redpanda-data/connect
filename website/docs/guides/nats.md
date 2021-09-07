---
title: NATS
description: Find out about NATS components in Benthos
---

## Authentication

There are several components within Benthos which utilise NATS services. You will find that each of these components
support optional advanced authentication parameters for [NKeys](https://docs.nats.io/nats-server/configuration/securing_nats/auth_intro/nkey_auth)
and [User Credentials](https://docs.nats.io/developing-with-nats/security/creds).

An in depth tutorial can be found [here](https://docs.nats.io/developing-with-nats/tutorials/jwt).

### NKey file

The NATS server can use these NKeys in several ways for authentication. The simplest is for the server to be configured
with a list of known public keys and for the clients to respond to the challenge by signing it with its private NKey
configured in the `nkey_file` field.

More details [here](https://docs.nats.io/developing-with-nats/security/nkey).

### User Credentials file

NATS server supports decentralized authentication based on JSON Web Tokens (JWT). Clients need an [user JWT](https://docs.nats.io/nats-server/configuration/securing_nats/jwt#json-web-tokens)
and a corresponding [NKey secret](https://docs.nats.io/developing-with-nats/security/nkey) when connecting to a server
which is configured to use this authentication scheme.

The `user_credentials_file` field should point to a file containing both the private key and the JWT and can be
generated with the [nsc tool](https://docs.nats.io/nats-tools/nsc).

More details [here](https://docs.nats.io/developing-with-nats/security/creds).
