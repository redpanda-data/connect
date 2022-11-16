---
title: Secrets
---

I sometimes like to fill my mouth with acorns and pretend I am a rodent, free of the burdens of humanity. That was a secret of mine and, similarly to secrets within your software, it's best not to share them publicly lest they become disturbing publications instead. This document outlines how to add secrets to a Benthos config without persisting them, and we won't mention acorns again.

## Using Environment Variables

One of the most prolific approaches to providing secrets to a service is via environment variables. Benthos allows you to inject the values of environment variables into a configuration with the interpolation syntax `${FOO}`, within a config it looks like this:

```yml
thing:
  super_secret: "${SECRET}"
```

:::info Use quotes
Note that it would be valid to have `super_secret: ${SECRET}` above (without the quotes), but if `SECRET` is unset then the config becomes structurally different. Therefore, it's always best to wrap environment variable interpolations with quotes so that when the variable is unset you still have a valid config (with an empty string).
:::

More information about this syntax can be found on the [interpolation field page][interpolation].

## Using CLI Flags

As an alternative to environment variables it's possible to set specific fields within a config using the CLI flag `--set` where the syntax is a `<path>=<value>` pair, the path being a [dot-separated path to the field being set][field_paths] and the value being the thing to set it to. If, for example, we had the config:

```yml
thing:
  super_secret: ""
```

And we wanted to set the value of `super_secret` to a value stored within something like Hashicorp Vault we could run the config using the `--set` flag with backticks to execute a shell command for the value:

```sh
benthos -c ./config.yaml \
  --set "thing.super_secret=`vault kv get -mount=secret thing_secret`"
```

Using this method we can inject the secret into the config without "leaking" it into an environment variable.

## Avoiding Leaked Secrets

There are a few ways in which configs parsed by Benthos can be exported back out of the service. In all of these cases Benthos will attempt to scrub any field values within the config that are known secrets (any field marked as a secret in the docs).

However, if you're embedding secrets within a config outside of the value of secret fields, maybe as part of a Bloblang mapping, then care should be made to avoid exposing the resulting config. This specifically means you should not enable [debug HTTP endpoints][http.debug] when the port is exposed, and don't use the `benthos echo` subcommand on configs containing secrets unless you're printing to a secure pipe.

[interpolation]: /docs/configuration/interpolation
[field_paths]: /docs/configuration/field_paths
[http.debug]: /docs/components/http/about#debug-endpoints

