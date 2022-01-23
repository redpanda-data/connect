---
title: About
---

:::warning Work in progress
This guide is a work in progress as V4 hasn't actually been released yet. Some of the links on these pages may therefore be incorrect or missing.
:::

Benthos has been at major version 3 [for more than two years][blog.v4roadmap], during which time it has gained a huge amount of functionality without introducing any breaking changes. However, the number of components, APIs and features that have been deprecated in favour of better solutions has grown steadily and the time has finally come to purge them. There are also some areas of functionality that have been improved with breaking changes.

This document outlines the changes made to Benthos since V3 and tips for how to migrate to V4 in places where those changes are significant.

### Pipeline Threads Behaviour Change

https://github.com/Jeffail/benthos/issues/399

In V3 the `pipeline.threads` field defaults to 1. If this field is explicitly set to `0` it will automatically match the number of CPUs on the host machine. In V4 this will change so that the default value of `pipeline.threads` is `-1`, where this value indicates we should match the number of host CPUs. A value of `0` will now throw a configuration error when set explicitly.

### Processor Batch Behaviour Changes

https://github.com/Jeffail/benthos/issues/408

Some processors that once executed only once per batch have been updated to execute upon each message individually by default. This change has been made because it was felt the individual message case was considerably more common (and intuitive) and that it is possible to satisfy the batch-wide behaviour in other ways that are opt-in.

### New Go Module Name

For users of the Go plugin APIs the import path of this module needs to be updated to `github.com/benthosdev/benthos/v4`, like so:

```go
import "github.com/benthosdev/benthos/v4/public/service"
```

It should be pretty quick to update your imports, either using a tool or just running something like:

```sh
grep "Jeffail/benthos/v3" . -Rl | grep -e "\.go$" | xargs -I{} sed -i 's/Jeffail\/benthos\/v3/benthosdev\/benthos\/v4/g' {}
```

### Deprecated Components Removed

All components, features and configuration fields that were marked as deprecated in the latest release of V3 have been removed in V4. In order to detect deprecated components or fields within your V3 configuration files you can run the linter from a V3 Benthos with the `--deprecated` flag:

```sh
benthos lint --deprecated ./configs/*.yaml
```

This should report all remaining deprecated components.

### Broker Ditto Macro Gone

The hidden macro `ditto` for broker configs is removed.

### Old Style Interpolation Functions Removed

The original style of interpolation functions, where you specify a function name followed by a colon and then any arguments (`${!json:foo,1}`) has been deprecated (and undocumented) for a while now. What we've had instead is a subset of Bloblang allowing you to use functions directly (`${! json("foo").from(1) }`), but with the old style still supported for backwards compatibility.

However, supporting the old style means our parsing capabilities are weakened and so it is now removed in order to allow more powerful interpolations in the future.

### Env Var Docker Configuration

Docker builds will no longer come with a default config that contains generated environment variables. This system doesn't scale at all for complex configuration files and was becoming a challenge to maintain (and also huge). Instead, the new `-s` flag has been the preferred way to configure Benthos through arguments and will need to be used exclusively in V4.

It's worth noting that this does not prevent you from defining your own env var based configuration and adding that to your docker image. It's entirely possible to copy the config from V3 and have that work, it just won't be present by default any more.

### Old Plugin APIs Removed

Any packages from within the `lib` directory have been removed. Please use only the APIs within the `public` directory, the API docs count be found on [pkg.go.dev][plugins.docs], and examples can be found in the [`benthos-plugin-example` repository][plugins.repo]. These new APIs can be found in V3 so if you have many components you can migrate them incrementally by sticking with V3 until completion.

Many of the old packages within `lib` can also still be found within `internal`, if you're in a pickle you can find some of those APIs and copy/paste them into your own repository.

### Bloblang Changes

https://github.com/Jeffail/benthos/issues/571

The functions `meta`, `root_meta`, `error` and `env` now return `null` when the target value does not exist. This is in order to improve consistency across different functions and query types. In cases where a default empty string is preferred you can add `.or("")` onto the function. In cases where you want to throw an error when the value does not exist you can add `.not_null()` onto the function.

TODO: NEEDS AN ISSUE

The method `re_replace` has been renamed to `re_replace_all`.

TODO: NEEDS AN ISSUE

It is now possible to reference the `root` of the document being created within a mapping query, i.e. `root.hash = root.string().hash("xxhash64")`.

### Field Default Changes

https://github.com/Jeffail/benthos/issues/392

Lots of fields have had default values removed in cases where they deemed unlikely to be useful and likely to cause frustration. This specifically applies to any `url`, `urls`, `address` or `addresses` fields that may have once had a default value containing a common example for the particular service. In most cases this should cause minimal disruption as the field is non-optional and therefore not specifying it explicitly will result in config errors.

However, there are the following exceptions that are worth noting:

#### AWS `region` fields

https://github.com/Jeffail/benthos/issues/696

Any configuration sections containing AWS fields no longer have a default `region` of `eu-west-1`. Instead, the field will be empty by default, where unless explicitly set the environment variable `AWS_REGION` will be used. This will cause problems for users where they expect the region `eu-west-1` to be targeted when neither the field nor the environment variable `AWS_REGION` are set.

### Serverless Default Output

The default output of the serverless distribution of Benthos is now the following config:

```yaml
output:
  switch:
    cases:
      - check: !errored()
        output:
          sync_response: {}
      - output:
          nack: "processing failed due to: ${! error() }"
```

This change was made in order to return processing errors directly to the invoker by default.

### Metrics Changes

https://github.com/Jeffail/benthos/issues/1066

### Tracing Changes

https://github.com/Jeffail/benthos/issues/872

### Logging Changes

https://github.com/Jeffail/benthos/issues/589

[blog.v4roadmap]: /blog/2021/01/04/v4-roadmap
[v3.docs]: https://v3docs.benthos.dev
[plugins.repo]: https://github.com/benthosdev/benthos-plugin-example
[plugins.docs]: https://pkg.go.dev/github.com/benthosdev/benthos/v4/public
