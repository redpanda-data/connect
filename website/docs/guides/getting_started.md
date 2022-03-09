---
title: Getting Started
description: Getting started with Benthos
---

Woops! You fell for the marketing hype. Let's try and get through this together.

<div style={{textAlign: 'center'}}><img style={{maxWidth: '300px'}} src="/img/teacher-blob.svg" /></div>

## Install

The easiest way to install Benthos is with this handy script:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs defaultValue="latest" values={[
  { label: 'Latest stable version', value: 'latest', },
  { label: 'Specific version', value: 'version', },
]}>
<TabItem value="latest">

```sh
curl -Lsf https://sh.benthos.dev | bash
```

</TabItem>
<TabItem value="version">

```sh
curl -Lsf https://sh.benthos.dev | bash -s -- 3.56.0
```

</TabItem>
</Tabs>

Or you can grab an archive containing Benthos from the [releases page][releases].

### Docker

If you have docker installed you can pull the latest official Benthos image with:

```sh
docker pull jeffail/benthos
docker run --rm -v /path/to/your/config.yaml:/benthos.yaml jeffail/benthos
```

### Homebrew

On macOS, Benthos can be installed via Homebrew:

```sh
brew install benthos
```

### Serverless

For information about serverless deployments of Benthos check out the serverless section [here][serverless].

## Run

A Benthos stream pipeline is configured with a single [config file][configuration], you can generate a fresh one with:

```shell
benthos create > config.yaml
```

The main sections that make up a config are `input`, `pipeline` and `output`. When you generate a fresh config it'll simply pipe `stdin` to `stdout` like this:

```yaml
input:
  stdin: {}

pipeline:
  processors: []

output:
  stdout: {}
```

Eventually we'll want to configure a more useful [input][inputs] and [output][outputs], but for now this is useful for quickly testing processors. You can execute this config with:

```sh
benthos -c ./config.yaml
```

Anything you write to stdin will get written unchanged to stdout, cool! Resist the temptation to play with this for hours, there's more stuff to try out.

Next, let's add some processing steps in order to mutate messages. The most powerful one is the [`bloblang` processor][processors.bloblang] which allows us to perform mappings, let's add a mapping to uppercase our messages:

```yaml
input:
  stdin: {}

pipeline:
  processors:
    - bloblang: root = content().uppercase()

output:
  stdout: {}
```

Now your messages should come out in all caps, how whacky! IT'S LIKE BENTHOS IS SHOUTING BACK AT YOU!

You can add as many [processing steps][processors] as you like, and since processors are what make Benthos powerful they are worth experimenting with. Let's create a more advanced pipeline that works with JSON documents:

```yaml
input:
  stdin: {}

pipeline:
  processors:
    - sleep:
        duration: 500ms
    - bloblang: |
        root.doc = this
        root.first_name = this.names.index(0).uppercase()
        root.last_name = this.names.index(-1).hash("sha256").encode("base64")

output:
  stdout: {}
```

First, we sleep for 500 milliseconds just to keep the suspense going. Next, we restructure our input JSON document by nesting it within a field `doc`, we map the upper-cased first element of `names` to a new field `first_name`. Finally, we map the hashed and base64 encoded value of the last element of `names` to a new field `last_name`.

Try running that config with some sample documents:

```sh
echo '
{"id":"1","names":["celine","dion"]}
{"id":"2","names":["chad","robert","kroeger"]}' | benthos -c ./config.yaml
```

You should see (amongst some logs):

```sh
{"doc":{"id":"1","names":["celine","dion"]},"first_name":"CELINE","last_name":"1VvPgCW9sityz5XAMGdI2BTA7/44Wb3cANKxqhiCo50="}
{"doc":{"id":"2","names":["chad","robert","kroeger"]},"first_name":"CHAD","last_name":"uXXg5wCKPjpyj/qbivPbD9H9CZ5DH/F0Q1Twytnt2hQ="}
```

How exciting! I don't know about you but I'm going to need to lie down for a while. Now that you are a Benthos expert might I suggest you peruse these sections to see if anything tickles your fancy?

- [Bloblang Walkthrough][bloblang.walkthrough]
- [Inputs][inputs]
- [Processors][processors]
- [Outputs][outputs]
- [Monitoring][monitoring]
- [Cookbooks][cookbooks]
- [More about configuration][configuration]

[processors.bloblang]: /docs/components/processors/bloblang
[proc_proc_field]: /docs/components/processors/process_field
[proc_text]: /docs/components/processors/text
[processors]: /docs/components/processors/about
[inputs]: /docs/components/inputs/about
[outputs]: /docs/components/outputs/about
[jmespath]: http://jmespath.org/
[releases]: https://github.com/benthosdev/benthos/releases
[serverless]: /docs/guides/serverless/about
[configuration]: /docs/configuration/about
[monitoring]: /docs/guides/monitoring
[cookbooks]: /cookbooks
[bloblang.walkthrough]: /docs/guides/bloblang/walkthrough