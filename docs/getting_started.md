Getting Started
===============

## Install

The easiest way to install Benthos is with this handy script:

```sh
curl -Lsf https://sh.benthos.dev | bash
```

### Download an Archive

Otherwise you can grab an archive containing Benthos from the
[releases page][releases].

### Pull With Docker

If you have docker installed you can pull the latest official Benthos image
with:

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

For information about serverless deployments of Benthos check out the serverless
section [here][serverless].

## Run

A Benthos stream pipeline is configured with a single
[config file][configuration], you can generate a fresh one with:

```sh
benthos --print-yaml > config.yaml
```

The main sections that make up a config are `input`, `pipeline` and `output`.
When you generate a fresh config it'll simply pipe `stdin` to `stdout` like
this:

```yaml
input:
  type: stdin

pipeline:
  processors: []

output:
  type: stdout
```

Eventually we'll want to configure a more useful [input][inputs] and
[output][outputs], but for now this is useful for quickly testing processors.
You can execute this config with:

```sh
benthos -c ./config.yaml
```

Anything you write to stdin will get written unchanged to stdout, cool! Resist
the temptation to play with this for hours, there's more stuff to try out.

Next, let's add some processing steps in order to mutate messages. For example,
we could add a [`text`][proc_text] processor that converts all text into upper
case:

```yaml
input:
  type: stdin

pipeline:
  processors:
    - text:
        operator: to_upper

output:
  type: stdout
```

Now your messages should come out in all caps, how whacky! IT'S LIKE BENTHOS IS
SHOUTING BACK AT YOU!

You can add as many [processing steps][processors] as you like, and since
processors are what make Benthos powerful they are worth experimenting with.
Let's create a more advanced pipeline that works with JSON documents:

```yaml
input:
  type: stdin

pipeline:
  processors:
    - jmespath:
        query: |
          {
            doc: @,
            first_name: names[0],
            last_name: names[-1]
          }

    - process_field:
        path: first_name
        processors:
          - text:
              operator: to_upper

    - process_field:
        path: last_name
        processors:
          - hash:
              algorithm: sha256
          - encode:
              scheme: base64

output:
  type: stdout
```

First, we use a [JMESPath][jmespath] query to restructure our input
JSON document. Next, we use [`process_field`][proc_proc_field] to uppercase the
value of `first_name`. Finally, we hash the value of `last_name` and base64
encode the result.

Try running that config with some sample documents:

```sh
echo '
{"id":"1","names":["celine","dion"]}
{"id":"2","names":["chad", "robert", "kroeger"]}' | benthos -c ./config.yaml
```

You should see (amongst some logs):

```sh
{"doc":{"id":"1","names":["celine","dion"]},"first_name":"CELINE","last_name":"1VvPgCW9sityz5XAMGdI2BTA7/44Wb3cANKxqhiCo50="}
{"doc":{"id":"2","names":["chad","robert","kroeger"]},"first_name":"CHAD","last_name":"uXXg5wCKPjpyj/qbivPbD9H9CZ5DH/F0Q1Twytnt2hQ="}
```

Now that you are a Benthos expert check out all the other
[cool stuff you can do][docs].

[proc_proc_field]: ./processors/README.md#process_field
[proc_text]: ./processors/README.md#text
[processors]: ./processors/README.md
[inputs]: ./inputs/README.md
[outputs]: ./outputs/README.md
[jmespath]: http://jmespath.org/
[docs]: ./README.md
[releases]: https://github.com/Jeffail/benthos/releases
[serverless]: ./serverless/README.md
[configuration]: ./configuration.md