# Redpanda Agents

This is a project generated from Redpanda Connect's agentic developer framework.

You can define new agents in the `agents` folder as python, and hook them up to
[`inputs`][inputs] and [`outputs`][outputs] using Redpanda Connect.

Each agent can also be given a set of tools (exposed over MCP) as [resources][resources].

To showcase each of these, there is an example weather agent, that processes messages
from `stdin` and writes it's output to `stdout` using an example `http` processor tool
to lookup the weather in a given location.

Running this example requires [`uv`](https://docs.astral.sh/uv/) to be installed on the
host. Then you can run the agent using `rpk connect agent run`.

[inputs]: https://docs.redpanda.com/redpanda-connect/components/inputs/about/
[outputs]: https://docs.redpanda.com/redpanda-connect/components/outputs/about/
[resources]: https://docs.redpanda.com/redpanda-connect/configuration/resources/

