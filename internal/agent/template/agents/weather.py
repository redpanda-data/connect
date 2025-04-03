import asyncio
import logging
from typing import Any, override

import redpanda.runtime
from redpanda.agents import Agent, AgentHooks, Tool


class MyHooks(AgentHooks):
    @override
    async def on_start(self, agent: Agent) -> None:
        logging.debug("Agent started")

    @override
    async def on_end(self, agent: Agent, output: Any) -> None:
        logging.debug("Agent ended")

    @override
    async def on_tool_start(
        self,
        agent: Agent,
        tool: Tool,
        args: str,
    ) -> None:
        logging.debug(f"Agent calling tool {tool.name} with args: {args}")

    @override
    async def on_tool_end(
        self,
        agent: Agent,
        tool: Tool,
        result: str,
    ) -> None:
        logging.debug(f"Agent tool {tool.name} resulted in: {result}")


my_agent = Agent(
    name="WeatherAgent",
    model="openai/gpt-4o",
    instructions="""
    You are a helpful AI agent for finding out about the weather.
    """.strip(),
    hooks=MyHooks(),
)

asyncio.run(redpanda.runtime.serve(my_agent))

