import asyncio
from collections.abc import AsyncIterator
import logging
from redpanda_connect import output, output_main, Value, Message

@output(max_in_flight=1)
async def my_output(config: Value, messages: AsyncIterator[Message]):
    _ = config
    async for message in messages:
        print(f"Outputting message: {message}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(output_main(my_output))
