import asyncio
import logging
from redpanda_connect import input, input_main, Value, Message

@input
async def my_input(config: Value):
    _ = config
    yield Message(payload="Hello")
    yield Message(payload="World")
    yield Message(payload="!")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(input_main(my_input))
