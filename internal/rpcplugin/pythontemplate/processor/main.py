import asyncio
import logging
from redpanda_connect import processor, processor_main, Message

@processor
def my_processor(msg: Message) -> Message:
    logging.info(f"Processing message: {msg}")
    return msg

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(processor_main(my_processor))
