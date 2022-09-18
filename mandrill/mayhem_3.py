import logging
import queue
import random
import string
import time

import attr


# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ("foo %s" % "bar") is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"

import uuid
import time

def publish_sync(queue):
    choices = string.ascii_lowercase + string.digits
    while True:
        msg_id=str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        queue.put(msg)
        logging.info(f"Published {msg}")
        time.sleep(random.random())
        # time.sleep(1)


def consume_sync(queue):
    """Consumer client to simulate subscribing to a publisher.
    Args:
        queue (queue.Queue): Queue from which to consume messages.
    """
    while True:
        # wait for an item from the publisher
        msg = queue.get()
        # process the msg
        logging.info(f"Consumed {msg}")
        # simulate i/o operation using sleep
        time.sleep(random.random())
        # time.sleep(1)

from concurrent.futures import ThreadPoolExecutor
import asyncio

async def publish(executor, queue):
    logging.info('Starting publisher')
    loop = asyncio.get_running_loop()
    # await loop.run_in_executor(executor, publish_sync, queue)
    # futures = [loop.run_in_executor(executor, publish_sync, queue) for i in range(5)]
    # asyncio.ensure_future(asyncio.gather(*futures, return_exceptions=True))
    asyncio.ensure_future(loop.run_in_executor(executor, publish_sync, queue))

async def consume(executor, queue):
    logging.info("Starting consumer")
    loop=asyncio.get_running_loop()
    # await loop.run_in_executor(executor, consume_sync, queue)
    # futures = [loop.run_in_executor(executor, consume_sync, queue) for i in range(5)]
    # asyncio.ensure_future(asyncio.gather(*futures, return_exceptions=True))
    asyncio.ensure_future(loop.run_in_executor(executor, consume_sync, queue))

def main():
    executor = ThreadPoolExecutor()
    loop=asyncio.get_event_loop()
    q = queue.Queue()
    try:
        loop.create_task(publish(executor, q))
        loop.create_task(consume(executor, q))
        loop.run_forever()
    finally:
        loop.close()
        logging.info("Successfully shutdown the mayhem service")


if __name__ == "__main__":
    main()