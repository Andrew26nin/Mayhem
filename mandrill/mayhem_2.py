import asyncio
import logging
import random
import uuid
import string


import attr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id = attr.ib(repr=False)
    hostname = attr.ib(repr=False, init=False)
    restarted = attr.ib(repr=False, default=False)
    saved = attr.ib(repr=False, default=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


async def save(msg):
    await asyncio.sleep(random.random())
    msg.saved = True
    logging.info(f"Saved {msg} into database")


async def restart_host(msg):
    await asyncio.sleep(random.random())
    msg.restarted = True
    logging.info(f"Restarted {msg.hostname}")


async def consume(queue):
    while True:
        msg = await queue.get()
        logging.info(f"Consumed {msg}")
        asyncio.create_task(save(msg))
        asyncio.create_task(restart_host(msg))


async def publish(queue):
    choices = string.ascii_lowercase + string.digits
    while True:
        msg_id = str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        asyncio.create_task(queue.put(msg))
        logging.debug(f"Published message {msg}")
        await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    try:
        loop.create_task(publish(queue))
        loop.create_task(consume(queue))
        loop.run_forever()
    except KeyboardInterrupt:
        logging.info("Process interrupted")
    finally:
        loop.close()
        logging.info("Successfully shutdown the Mayhem service")


if __name__ == "__main__":
    main()
