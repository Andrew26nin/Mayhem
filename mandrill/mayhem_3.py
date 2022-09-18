import asyncio
import logging
import queue
import random
import string
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
import signal
import attr
import functools

# NB: Using f-strings with log messages may not be ideal since no matter
# what the log level is set at, f-strings will always be evaluated
# whereas the old form ("foo %s" % "bar") is lazily-evaluated.
# But I just love f-strings.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)


class RestartFailed(Exception):
    pass


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id = attr.ib(repr=False)
    hostname = attr.ib(repr=False, init=False)
    restarted = attr.ib(repr=False, default=False)
    saved = attr.ib(repr=False, default=False)
    acked = attr.ib(repr=False, default=False)
    extended_cnt = attr.ib(repr=False, default=0)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


async def restart_host(msg):
    """Restart a given host.
    Args:
        msg (PubSubMessage): consumed event message for a particular
            host to be restarted.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    # totally realistic exception
    if random.randrange(1, 5) == 3:
        raise RestartFailed(f"Could not restart {msg.hostname}")
    msg.restart = True
    logging.info(f"Restarted {msg.hostname}")


async def save(msg):
    """Save message to a database.
    Args:
        msg (PubSubMessage): consumed event message to be saved.
    """
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    # totally realistic exception
    if random.randrange(1, 5) == 3:
        raise Exception(f"Could not save {msg}")
    msg.save = True
    logging.info(f"Saved {msg} into database")


async def cleanup(msg, event):
    """Cleanup tasks related to completing work on a message.
    Args:
        msg (PubSubMessage): consumed event message that is done being
            processed.
    """
    # this will block the rest of the coro until `event.set` is called
    await event.wait()
    # unhelpful simulation of i/o work
    await asyncio.sleep(random.random())
    msg.acked = True
    logging.info(f"Done. Acked {msg}")


async def extend(msg, event):
    """Periodically extend the message acknowledgement deadline.
    Args:
        msg (PubSubMessage): consumed event message to extend.
        event (asyncio.Event): event to watch for message extention or
            cleaning up.
    """
    while not event.is_set():
        msg.extended_cnt += 1
        logging.info(f"Extended deadline by 3 seconds for {msg}")
        # want to sleep for less than the deadline amount
        await asyncio.sleep(2)


def handle_results(results, msg):
    """Handle exception results for a given message."""
    for result in results:
        if isinstance(result, RestartFailed):
            logging.error(f"Retrying for failure to restart: {msg.hostname}")
        elif isinstance(result, Exception):
            logging.error(f"Handling general error: {result}")


async def handle_message(msg):
    """Kick off tasks for a given message.
    Args:
        msg (PubSubMessage): consumed message to process.
    """
    event = asyncio.Event()
    asyncio.create_task(extend(msg, event))
    asyncio.create_task(cleanup(msg, event))

    results = await asyncio.gather(save(msg), restart_host(msg), return_exceptions=True)
    handle_results(results, msg)
    event.set()


def publish_sync(queue):
    choices = string.ascii_lowercase + string.digits
    while True:
        msg_id = str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        queue.put(msg)
        logging.info(f"Published {msg}")
        time.sleep(random.random())
        # time.sleep(1)


def consume_sync(queue, loop):
    """Consumer client to simulate subscribing to a publisher.
    Args:
        queue (queue.Queue): Queue from which to consume messages.
    """
    while True:
        msg = queue.get()
        logging.info(f"Consumed {msg}")
        asyncio.run_coroutine_threadsafe(handle_message(msg), loop)
        # loop.create_task(handle_message(msg))


async def publish(executor, queue):
    logging.info("Starting publisher")
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(loop.run_in_executor(executor, publish_sync, queue))


async def consume(executor, queue):
    logging.info("Starting consumer")
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(
        loop.run_in_executor(executor, consume_sync, queue, loop), loop=loop
    )


async def shutdown(loop, executor, signal=None):
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    logging.info("Nacking outstanding messages")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("Shutting down executor")
    executor.shutdown(wait=False)

    logging.info(f"Releasing {len(executor._threads)} threads from executor")
    for thread in executor._threads:
        try:
            thread._tstate_lock.release()
        except Exception:
            pass

    logging.info(f"Flushing metrics")
    loop.stop()


def handle_exception(executor, loop, context):
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop, executor))


def main():
    q = queue.Queue()
    executor = ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(loop, executor, signal=s))
        )
    handle_exc_func = functools.partial(handle_exception, executor)
    loop.set_exception_handler(handle_exc_func)
    try:
        loop.create_task(publish(executor, q))
        loop.create_task(consume(executor, q))
        loop.run_forever()
    finally:
        loop.close()
        logging.info("Successfully shutdown the mayhem service")


if __name__ == "__main__":
    main()
