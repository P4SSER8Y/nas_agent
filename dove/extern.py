from typing import Optional
from aiozmq import rpc
import asyncio
import logging
import time

__all__ = ["rpc"]


async def publish(server: str, msg: dict, names: Optional[list[str]] = None):
    url = f"inproc://{server}"
    client: rpc.pubsub.PubSubClient = await rpc.connect_pubsub(connect=url)
    await asyncio.sleep(0.1)
    await client.publish('publish').publish(msg, names)
