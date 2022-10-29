from .base import DoveBase
import logging
from typing import Optional
import httpx

class ServerChan(DoveBase):
    def __init__(self, name: str, arg: dict) -> None:
        super().__init__(name)
        self._url = "https://sctapi.ftqq.com/{key}.send".format(**arg)
        self._channel = arg.get("channel", None)

    async def publish(self, msg: dict) -> None:
        data = {"desp": msg["msg"]}
        data["title"] = msg.get("title", None)
        data["short"] = msg.get("short", None)
        if self._channel:
            data["channel"] = self._channel
        async with httpx.AsyncClient() as client:
            ret = await client.post(self._url, data=data)
            if ret.status_code != httpx.codes.OK:
                logging.error(f"post to {self._url} with data={data} failed with status_code={ret.status_code}")
                ret.raise_for_status()

