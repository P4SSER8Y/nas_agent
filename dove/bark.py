from typing import Optional
from .base import DoveBase
import httpx
import urllib.parse
import logging


class Bark(DoveBase):
    def __init__(self, name: str, arg: dict) -> None:
        super().__init__(name)
        self._url = "https://api.day.app/{key}".format(**arg)
        self._group = arg.get("group", None)

    async def publish(self, msg: dict) -> None:
        data = {"body": msg["msg"]}
        if "title" in msg.keys():
            data["title"] = msg["title"]
        if "group" in msg.keys():
            data["group"] = msg["group"]
        elif self._group:
            data["group"] = self._group
        async with httpx.AsyncClient() as client:
            ret = await client.post(self._url, data=data)
            if ret.status_code != httpx.codes.OK:
                logging.error(f"post to {self._url} with data={data} failed with status_code={ret.status_code}")
                ret.raise_for_status()

