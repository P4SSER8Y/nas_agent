import traceback
from .base import DoveBase
from .serverchan import ServerChan
from .bark import Bark

import asyncio
from typing import Callable, Iterable, Mapping, Any, Optional
import pathlib
import threading
import yaml
import logging
from aiozmq import rpc
import shortuuid
import time

__all__ = ["Dove"]


class Handler(rpc.AttrHandler):
    def __init__(self, dove) -> None:
        super().__init__()
        self._dove_ = dove

    @rpc.method
    def publish(self, msg, names):
        self._dove_.publish(msg, names)


class Dove(threading.Thread):
    def __init__(self,
                 target: Callable[..., object] | None = ...,
                 name: str | None = "dove",
                 args: Iterable[Any] = ...,
                 kwargs: Mapping[str,
                                 Any] | None = ...,
                 *,
                 daemon: bool | None = ...) -> None:
        super().__init__(None, target, name, args, kwargs, daemon=daemon)
        self._loop_ = asyncio.new_event_loop()
        self._doves_: dict[str, DoveBase] = {}
        self._event_quit_ = asyncio.Event()

    def _factory(self, type: str, name: str, arg: Any) -> DoveBase:
        match type.lower():
            case "bark":
                return Bark(name, arg)
            case "serverchan":
                return ServerChan(name, arg)
            case _:
                raise NameError(f"{type} not found")

    def load_config(self, path: pathlib.Path | str):
        with open(path, "r") as f:
            cfg = yaml.load(f, Loader=yaml.SafeLoader)
        for item in cfg["doves"]:
            name = item.get("name", shortuuid.random())
            self._doves_[name] = self._factory(item["type"], name, item["arg"])

    async def _async_quit(self):
        self._event_quit_.set()

    def require_quit(self):
        asyncio.run_coroutine_threadsafe(self._async_quit(), self._loop_)

    async def _async_publish(self, msg: dict, names: Optional[list[str]] = None):
        if not names:
            names = self._doves_.keys()
        for name in names:
            try:
                await self._doves_[name].publish(msg)
            except Exception as e:
                logging.error(f"cannot publish to {name}: {e}")
                logging.error(traceback.format_exc())

    def publish(self, msg: dict, names: Optional[list[str]] = None):
        asyncio.run_coroutine_threadsafe(self._async_publish(msg, names), self._loop_)

    def run(self):
        asyncio.set_event_loop(self._loop_)
        url = f"inproc://{self.name}"
        logging.info(f"creating server {url}")
        handler = Handler(self)
        server = self._loop_.run_until_complete(rpc.serve_pubsub(handler, subscribe="publish", bind=url))
        logging.info(f"server {url} created")
        logging.info("started")
        self._loop_.run_until_complete(self._event_quit_.wait())
        server.close()
        self._loop_.run_until_complete(server.wait_closed())
        logging.info("stopped")
