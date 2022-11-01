import logging
import threading
import pathlib
from typing import Callable, Iterable, Mapping, Any
import yaml
import subprocess

class Jarvis(threading.Thread):
    def __init__(self, group: None = ..., target: Callable[..., object] | None = ..., name: str | None = ..., args: Iterable[Any] = ..., kwargs: Mapping[str, Any] | None = ..., *, daemon: bool | None = ...) -> None:
        super().__init__(None, target, name, args, kwargs, daemon=daemon)

    def load_config(self, path: pathlib.Path | str):
        with open(path, "r") as f:
            raw = yaml.load(f, Loader=yaml.SafeLoader)
        cfg = raw["jarvis"]
        self._host = cfg.get("host", "127.0.0.1")
        self._port = cfg.get("port", 5000)
        self._event = threading.Event()
        
    def require_quit(self):
        self._event.set()

    def run(self):
        cmd = f"uvicorn jarvis:app --host '{self._host}' --port {self._port}"
        logging.info(f"wakeup Jarvis by '{cmd}'")
        p = subprocess.Popen(cmd, shell=True)
        self._event.wait()
        logging.info("hibernate Jarvis")
        p.kill()
        p.wait()
