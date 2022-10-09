import threading
import asyncio
import logging
from time import sleep
import yaml
from .processes import ProcessMap

__all__ = ["SortingAgent"]

class SortingAgent(threading.Thread):
    _event_quit_ = None
    _loop_ = None
    _map_ = {}

    def __init__(self, group=None, name="SortingAgent", args=(), kwargs={}, daemon=None):
        super().__init__(group=group, name=name, args=args, kwargs=kwargs, daemon=daemon)
        self._loop_ = asyncio.new_event_loop()
        self._event_quit_ = asyncio.Event()
        self._event_quit_.clear()

    def load_config(self, path):
        raw = None
        logging.info("loading {}".format(path))
        with open(path, "r") as f:
            raw = yaml.load(f, Loader=yaml.Loader)
        for item in raw["pipelines"]:
            temp = {}
            temp["pattern"] = item["pattern"]
            temp["process"] = []
            for process in item["process"]:
                p = {}
                p["function"] = ProcessMap[process["type"]]
                if "arg" in process.keys():
                    p["arg"] = process["arg"]
                else:
                    p["arg"] = ""
                temp["process"].append(p)
            self._map_[item["name"]] = temp
        logging.debug(self._map_)

    def run(self):
        asyncio.set_event_loop(self._loop_)
        logging.info("agent started")
        self._loop_.run_until_complete(self._event_quit_.wait())
        logging.info("agent stopped")

    async def _async_quit(self):
        self._event_quit_.set()

    def require_quit(self):
        asyncio.run_coroutine_threadsafe(self._async_quit(), self._loop_)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    agent = SortingAgent()
    agent.start()
    sleep(1.0)
    agent.require_quit()
    agent.join()
