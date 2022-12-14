import yaml
import signal
from sorting_agent import SortingAgent
from dove import Dove
import logging


threads = []


def handler_SIGINT(signum, frame):
    logging.warning("SIGINT raised")
    for item in threads:
        if hasattr(item, "require_quit"):
            item.require_quit()


def factory(object: dict):
    _MAP_ = {
        "sorting_agent": SortingAgent,
        "dove": Dove,
    }
    logging.debug(object)

    ret = None
    if not object["type"].lower() in _MAP_.keys():
        return None
    ret = _MAP_[object["type"]](name=object["name"])
    if hasattr(ret, "load_config") and "config" in object.keys():
        ret.load_config(object["config"])
    return ret


def entry(config):
    signal.signal(signal.SIGINT, handler_SIGINT)
    with open(config, "r") as f:
        items = yaml.load(f, yaml.FullLoader)
    for item in items["agents"]:
        object = factory(item)
        if object:
            threads.append(object)
    [x.start() for x in threads]
    [x.join() for x in threads]
    logging.info("goodbye")
