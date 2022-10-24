import logging
from rich.logging import RichHandler
logging.basicConfig(level=logging.INFO,
                    format="[%(threadName)s] %(message)s",
                    datefmt="[%m-%d %H:%M:%S]",
                    handlers=[RichHandler(rich_tracebacks=True)])

import click
from sorting_agent import SortingAgent
import signal
import yaml


threads = []


def handler_SIGINT(signum, frame):
    logging.warning("SIGINT raised")
    for item in threads:
        if hasattr(item, "require_quit"):
            item.require_quit()


def factory(object: dict):
    _MAP_ = {
        "sorting_agent": SortingAgent,
    }
    logging.debug(object)

    ret = None
    if not object["type"].lower() in _MAP_.keys():
        return None
    ret = _MAP_[object["type"]](name=object["name"])
    if hasattr(ret, "load_config") and "config" in object.keys():
        ret.load_config(object["config"])
    return ret


@click.command()
@click.argument("config", default="./launch.yml", type=click.Path(exists=True))
def main(config):
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


if __name__ == "__main__":
    main()
