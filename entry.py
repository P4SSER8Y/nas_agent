import logging
logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s][%(levelname)s][%(threadName)s] %(message)s")

import signal
import click
from sorting_agent import SortingAgent


sorting_agent = SortingAgent()


def handler_SIGINT(signum, frame):
    logging.warning("SIGINT raised")
    sorting_agent.require_quit()


@click.command()
@click.argument("sorting_config", default="./sorting_config.yml", type=click.Path(exists=True))
def main(sorting_config):
    signal.signal(signal.SIGINT, handler_SIGINT)
    sorting_agent.load_config(sorting_config)
    sorting_agent.start()
    sorting_agent.join()


if __name__ == "__main__":
    main()