# autopep8: on
import logging
logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s][%(levelname)s][%(threadName)s] %(message)s")
# autopep8: off

from sorting_agent import SortingAgent
import click
import signal

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
