import logging
from rich.logging import RichHandler
import click
import sys


@click.group()
@click.option("-l", "--log-level", default="info",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False))
def main(log_level):
    MAP_LOG_LEVEL = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
    }
    logging.basicConfig(level=MAP_LOG_LEVEL[log_level],
                        format="[%(threadName)s] %(message)s",
                        datefmt="[%m-%d %H:%M:%S]",
                        handlers=[RichHandler(rich_tracebacks=True)])
    logging.log(MAP_LOG_LEVEL[log_level], f"set log level to {log_level}")
    logging.log(MAP_LOG_LEVEL[log_level], f"{sys.argv}")


@main.command(help="takeoff and start")
@click.argument("config", default="./launch.yml", type=click.Path(exists=True))
def takeoff(config):
    from headquarter import entry
    entry(config)


@main.command(help="list all available processors of sorting agent")
def list_processors():
    from sorting_agent import processes
    from rich.console import Console
    from rich.markdown import Markdown
    m = processes.ProcessMap
    md = []
    for item in m.keys():
        md.append(f"# {item}")
        if m[item].__doc__:
            md.append(m[item].__doc__)
        md.append('')
    Console().print(Markdown("\n".join(md)))


if __name__ == "__main__":
    main()
