import logging
from rich.logging import RichHandler
import click


@click.command()
@click.argument("config", default="./launch.yml", type=click.Path(exists=True))
@click.option("-l", "--log-level", default="info",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False))
def main(config, log_level):
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

    from headquarter import entry
    entry(config)


if __name__ == "__main__":
    main()
